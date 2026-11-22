//! Exact 64-bit keys for ID strings, per kind of ID.
//!
//! Every ID column has a usual first letter (`e` for event_id, `u` for user
//! IDs, `k` for click_id). An ID of 2 to 9 bytes that starts with its column's
//! letter and has no NUL byte is "raw": its key is the bytes after the letter,
//! little-endian and zero-padded, so the low byte (the second character) is
//! never zero. Reading one is a single 8-byte load and a mask. Every other
//! non-empty ID is interned and gets `index << 8`, whose low byte is zero.
//!
//! Keys of one column are equal exactly when the IDs are. Raw keys of a column
//! share the first letter, so comparing their big-endian bytes compares the
//! IDs byte by byte; anything involving an interned key compares the strings.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Mutex, RwLock};

/// Mask keeping the low `n` bytes (n = 0..=8).
pub const LENMASK: [u64; 9] = [
    0,
    0xFF,
    0xFFFF,
    0xFF_FFFF,
    0xFFFF_FFFF,
    0xFF_FFFF_FFFF,
    0xFFFF_FFFF_FFFF,
    0xFF_FFFF_FFFF_FFFF,
    u64::MAX,
];

#[inline(always)]
pub fn is_raw(k: u64) -> bool {
    k & 0xFF != 0
}

/// Raw key of `s` if it is a raw ID of the column whose letter is `pfx`.
#[inline(always)]
pub fn raw(s: &[u8], pfx: u8) -> Option<u64> {
    let n = s.len();
    if !(2..=9).contains(&n) || s[0] != pfx {
        return None;
    }
    let v = if n == 9 {
        // the usual case (letter + 8 characters): one 8-byte load
        u64::from_le_bytes(s[1..9].try_into().unwrap())
    } else {
        let mut b = [0u8; 8];
        b[..n - 1].copy_from_slice(&s[1..]);
        u64::from_le_bytes(b)
    };
    if has_zero(v, n - 1) {
        return None;
    }
    Some(v)
}

/// Any zero byte among the low `n` bytes of `v` (n = 1..=8).
#[inline(always)]
pub fn has_zero(v: u64, n: usize) -> bool {
    let w = v | !LENMASK[n];
    (w.wrapping_sub(0x0101_0101_0101_0101) & !w & 0x8080_8080_8080_8080) != 0
}

static TABLE: Mutex<Option<HashMap<Vec<u8>, u64>>> = Mutex::new(None);
static FROZEN: RwLock<Vec<Vec<u8>>> = RwLock::new(Vec::new());

/// The key of a non-empty ID of the column whose letter is `pfx`.
#[inline(always)]
pub fn key(s: &[u8], pfx: u8) -> u64 {
    match raw(s, pfx) {
        Some(k) => k,
        None => {
            if s.len() == 33 && s[0] == pfx {
                if let Some(v) = hex128(&s[1..]) {
                    return long_key(v);
                }
            }
            intern(s)
        }
    }
}

// ---- Long IDs: the column letter plus exactly 32 lowercase hex digits ----
//
// Scorer v5 gives one in eight IDs this form. Such an ID is exactly a 128-bit
// number, and fixed-width lowercase hex sorts like that number.
//
// Fast mode (default): the key is the top 55 bits of the value (bit 63 set,
// low byte zero, so never raw), computed with no table and no lock. Each value
// is also appended to the calling thread's own list. After parsing,
// `verify_longs` sorts all values and checks that no two different values
// share a key; the sorted unique values then serve `bytes`. If a key is shared
// (or a comparison could not be decided from keys before verification), the
// caller re-runs the whole pipeline in exact mode.
//
// Exact mode (SHOP_EXACT_LONG=1): values are interned in 256 independently
// locked shards; the key holds the shard and the index.

use std::cell::{Cell, UnsafeCell};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AO};

const HEX_TAG: u64 = 1 << 63;

/// Exact mode switch (set at start from SHOP_EXACT_LONG).
pub static EXACT: AtomicBool = AtomicBool::new(false);
/// Set when fast mode met something it cannot settle (forces an exact re-run).
static NEED_EXACT: AtomicBool = AtomicBool::new(false);

// Per-thread value lists, one slot per thread, claimed on first use. A slot is
// written only by its own thread; `verify_longs` reads them after every worker
// thread has finished.
const STRIPES: usize = 1024;
struct Stripe(UnsafeCell<Vec<u128>>);
unsafe impl Sync for Stripe {}
static STRIPE: [Stripe; STRIPES] = [const { Stripe(UnsafeCell::new(Vec::new())) }; STRIPES];
static NEXT_STRIPE: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static MY_STRIPE: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// Sorted unique long values, filled by `verify_longs`.
static LONGS: std::sync::OnceLock<Vec<u128>> = std::sync::OnceLock::new();
static VERIFIED: AtomicBool = AtomicBool::new(false);

#[inline(always)]
fn long_key(v: u128) -> u64 {
    if EXACT.load(AO::Relaxed) {
        return intern_hex(v);
    }
    let s = MY_STRIPE.with(|c| {
        let mut s = c.get();
        if s == usize::MAX {
            s = NEXT_STRIPE.fetch_add(1, AO::Relaxed);
            c.set(s);
        }
        s
    });
    if s < STRIPES {
        unsafe { (*STRIPE[s].0.get()).push(v) };
    } else {
        NEED_EXACT.store(true, AO::Relaxed);
    }
    HEX_TAG | ((v >> 73) as u64) << 8
}

/// After all parsing threads have finished: check that every long key stands
/// for one value and build the lookup table. False means "re-run exactly".
pub fn verify_longs(threads: usize) -> bool {
    if EXACT.load(AO::Relaxed) {
        return true;
    }
    if NEED_EXACT.load(AO::Relaxed) {
        return false;
    }
    let n = NEXT_STRIPE.load(AO::Relaxed).min(STRIPES);
    let mut all: Vec<Vec<u128>> = (0..n).map(|i| std::mem::take(unsafe { &mut *STRIPE[i].0.get() })).collect();
    let total: usize = all.iter().map(|v| v.len()).sum();
    if total == 0 {
        VERIFIED.store(true, AO::Relaxed);
        return true;
    }
    // Bucket by the top 8 bits (values are spread out), then sort buckets in parallel.
    const B: usize = 256;
    let mut buckets: Vec<Vec<u128>> = (0..B).map(|_| Vec::with_capacity(total / B + 16)).collect();
    for list in all.drain(..) {
        for v in list {
            buckets[(v >> 120) as usize].push(v);
        }
    }
    let ok = AtomicBool::new(true);
    let next = AtomicUsize::new(0);
    let cells: Vec<UnsafeCellVec> = buckets.into_iter().map(|b| UnsafeCellVec(UnsafeCell::new(b))).collect();
    std::thread::scope(|sc| {
        for _ in 0..threads.max(1) {
            sc.spawn(|| loop {
                let i = next.fetch_add(1, AO::Relaxed);
                if i >= B {
                    break;
                }
                let b = unsafe { &mut *cells[i].0.get() };
                b.sort_unstable();
                b.dedup();
                for w in b.windows(2) {
                    if (w[0] >> 73) == (w[1] >> 73) {
                        ok.store(false, AO::Relaxed);
                    }
                }
            });
        }
    });
    if !ok.load(AO::Relaxed) {
        return false;
    }
    let mut table = Vec::with_capacity(total);
    for c in cells {
        table.extend(c.0.into_inner());
    }
    let _ = LONGS.set(table);
    VERIFIED.store(true, AO::Relaxed);
    !NEED_EXACT.load(AO::Relaxed)
}

struct UnsafeCellVec(UnsafeCell<Vec<u128>>);
unsafe impl Sync for UnsafeCellVec {}

const HEX_SHARDS: usize = 256;

#[derive(Default)]
struct Fx(u64);
impl std::hash::Hasher for Fx {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.rotate_left(5) ^ b as u64).wrapping_mul(0x51_7cc1_b727_220a_95);
        }
    }
    #[inline(always)]
    fn write_u128(&mut self, v: u128) {
        self.0 = ((v as u64) ^ ((v >> 64) as u64).rotate_left(29)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}
type FxBuild = std::hash::BuildHasherDefault<Fx>;

struct HexShard {
    map: HashMap<u128, u64, FxBuild>,
    vals: Vec<u128>,
}

static HEX: [Mutex<Option<HexShard>>; HEX_SHARDS] = [const { Mutex::new(None) }; HEX_SHARDS];

/// Value of exactly 32 lowercase hex digits, eight at a time (SWAR).
#[inline(always)]
fn hex128(s: &[u8]) -> Option<u128> {
    let w = |i: usize| u64::from_le_bytes(s[8 * i..8 * i + 8].try_into().unwrap());
    let (a, b, c, d) = (hex8(w(0))?, hex8(w(1))?, hex8(w(2))?, hex8(w(3))?);
    Some((a as u128) << 96 | (b as u128) << 64 | (c as u128) << 32 | d as u128)
}

/// Eight lowercase hex digits (first digit in the low byte) to their value,
/// or None if any byte is not 0-9 or a-f.
#[inline(always)]
fn hex8(x: u64) -> Option<u32> {
    const O: u64 = 0x0101_0101_0101_0101;
    const H: u64 = 0x8080_8080_8080_8080;
    if x & H != 0 {
        return None;
    }
    // per byte: high bit set when the byte is in [lo, hi] (bytes are < 0x80)
    let range = |lo: u64, hi: u64| (x.wrapping_add(O * (0x80 - lo)) & !x.wrapping_add(O * (0x7F - hi))) & H;
    if (range(b'0' as u64, b'9' as u64) | range(b'a' as u64, b'f' as u64)) != H {
        return None;
    }
    // '0'..'9' -> 0..9, 'a'..'f' (bit 6 set) -> low nibble + 9
    let n = (x & 0x0F0F_0F0F_0F0F_0F0F) + 9 * ((x >> 6) & O);
    // pack: the first digit is the most significant
    let p = ((n << 4) | (n >> 8)) & 0x00FF_00FF_00FF_00FF;
    let p = ((p << 8) | (p >> 16)) & 0x0000_FFFF_0000_FFFF;
    let p = ((p << 16) | (p >> 32)) & 0xFFFF_FFFF;
    Some(p as u32)
}

#[cfg(test)]
fn hex128_slow(s: &[u8]) -> Option<u128> {
    let mut v: u128 = 0;
    for &c in &s[..32] {
        let d = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            _ => return None,
        };
        v = v << 4 | d as u128;
    }
    Some(v)
}

/// Exact mode: intern by value in a sharded table.
fn intern_hex(v: u128) -> u64 {
    let shard = ((v as u64) ^ ((v >> 64) as u64)) as usize & (HEX_SHARDS - 1);
    let mut guard = HEX[shard].lock().unwrap();
    let sh = guard.get_or_insert_with(|| HexShard { map: HashMap::with_hasher(FxBuild::default()), vals: Vec::new() });
    if let Some(&k) = sh.map.get(&v) {
        return k;
    }
    let k = HEX_TAG | (sh.vals.len() as u64) << 16 | (shard as u64) << 8;
    sh.vals.push(v);
    sh.map.insert(v, k);
    k
}

#[inline(always)]
fn is_hex(k: u64) -> bool {
    k & HEX_TAG != 0 && !is_raw(k)
}

/// The 128-bit value behind a long key. Fast mode needs `verify_longs` first.
fn hex_value(k: u64) -> Option<u128> {
    if EXACT.load(AO::Relaxed) {
        let shard = ((k >> 8) & 0xFF) as usize;
        let idx = ((k & !HEX_TAG) >> 16) as usize;
        return Some(HEX[shard].lock().unwrap().as_ref().unwrap().vals[idx]);
    }
    if !VERIFIED.load(AO::Relaxed) {
        return None;
    }
    let top = (k & !HEX_TAG) >> 8;
    let Some(t) = LONGS.get() else { return None };
    let i = t.partition_point(|&v| ((v >> 73) as u64) < top);
    (i < t.len() && (t[i] >> 73) as u64 == top).then(|| t[i])
}

/// Fast mode, before verification: the first `n` hex digits (n <= 13) of the
/// value behind a long key, from the key alone.
fn hex_prefix(k: u64, n: usize, out: &mut Vec<u8>) {
    let top = (k & !HEX_TAG) >> 8; // 55 bits = the top 13.75 hex digits
    for i in 0..n {
        let nib = (top >> (51 - 4 * i)) & 15;
        out.push(b"0123456789abcdef"[nib as usize]);
    }
}

#[cold]
pub fn intern(s: &[u8]) -> u64 {
    let mut guard = TABLE.lock().unwrap();
    let map = guard.get_or_insert_with(HashMap::new);
    if let Some(&k) = map.get(s) {
        return k;
    }
    let mut frozen = FROZEN.write().unwrap();
    let k = (frozen.len() as u64 + 1) << 8;
    frozen.push(s.to_vec());
    map.insert(s.to_vec(), k);
    k
}

/// Append the ID behind `k` (a key of the column whose letter is `pfx`).
#[inline]
pub fn bytes(k: u64, pfx: u8, out: &mut Vec<u8>) {
    if is_raw(k) {
        out.push(pfx);
        let b = k.to_le_bytes();
        let n = 8 - (k.leading_zeros() as usize / 8);
        out.extend_from_slice(&b[..n]);
    } else if is_hex(k) {
        out.push(pfx);
        match hex_value(k) {
            Some(v) => {
                for i in (0..32).rev() {
                    out.push(b"0123456789abcdef"[((v >> (4 * i)) & 15) as usize]);
                }
            }
            None => {
                // only reachable in fast mode before verification: never written out
                NEED_EXACT.store(true, AO::Relaxed);
                hex_prefix(k, 13, out);
            }
        }
    } else {
        out.extend_from_slice(&FROZEN.read().unwrap()[((k >> 8) - 1) as usize]);
    }
}

/// Byte-wise comparison of the IDs behind two keys of one column.
#[inline]
pub fn cmp(a: u64, b: u64, pfx: u8) -> Ordering {
    if is_raw(a) && is_raw(b) {
        return a.swap_bytes().cmp(&b.swap_bytes());
    }
    if is_hex(a) && is_hex(b) {
        if a == b {
            return Ordering::Equal;
        }
        if !EXACT.load(AO::Relaxed) {
            // fast mode: keys are the top 55 bits; distinct keys order like the values
            return a.cmp(&b);
        }
        return hex_value(a).cmp(&hex_value(b));
    }
    cmp_slow(a, b, pfx)
}

/// Byte order of `other` (a whole ID that is not a long one) against the long
/// ID behind fast-mode key `long`, using only the key's 13 known hex digits.
/// None when those digits cannot settle it.
fn other_vs_long_prefix(other: &[u8], long: u64, pfx: u8) -> Option<Ordering> {
    let mut l = vec![pfx];
    hex_prefix(long, 13, &mut l);
    let n = other.len().min(l.len());
    match other[..n].cmp(&l[..n]) {
        // other is a prefix of the known part and shorter than 33 bytes: a proper prefix
        Ordering::Equal if other.len() <= l.len() => Some(Ordering::Less),
        Ordering::Equal => None,
        r => Some(r),
    }
}

#[cold]
fn cmp_slow(a: u64, b: u64, pfx: u8) -> Ordering {
    let fast_unverified = !EXACT.load(AO::Relaxed) && !VERIFIED.load(AO::Relaxed);
    if fast_unverified && (is_hex(a) || is_hex(b)) {
        // exactly one side is long; compare the other side with the long ID's
        // known prefix (column letter + 13 hex digits)
        // r below is cmp(other, long); cmp(a, b) is r when a is the other side
        let (long, other, a_is_long) = if is_hex(a) { (a, b, true) } else { (b, a, false) };
        let mut o = Vec::new();
        bytes(other, pfx, &mut o);
        let r = match other_vs_long_prefix(&o, long, pfx) {
            Some(r) => r,
            None => {
                NEED_EXACT.store(true, AO::Relaxed);
                Ordering::Equal
            }
        };
        return if a_is_long { r.reverse() } else { r };
    }
    let (mut x, mut y) = (Vec::new(), Vec::new());
    bytes(a, pfx, &mut x);
    bytes(b, pfx, &mut y);
    x.cmp(&y)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex128_matches_slow() {
        let mut st = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            st ^= st << 13;
            st ^= st >> 7;
            st ^= st << 17;
            st
        };
        let alphabet = b"0123456789abcdefABCDEFgGzZ/:`@\x00\x7f\x80\xff ";
        for round in 0..200_000 {
            let mut s = [0u8; 32];
            for c in s.iter_mut() {
                // mostly valid hex, with an invalid byte now and then
                *c = if round % 3 == 0 && next() % 16 == 0 {
                    alphabet[(next() % alphabet.len() as u64) as usize]
                } else {
                    b"0123456789abcdef"[(next() % 16) as usize]
                };
            }
            assert_eq!(hex128(&s), hex128_slow(&s), "{:?}", String::from_utf8_lossy(&s));
        }
        for b in 0..=255u8 {
            let mut s = *b"0123456789abcdef0123456789abcdef";
            for i in [0, 7, 8, 15, 31] {
                let mut t = s;
                t[i] = b;
                assert_eq!(hex128(&t), hex128_slow(&t), "byte {b} at {i}");
            }
            s[3] = b;
            assert_eq!(hex128(&s), hex128_slow(&s));
        }
    }

    #[test]
    fn order_and_roundtrip() {
        let words: Vec<&[u8]> = vec![
            b"u1", b"U1", b"u", b"uz", b"u10", b"_", b"a", b"0", b"zzzzzzzzzz", b" u1", b"u1 ",
            b"u1\r", b"abcdefghijk", b"user_id_a", b"user_id_b\t", b"p\xc3\xa9", b"u\xc3\xa9",
            b"uzzzzzzzz", b"u00000000", b"u0000000", b"u~~~~~~~~", b"u\x7f", b"ua\x00b", b"u\x00",
            b"uab\x00", b"u12345678", b"u123456789", b"U1ABCDEFG", b" ", b"  ", b"u\xff\xff\xff\xff\xff\xff\xff",
            b"u0123456789abcdef0123456789abcdef", b"uffffffffffffffffffffffffffffffff", b"u00000000000000000000000000000000",
            b"u0123456789abcdef0123456789abcdeF", b"u0123456789abcdef0123456789abcde", b"u0123456789abcdef0123456789abcdef0",
            b"e0123456789abcdef0123456789abcdef", b"ua23456789abcdef0123456789abcdef0", b"u9fffffffffffffffffffffffffffffff",
            b"k0123456789abcdef0123456789abcdef", b"u0123456789abcdeg0123456789abcdef",
            b"U0123456789ABCDEF0123456789ABCDEF", b"U3BQELVHV", b"u0123456789abcd", b"u0123456789abcde", b"u0123",
        ];
        // fast mode: key everything (recording long values), then verify
        for pfx in [b'u', b'e', b'k'] {
            for a in &words {
                let _ = key(a, pfx);
            }
        }
        // before verification: a long ID against any other ID, from the key's prefix alone
        for pfx in [b'u', b'e', b'k'] {
            for a in &words {
                let ka = key(a, pfx);
                if !is_hex(ka) {
                    continue;
                }
                for b in &words {
                    if is_hex(key(b, pfx)) {
                        continue;
                    }
                    if let Some(r) = other_vs_long_prefix(b, ka, pfx) {
                        assert_eq!(r, b.cmp(a), "prefix order {:?} vs {:?}", String::from_utf8_lossy(b), String::from_utf8_lossy(a));
                    }
                }
            }
        }
        assert!(verify_longs(2), "long keys verify");
        for pfx in [b'u', b'e', b'k'] {
            for a in &words {
                let mut out = Vec::new();
                bytes(key(a, pfx), pfx, &mut out);
                assert_eq!(&out[..], *a, "roundtrip {:?}", String::from_utf8_lossy(a));
                for b in &words {
                    assert_eq!(cmp(key(a, pfx), key(b, pfx), pfx), a.cmp(b), "{:?} {:?}", a, b);
                    assert_eq!(key(a, pfx) == key(b, pfx), a == b);
                }
            }
        }
    }
}
