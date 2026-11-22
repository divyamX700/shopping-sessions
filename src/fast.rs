//! Fast path for input whose IDs are reversible scrambles of dense indexes.
//!
//! IDs of the form `<letter><base36>` are bijective 40-bit scrambles of a
//! counter. If the keys can be recovered from the input itself (pairs of IDs
//! whose counters are close), every event decodes to a dense slot, which turns
//! dedup into an array store, and every user ID decodes to (person, alias),
//! which turns the link join into tiny per-person union-finds. Events are then
//! walked in slot order, which groups each person's events in time order.
//!
//! Every assumption is checked; anything unexpected sends the row to a small
//! general path (strings + hash map), and anything structural aborts the fast
//! path before output is written, so the general pipeline in main.rs runs.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::OnceLock;
use std::thread;
use std::time::Instant;

use memmap2::Mmap;

use crate::sink::{Region, Sink};
use crate::{body, csv, index, list_csv, map_files, out, pool_finish, scan, ts, Ctx};

const M40: u64 = (1 << 40) - 1;
const MA: u64 = 0x9E37_79B9_7F4B;
const MB: u64 = 0xD6E8_FEB8_6659;
const DAYMS: i64 = 86_400_000;
const GAP: u32 = 30 * 60 * 1000;
const REACH: u32 = 7 * 86_400_000;
const EB: u32 = 16;
const CB: u32 = 12;
const ESC: u64 = 1 << 25;
const T_PURCHASE: u32 = 2;
const T_INVALID: u64 = 3;
const COMPLEX: u32 = 1 << 31;
const NONE: u32 = u32::MAX;

// ---------------------------------------------------------------- IDs

static B36: [u8; 256] = {
    let mut t = [255u8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = i as u8;
        i += 1;
    }
    let mut j = 0;
    while j < 26 {
        t[b'a' as usize + j] = 10 + j as u8;
        j += 1;
    }
    t
};
const DIG: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

/// Canonical lowercase base 36 (as the scrambled IDs are written), < 2^40.
#[inline(always)]
fn b36(s: &[u8]) -> Option<u64> {
    let n = s.len();
    if n == 0 || n > 8 || (n > 1 && s[0] == b'0') {
        return None;
    }
    let mut v = 0u64;
    let mut bad = 0u8;
    for &c in s {
        let d = B36[c as usize];
        bad |= d;
        v = v * 36 + d as u64;
    }
    if bad & 0x80 != 0 || v > M40 {
        return None;
    }
    Some(v)
}

fn push_b36(out: &mut Vec<u8>, mut v: u64) {
    let mut buf = [0u8; 13];
    let mut at = buf.len();
    loop {
        at -= 1;
        buf[at] = DIG[(v % 36) as usize];
        v /= 36;
        if v == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[at..]);
}

fn inv40(a: u64) -> u64 {
    let mut x = a;
    for _ in 0..6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
    }
    x & M40
}

/// The key-independent half of the inverse scramble.
#[inline(always)]
fn unx(o: u64, binv: u64) -> u64 {
    let y = o.wrapping_mul(binv) & M40;
    y ^ (y >> 17) ^ (y >> 34)
}

#[inline(always)]
fn dec(x: u64, k: u64, ainv: u64) -> u64 {
    (x ^ k).wrapping_mul(ainv) & M40
}

fn scramble(v: u64, k: u64) -> u64 {
    let mut m = (v.wrapping_mul(MA) ^ k) & M40;
    m ^= m >> 17;
    m.wrapping_mul(MB) & M40
}

static C62: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 62 {
        t[C62S[i] as usize] = i as u8 + 1;
        i += 1;
    }
    t
};
const C62S: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const CKM: u64 = (1 << 49) - 1;
const REF: u64 = 1 << 63;

/// Click IDs `k`/`K` + up to 8 of [0-9A-Za-z] as an order-preserving 49-bit key.
#[inline(always)]
fn ckey(s: &[u8]) -> Option<u64> {
    let n = s.len();
    if !(2..=9).contains(&n) || (s[0] | 0x20) != b'k' {
        return None;
    }
    let mut k = ((s[0] == b'k') as u64) << 48;
    for (i, &c) in s[1..].iter().enumerate() {
        let d = C62[c as usize];
        if d == 0 {
            return None;
        }
        k |= (d as u64) << (42 - 6 * i);
    }
    Some(k)
}

fn ckey_str(k: u64) -> Vec<u8> {
    let mut s = vec![if k >> 48 & 1 != 0 { b'k' } else { b'K' }];
    for i in 0..8 {
        let c = (k >> (42 - 6 * i)) & 63;
        if c == 0 {
            break;
        }
        s.push(C62S[(c - 1) as usize]);
    }
    s
}

/// Recover a scramble key from pairs of unscrambled values whose counters
/// differ by less than 2^dbits. The difference mod 2^(b+1) depends only on key
/// bits below b, so bits are fixed low to high with a small beam; the top bit
/// is left to `check` (a count of plausible decoded values).
fn recover(pairs: &[(u64, u64)], dbits: u32, ainv: u64, check: &dyn Fn(u64) -> usize) -> Option<(u64, usize)> {
    if pairs.len() < 24 {
        return None;
    }
    let off = 1u64 << dbits;
    let lim = 2u64 << dbits;
    let score = |k: u64, bit: u32, ps: &[(u64, u64)]| -> usize {
        let m = (2u64 << bit) - 1;
        ps.iter()
            .filter(|&&(a, b)| (dec(b, k, ainv).wrapping_sub(dec(a, k, ainv)).wrapping_add(off) & m) < lim)
            .count()
    };
    let n0 = dbits + 1;
    let mut cands: Vec<u64> = (0..1u64 << n0).collect();
    let few = &pairs[..pairs.len().min(96)];
    for bit in n0..40 {
        let ps = if cands.len() > 64 { few } else { pairs };
        let mut sc: Vec<(usize, u64)> = cands.iter().map(|&k| (score(k, bit, ps), k)).collect();
        sc.sort_unstable_by(|a, b| b.0.cmp(&a.0));
        sc.truncate(16);
        cands = sc.iter().flat_map(|&(_, k)| [k, k | 1 << bit]).collect();
    }
    let mut best: Option<(usize, usize, u64)> = None;
    for &k in &cands {
        let s = (score(k, 39, pairs), check(k), k);
        if best.is_none_or(|b| (s.0, s.1) > (b.0, b.1)) {
            best = Some(s);
        }
    }
    let (s, c, k) = best?;
    (s * 10 >= pairs.len() * 9).then_some((k, c))
}

// ---------------------------------------------------------------- timestamps

/// Timestamp parsing with a day table for the years around the window. A
/// valid timestamp outside those years is outside every window, so `None`
/// there means the same as for any bad timestamp.
pub(crate) struct Tsp {
    y0: i64,
    ny: i64,
    days: Vec<i32>,
}

#[inline(always)]
fn le8(s: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(s[at..at + 8].try_into().unwrap())
}

impl Tsp {
    pub(crate) fn new(ws: i64, we: i64) -> Self {
        let year = |ms: i64| {
            let days = ms.div_euclid(DAYMS);
            let mut y = 1970 + days / 366;
            while crate::ts::days_from_civil(y + 1, 1, 1) <= days {
                y += 1;
            }
            y
        };
        let y0 = year(ws - 10 * DAYMS) - 1;
        let ny = year(we + 10 * DAYMS) + 1 - y0 + 1;
        let mut days = vec![i32::MIN; (ny * 12 * 32) as usize];
        for y in 0..ny {
            for m in 1..=12i64 {
                let dim = crate::ts::days_in_month(y0 + y, m);
                for d in 1..=dim {
                    days[((y * 12 + m - 1) * 32 + d) as usize] = crate::ts::days_from_civil(y0 + y, m, d) as i32;
                }
            }
        }
        Tsp { y0, ny, days }
    }

    /// `YYYY-MM-DDTHH:MM:SS` at the start of `s` (at least 19 bytes).
    #[inline(always)]
    fn civil19(&self, s: &[u8]) -> Option<i64> {
        const DATE: u64 = u64::from_le_bytes(*b"0000-00-");
        const DATE_DIGITS: u64 = u64::from_le_bytes([0xFF, 0xFF, 0xFF, 0xFF, 0, 0xFF, 0xFF, 0]);
        const TIME: u64 = u64::from_le_bytes(*b"00:00:00");
        const TIME_DIGITS: u64 = u64::from_le_bytes([0xFF, 0xFF, 0, 0xFF, 0xFF, 0, 0xFF, 0xFF]);
        let x = le8(s, 0) ^ DATE;
        let t = le8(s, 11) ^ TIME;
        let bad = |x: u64, dl: u64| {
            (x & 0xF0F0_F0F0_F0F0_F0F0) | (x.wrapping_add(0x7676_7676_7676_7676) & 0x8080_8080_8080_8080 & dl) | (x & !dl)
        };
        let d1 = s[8].wrapping_sub(b'0');
        let d2 = s[9].wrapping_sub(b'0');
        if (bad(x, DATE_DIGITS) | bad(t, TIME_DIGITS)) != 0 || s[10] != b'T' || d1 > 9 || d2 > 9 {
            return None;
        }
        let dp = x.wrapping_mul(10).wrapping_add(x >> 8);
        let tp = t.wrapping_mul(10).wrapping_add(t >> 8);
        let year = (dp & 0xFF) as i64 * 100 + ((dp >> 16) & 0xFF) as i64;
        let month = ((dp >> 40) & 0xFF) as i64;
        let day = (d1 * 10 + d2) as i64;
        let (h, mi, se) = ((tp & 0xFF) as i64, ((tp >> 24) & 0xFF) as i64, ((tp >> 48) & 0xFF) as i64);
        let yi = year - self.y0;
        if yi < 0 || yi >= self.ny || !(1..=12).contains(&month) || day > 31 || h > 23 || mi > 59 || se > 59 {
            return None;
        }
        let dd = self.days[((yi * 12 + month - 1) * 32 + day) as usize];
        if dd == i32::MIN {
            return None;
        }
        Some(dd as i64 * DAYMS + ((h * 60 + mi) * 60 + se) * 1000)
    }

    /// `ingest_ts`, which rules.md guarantees is valid F1: whole seconds,
    /// relative to `base_s`, without validating the digits.
    #[inline(always)]
    fn ingest_rel(&self, s: &[u8], base_s: i64) -> Option<i64> {
        if s.len() != 20 {
            return None;
        }
        let x = le8(s, 0) ^ u64::from_le_bytes(*b"0000-00-");
        let t = le8(s, 11) ^ u64::from_le_bytes(*b"00:00:00");
        let dp = x.wrapping_mul(10).wrapping_add(x >> 8);
        let tp = t.wrapping_mul(10).wrapping_add(t >> 8);
        let year = (dp & 0xFF) as i64 * 100 + ((dp >> 16) & 0xFF) as i64;
        let month = ((dp >> 40) & 0xFF) as i64;
        let day = ((s[8].wrapping_sub(b'0')) * 10 + s[9].wrapping_sub(b'0')) as i64;
        let yi = year - self.y0;
        if yi < 0 || yi >= self.ny || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
            return None;
        }
        let dd = self.days[((yi * 12 + month - 1) * 32 + day) as usize];
        if dd == i32::MIN {
            return None;
        }
        let secs = (tp & 0xFF) as i64 * 3600 + ((tp >> 24) & 0xFF) as i64 * 60 + ((tp >> 48) & 0xFF) as i64;
        Some(dd as i64 * 86_400 + secs - base_s)
    }

    #[inline(always)]
    pub(crate) fn parse(&self, s: &[u8]) -> Option<i64> {
        let n = s.len();
        if n == 20 {
            if s[19] != b'Z' {
                return None;
            }
            return self.civil19(s);
        }
        if n == 25 {
            if (s[19] != b'+' && s[19] != b'-') || s[22] != b':' {
                return None;
            }
            let (a, b, c, d) = (s[20].wrapping_sub(b'0'), s[21].wrapping_sub(b'0'), s[23].wrapping_sub(b'0'), s[24].wrapping_sub(b'0'));
            if a > 9 || b > 9 || c > 9 || d > 9 {
                return None;
            }
            let (oh, om) = ((a * 10 + b) as i64, (c * 10 + d) as i64);
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 60 + om) * 60_000;
            let local = self.civil19(s)?;
            return Some(if s[19] == b'+' { local - off } else { local + off });
        }
        crate::ts::parse(s)
    }
}

// ---------------------------------------------------------------- records

/// An event copy. lo: ts 31 | type 2 | cat 5 | price 24. hi: slot low 16 |
/// ingest 22 (seconds from the ingest base, never 0) | user 26.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct ERec {
    lo: u64,
    hi: u64,
}

/// A click. lo: id key 48 | campaign 12. hi: ts 31 (from window_start - 7d) | user 26.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct CRec {
    lo: u64,
    hi: u64,
}

#[derive(Clone, Copy)]
struct Ev {
    ts: u32,
    pay: u32,
    alias: u8,
}

#[derive(Clone, Copy)]
struct Cl {
    cts: u32,
    camp: u16,
    alias: u8,
    key: u64,
}

struct OddEv {
    eid: Vec<u8>,
    ingest: u32,
    ts: u32,
    pay: u32,
    user: u64,
}

/// Rows for the general path. `key` is a user number (p << 9 | alias) or
/// REF | a reference to a string in a parse thread's side table.
#[derive(Clone, Copy)]
struct XEv {
    key: u64,
    ts: u32,
    pay: u32,
}

#[derive(Clone, Copy)]
struct XCl {
    key: u64,
    cts: u32,
    camp: u16,
    /// Click ID key (49 bits), or REF | a side-table reference.
    ck: u64,
}

const ODD: u32 = 1 << 30;

struct Shared {
    uk: u64,
    ainv: u64,
    binv: u64,
    p_cap: usize,
    n_slots: usize,
    ing_base: i64,
    pstate: Vec<AtomicU32>,
    labels: Vec<AtomicU64>,
    abort: AtomicBool,
}

impl Shared {
    #[inline(always)]
    fn flag(&self, p: usize) {
        if p < self.p_cap {
            self.pstate[p].fetch_or(COMPLEX, Relaxed);
        }
    }
    /// (person, alias) of a canonical `u` ID, any alias.
    #[inline(always)]
    fn user(&self, s: &[u8]) -> Option<(usize, u32)> {
        if s.len() < 2 || s[0] != b'u' {
            return None;
        }
        let v = dec(unx(b36(&s[1..])?, self.binv), self.uk, self.ainv);
        if v >= 1 << 39 {
            return None;
        }
        let p = (v >> 9) as usize;
        (p < self.p_cap).then_some((p, (v & 511) as u32))
    }
    /// `user` for a field inside a mapping that ends at `end`.
    #[inline(always)]
    fn user_at(&self, s: &[u8], end: *const u8) -> Option<(usize, u32)> {
        if s.len() < 2 || s[0] != b'u' {
            return None;
        }
        let r = &s[1..];
        let v = dec(unx(scan::b36(r, scan::load8(r, end))?, self.binv), self.uk, self.ainv);
        if v >= 1 << 39 {
            return None;
        }
        let p = (v >> 9) as usize;
        (p < self.p_cap).then_some((p, (v & 511) as u32))
    }
    fn uname(&self, p: usize, a: u32, out: &mut Vec<u8>) {
        out.push(b'u');
        push_b36(out, scramble((p as u64) << 9 | a as u64, self.uk));
    }
    #[inline(always)]
    fn label(&self, p: usize, a: u32) -> u32 {
        (((self.labels[p].load(Relaxed) >> (4 * a)) & 15) as u32) ^ a
    }
    fn union(&self, p: usize, a: u32, b: u32) {
        let cell = &self.labels[p];
        let mut cur = cell.load(Relaxed);
        loop {
            let lab = |w: u64, i: u32| (((w >> (4 * i)) & 15) as u32) ^ i;
            let (la, lb) = (lab(cur, a), lab(cur, b));
            if la == lb {
                return;
            }
            let (lo, hi) = (la.min(lb), la.max(lb));
            let mut w = cur;
            for i in 0..16u32 {
                if lab(cur, i) == hi {
                    w = (w & !(15u64 << (4 * i))) | (((lo ^ i) as u64) << (4 * i));
                }
            }
            match cell.compare_exchange_weak(cur, w, Relaxed, Relaxed) {
                Ok(_) => return,
                Err(x) => cur = x,
            }
        }
    }
}

// ---------------------------------------------------------------- aggregation

struct Agg {
    names: Vec<u8>,
    name_ends: Vec<u32>,
    days: Vec<i32>,
    sessions: Vec<i32>,
    events: Vec<i32>,
    revenue: Vec<i64>,
    top: Vec<u8>,
    top_ends: Vec<u32>,
    top_defs: Vec<i16>,
    /// (campaign incl. organic) x day: sessions, purchasing, revenue, assisted.
    camp: Vec<[i64; 4]>,
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u16>,
    catsum: Vec<i64>,
    touched: Vec<u8>,
    ev: Vec<Ev>,
    ev2: Vec<Ev>,
    cl: Vec<Cl>,
    cl2: Vec<Cl>,
    name: Vec<u8>,
    tmp: Vec<u8>,
}

impl Agg {
    fn new(ctx: &Ctx) -> Self {
        let nc = ctx.camp_ids.len() + 1;
        Agg {
            names: Vec::new(),
            name_ends: Vec::new(),
            days: Vec::new(),
            sessions: Vec::new(),
            events: Vec::new(),
            revenue: Vec::new(),
            top: Vec::new(),
            top_ends: Vec::new(),
            top_defs: Vec::new(),
            camp: vec![[0; 4]; nc * ctx.ndays],
            cnt: vec![0; nc],
            pos: vec![0; nc],
            active: Vec::new(),
            catsum: vec![0; ctx.cat_names.len()],
            touched: Vec::new(),
            ev: Vec::new(),
            ev2: Vec::new(),
            cl: Vec::new(),
            cl2: Vec::new(),
            name: Vec::new(),
            tmp: Vec::new(),
        }
    }

    #[inline(always)]
    fn add(&mut self, c: u16) {
        let ci = c as usize;
        if self.cnt[ci] == 0 {
            self.pos[ci] = self.active.len() as u32;
            self.active.push(c);
        }
        self.cnt[ci] += 1;
    }

    #[inline(always)]
    fn remove(&mut self, c: u16) {
        let ci = c as usize;
        self.cnt[ci] -= 1;
        if self.cnt[ci] == 0 {
            let at = self.pos[ci] as usize;
            let last = self.active.pop().unwrap();
            if last != c {
                self.active[at] = last;
                self.pos[last as usize] = at as u32;
            }
        }
    }

    fn flush_day(&mut self, ctx: &Ctx, name: &[u8], d: usize, s: i32, n: i32, rev: i64) {
        self.names.extend_from_slice(name);
        self.name_ends.push(self.names.len() as u32);
        self.days.push((ctx.day0 + d as i64) as i32);
        self.sessions.push(s);
        self.events.push(n);
        self.revenue.push(rev);
        if self.touched.is_empty() {
            self.top_defs.push(0);
        } else {
            let mut best = self.touched[0];
            for &c in &self.touched {
                let (v, bv) = (self.catsum[c as usize], self.catsum[best as usize]);
                if v > bv || (v == bv && c < best) {
                    best = c;
                }
            }
            for &c in &self.touched {
                self.catsum[c as usize] = 0;
            }
            self.touched.clear();
            self.top.push(best);
            self.top_defs.push(1);
        }
    }

    /// One person: events sorted by ts, clicks sorted by (ts, id) without
    /// duplicates, both in self.ev2 / self.cl2.
    fn person(&mut self, ctx: &Ctx, name_buf: &[u8]) {
        let ev = std::mem::take(&mut self.ev2);
        let cl = std::mem::take(&mut self.cl2);
        let organic = ctx.camp_ids.len();
        let nd = ctx.ndays;
        let mut i = 0;
        let (mut lo, mut hi) = (0usize, 0usize);
        let mut cur_d = usize::MAX;
        let (mut ds, mut dn, mut drev) = (0i32, 0i32, 0i64);
        while i < ev.len() {
            let start = ev[i].ts;
            let d = ((ctx.ws + start as i64).div_euclid(DAYMS) - ctx.day0) as usize;
            if d != cur_d {
                if cur_d != usize::MAX {
                    self.flush_day(ctx, name_buf, cur_d, ds, dn, drev);
                }
                cur_d = d;
                ds = 0;
                dn = 0;
                drev = 0;
            }
            let mut last = start;
            let (mut n, mut rev, mut bought) = (0i32, 0i64, false);
            while i < ev.len() && ev[i].ts - last <= GAP {
                last = ev[i].ts;
                n += 1;
                let pay = ev[i].pay;
                if pay & 3 == T_PURCHASE {
                    let price = (pay >> 7) as i64;
                    let cat = ((pay >> 2) & 31) as usize;
                    rev += price;
                    bought = true;
                    if self.catsum[cat] == 0 {
                        self.touched.push(cat as u8);
                    }
                    self.catsum[cat] += price;
                }
                i += 1;
            }
            ds += 1;
            dn += n;
            drev += rev;
            let sc = start + REACH;
            while hi < cl.len() && cl[hi].cts <= sc {
                self.add(cl[hi].camp);
                hi += 1;
            }
            while lo < hi && cl[lo].cts < start {
                // cts + REACH < sc  <=>  cts < start
                self.remove(cl[lo].camp);
                lo += 1;
            }
            let credit = if hi > lo {
                let t = cl[hi - 1].cts;
                let mut j = hi - 1;
                while j > lo && cl[j - 1].cts == t {
                    j -= 1;
                }
                cl[j].camp as usize
            } else {
                organic
            };
            for k in 0..self.active.len() {
                let c = self.active[k] as usize;
                self.camp[c * nd + d][3] += 1;
            }
            let row = &mut self.camp[credit * nd + d];
            row[0] += 1;
            row[1] += bought as i64;
            row[2] += rev;
        }
        if cur_d != usize::MAX {
            self.flush_day(ctx, name_buf, cur_d, ds, dn, drev);
        }
        for k in 0..self.active.len() {
            self.cnt[self.active[k] as usize] = 0;
        }
        self.active.clear();
        self.ev2 = ev;
        self.cl2 = cl;
    }
}

// ---------------------------------------------------------------- run

/// Fault a mapped range in with one call instead of one trap per page.
#[inline]
fn populate(s: &[u8]) {
    #[cfg(target_os = "linux")]
    unsafe {
        extern "C" {
            fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
        }
        let a = s.as_ptr() as usize & !4095;
        let z = s.as_ptr() as usize + s.len();
        madvise(a as *mut u8, z - a, 22);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = s;
}

fn sample_body(m: &[u8], max: usize) -> &[u8] {
    let b = body(m);
    if b.len() <= max {
        return b;
    }
    match memchr::memrchr(b'\n', &b[..max]) {
        Some(i) => &b[..i + 1],
        None => b,
    }
}

/// Newline-aligned (file, start, end) ranges of every file body.
fn chunks_s(maps: &[&[u8]]) -> Vec<(usize, usize, usize)> {
    let mut out = Vec::new();
    for (fi, m) in maps.iter().enumerate() {
        let len = m.len();
        let mut pos = len - body(m).len();
        while pos < len {
            let mut end = (pos + (2 << 20)).min(len);
            if end < len {
                end = match memchr::memchr(b'\n', &m[end..]) {
                    Some(i) => end + i + 1,
                    None => len,
                };
            }
            out.push((fi, pos, end));
            pos = end;
        }
    }
    out
}

/// Every input file's bytes: mapped, or (FAST_READ) read in parallel into
/// one huge-page buffer, which is cheaper to tear down at exit.
fn load_inputs(dirs: &[&Path], threads: usize) -> Vec<Vec<&'static [u8]>> {
    let lists: Vec<Vec<std::path::PathBuf>> = dirs.iter().map(|d| list_csv(d)).collect();
    if std::env::var_os("FAST_READ").is_none() {
        return lists
            .iter()
            .map(|l| {
                let maps: &'static Vec<Mmap> = Box::leak(Box::new(map_files(l)));
                maps.iter().map(|m| &m[..]).collect()
            })
            .collect();
    }
    let mut files: Vec<(usize, std::path::PathBuf, usize)> = Vec::new();
    for (k, l) in lists.iter().enumerate() {
        for p in l {
            let n = std::fs::metadata(p).map(|m| m.len() as usize).unwrap_or(0);
            if n > 0 {
                files.push((k, p.clone(), n));
            }
        }
    }
    let total: usize = files.iter().map(|f| f.2.next_multiple_of(64) + 64).sum();
    let mut region = memmap2::MmapMut::map_anon(total.max(64)).expect("input buffer");
    #[cfg(target_os = "linux")]
    let _ = region.advise(memmap2::Advice::HugePage);
    let base = region.as_mut_ptr() as usize;
    std::mem::forget(region);
    let mut offs = Vec::new();
    let mut at = 0usize;
    for f in &files {
        offs.push(at);
        at += f.2.next_multiple_of(64) + 64;
    }
    const PIECE: usize = 8 << 20;
    let mut pieces: Vec<(usize, usize, usize)> = Vec::new();
    for (i, f) in files.iter().enumerate() {
        let mut a = 0;
        while a < f.2 {
            let z = (a + PIECE).min(f.2);
            pieces.push((i, a, z));
            a = z;
        }
    }
    pieces.sort_by_key(|p| std::cmp::Reverse(p.2 - p.1));
    let next = AtomicUsize::new(0);
    thread::scope(|sc| {
        for _ in 0..threads {
            sc.spawn(|| {
                use std::os::unix::fs::FileExt;
                let mut open: Option<(usize, std::fs::File)> = None;
                loop {
                    let t = next.fetch_add(1, Relaxed);
                    let Some(&(i, a, z)) = pieces.get(t) else { break };
                    if open.as_ref().map(|o| o.0) != Some(i) {
                        open = Some((i, std::fs::File::open(&files[i].1).expect("open input")));
                    }
                    let dst = unsafe { std::slice::from_raw_parts_mut((base + offs[i] + a) as *mut u8, z - a) };
                    open.as_ref().unwrap().1.read_exact_at(dst, a as u64).expect("read input");
                }
            });
        }
    });
    let mut out: Vec<Vec<&'static [u8]>> = vec![Vec::new(); dirs.len()];
    for (i, f) in files.iter().enumerate() {
        out[f.0].push(unsafe { std::slice::from_raw_parts((base + offs[i]) as *const u8, f.2) });
    }
    out
}

pub fn run(ctx: &Ctx, data: &Path, outdir: &Path, threads: usize, profile: bool) -> bool {
    let t0 = Instant::now();
    let lap = |what: &str| {
        if profile {
            eprintln!("{:>8.3}s fast: {what}", t0.elapsed().as_secs_f64());
        }
    };
    if ctx.cat_names.len() > 32 || ctx.camp_ids.len() >= 4095 || ctx.we - ctx.ws > 24 * DAYMS {
        return false;
    }
    let (ed, cd, ld) = (data.join("events"), data.join("clicks"), data.join("links"));
    let mut inputs = load_inputs(&[&ed, &cd, &ld], threads).into_iter();
    let event_files = inputs.next().unwrap();
    let click_files = inputs.next().unwrap();
    let link_files = inputs.next().unwrap();
    lap("inputs");
    if event_files.is_empty() || link_files.is_empty() {
        return false;
    }
    let ainv = inv40(MA);
    let binv = inv40(MB);

    // User key from link pairs: one person's IDs differ only in the low 9 bits.
    let mut lpairs = Vec::new();
    let mut lids = Vec::new();
    'outer: for &m in &link_files {
        csv::scan::<2>(sample_body(m, 1 << 20), |f| {
            if lpairs.len() >= 3000 {
                return;
            }
            let x = |s: &[u8]| if s.len() > 1 && s[0] == b'u' { b36(&s[1..]).map(|o| unx(o, binv)) } else { None };
            if let (Some(a), Some(b)) = (x(f[0]), x(f[1])) {
                if a != b {
                    lpairs.push((a, b));
                    lids.push(a);
                }
            }
        });
        if lpairs.len() >= 3000 {
            break 'outer;
        }
    }
    let ucheck = |k: u64| lids.iter().filter(|&&x| { let v = dec(x, k, ainv); v < 1 << 31 && (v & 511) < 64 }).count();
    let Some((uk, uc)) = recover(&lpairs, 9, ainv, &ucheck) else { return false };
    if uc * 100 < lids.len() * 97 {
        return false;
    }
    lap("user key");

    // Event key: events of one person have consecutive counters in time order.
    let big = (0..event_files.len()).max_by_key(|&i| event_files[i].len()).unwrap();
    let sample = sample_body(event_files[big], 6 << 20);
    let mut rows: Vec<(u64, i64, u64)> = Vec::new();
    let mut tsc = ts::DateCache::new();
    let mut sample_lines = 0usize;
    csv::scan::<7>(sample, |f| {
        sample_lines += 1;
        if f[0].len() < 2 || f[0][0] != b'e' || f[1].len() < 2 || f[1][0] != b'u' {
            return;
        }
        let (Some(e), Some(u)) = (b36(&f[0][1..]), b36(&f[1][1..])) else { return };
        let Some(t) = ts::parse_c(f[2], &mut tsc) else { return };
        let v = dec(unx(u, binv), uk, ainv);
        rows.push((v >> 9, t, unx(e, binv)));
    });
    if sample_lines < 1000 {
        return false;
    }
    let lines_est = (event_files.iter().map(|m| m.len()).sum::<usize>() as f64 * sample_lines as f64
        / sample.len() as f64) as usize;
    rows.sort_unstable();
    let mut epairs = Vec::new();
    for w in rows.windows(2) {
        if w[0].0 == w[1].0 && w[0].2 != w[1].2 && w[0].1 < w[1].1 {
            epairs.push((w[0].2, w[1].2));
        }
    }
    let n_slots = ((lines_est as f64 * 1.1) as usize + (1 << EB)).next_multiple_of(1 << EB);
    let exs: Vec<u64> = rows.iter().map(|r| r.2).take(4000).collect();
    let echeck = |k: u64| exs.iter().filter(|&&x| (dec(x, k, ainv) as usize) < n_slots).count();
    // The event key is recovered while links and clicks are parsed.
    let ek_cell: OnceLock<Option<u64>> = OnceLock::new();
    let find_ek = || {
        let r = recover(&epairs, 12, ainv, &echeck).filter(|&(_, c)| c * 100 >= exs.len() * 99).map(|r| r.0);
        let _ = ek_cell.set(r);
    };
    let p_max = rows.iter().map(|r| r.0).filter(|&p| p < 1 << 22).max().unwrap_or(0) as usize;
    let p_cap = (n_slots / 24 + 4096).max(p_max * 2).min(1 << 22);
    lap("event key");

    let sh = Shared {
        uk,
        ainv,
        binv,
        p_cap,
        n_slots,
        ing_base: ctx.ws.div_euclid(1000) - 86_400,
        pstate: (0..p_cap).map(|_| AtomicU32::new(0)).collect(),
        labels: (0..p_cap).map(|_| AtomicU64::new(0)).collect(),
        abort: AtomicBool::new(false),
    };
    let n_eb = n_slots >> EB;
    let n_cb = (p_cap >> CB) + 1;
    let event_bytes: usize = event_files.iter().map(|m| m.len()).sum();
    let click_bytes: usize = click_files.iter().map(|m| m.len()).sum();
    let ev_region = Region::new(event_bytes / 40 * 16 + Sink::<ERec>::slack(n_eb) * threads);
    let cl_region = Region::new(click_bytes / 28 * 16 + Sink::<CRec>::slack(n_cb) * threads);
    let ec = chunks_s(&event_files);
    let cc = chunks_s(&click_files);
    let lc = chunks_s(&link_files);
    let (n_lc, n_cc) = (lc.len(), cc.len());
    lap("setup");

    // ------------------------------------------------ pass 1: parse
    struct P1<'r> {
        tid: usize,
        idx: index::Index,
        tsc: ts::DateCache,
        ev: Sink<'r, ERec>,
        cl: Sink<'r, CRec>,
        esc_b: Vec<u8>,
        esc_e: Vec<u32>,
        odd: Vec<OddEv>,
        xcl: Vec<XCl>,
        hard: Vec<(u64, u64)>,
        cpu: [f64; 3],
    }
    impl P1<'_> {
        fn side(&mut self, s: &[u8], sh: &Shared) -> u64 {
            let i = self.esc_e.len();
            if i >= 1 << 21 {
                sh.abort.store(true, Relaxed);
                return 0;
            }
            self.esc_b.extend_from_slice(s);
            self.esc_e.push(self.esc_b.len() as u32);
            (self.tid as u64) << 21 | i as u64
        }
    }
    let scan_only = std::env::var_os("FAST_SCANONLY").is_some();
    let km: u32 = std::env::var("FAST_K").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let tp = Tsp::new(ctx.ws, ctx.we);
    let tid_next = AtomicUsize::new(0);
    let p1: Vec<P1> = thread::scope(|scope| {
    scope.spawn(find_ek);
    pool_finish(
        threads,
        n_lc + n_cc + ec.len(),
        || P1 {
            tid: tid_next.fetch_add(1, Relaxed),
            idx: index::Index::new(),
            tsc: ts::DateCache::new(),
            ev: Sink::new(&ev_region, n_eb),
            cl: Sink::new(&cl_region, n_cb),
            esc_b: Vec::new(),
            esc_e: Vec::new(),
            odd: Vec::new(),
            xcl: Vec::new(),
            hard: Vec::new(),
            cpu: [0.0; 3],
        },
        |st, t| {
            let tt = Instant::now();
            let cat = if t < n_lc { 0 } else if t < n_lc + n_cc { 1 } else { 2 };
            if scan_only && cat == 2 {
                let (fi, a, b) = ec[t - n_lc - n_cc];
                let mut n = 0usize;
                populate(&event_files[fi][a..b]);
                if std::env::var("FAST_SCANONLY").as_deref() == Ok("2") {
                    let d = &event_files[fi][a..b];
                    let mut x = 0u64;
                    for c in d.chunks_exact(8) {
                        x = x.wrapping_add(u64::from_le_bytes(c.try_into().unwrap()));
                    }
                    n = x as usize & 1;
                } else {
                    scan::lines::<7>(&event_files[fi][a..b], |f| n += f[0].len());
                }
                st.esc_e.push(n as u32);
            } else if t < n_lc {
                let (fi, a, b) = lc[t];
                let slice = &link_files[fi][a..b];
                let end = link_files[fi].as_ptr_range().end;
                populate(slice);
                let mut idx = std::mem::replace(&mut st.idx, index::Index::new());
                scan::lines::<2>(slice, |f| {
                    if f[0].is_empty() || f[1].is_empty() {
                        return;
                    }
                    match (sh.user_at(f[0], end), sh.user_at(f[1], end)) {
                        (Some((pa, aa)), Some((pb, ab))) if pa == pb && aa < 16 && ab < 16 => {
                            sh.pstate[pa].fetch_or(1 << aa | 1 << ab, Relaxed);
                            if aa != ab {
                                sh.union(pa, aa, ab);
                            }
                        }
                        (ua, ub) => {
                            if let Some((p, _)) = ua {
                                sh.flag(p);
                            }
                            if let Some((p, _)) = ub {
                                sh.flag(p);
                            }
                            let ka = match ua { Some((p, a)) => (p as u64) << 9 | a as u64, None => REF | st.side(f[0], &sh) };
                            let kb = match ub { Some((p, a)) => (p as u64) << 9 | a as u64, None => REF | st.side(f[1], &sh) };
                            st.hard.push((ka, kb));
                        }
                    }
                });
                st.idx = idx;
            } else if t < n_lc + n_cc {
                let (fi, a, b) = cc[t - n_lc];
                let slice = &click_files[fi][a..b];
                let end = click_files[fi].as_ptr_range().end;
                populate(slice);
                let (cws, we) = (ctx.cws, ctx.we);
                let mut idx = std::mem::replace(&mut st.idx, index::Index::new());
                scan::lines::<4>(slice, |f| {
                    if f[0].is_empty() || f[1].is_empty() {
                        return;
                    }
                    let Some(t) = tp.parse(f[3]) else { return };
                    if t < cws || t >= we {
                        return;
                    }
                    let Some(camp) = ctx.campaigns.get(f[2]) else { return };
                    let cts = (t - cws) as u32;
                    let u = sh.user_at(f[1], end);
                    match (u, ckey(f[0])) {
                        (Some((p, a)), Some(k)) if a < 8 => {
                            let user = (p as u64) << 3 | a as u64;
                            st.cl.push(p >> CB, CRec { lo: k | (camp as u64) << 49, hi: cts as u64 | user << 31 });
                        }
                        (u, k) => {
                            let key = match u {
                                Some((p, a)) => {
                                    sh.flag(p);
                                    (p as u64) << 9 | a as u64
                                }
                                None => REF | st.side(f[1], &sh),
                            };
                            let ck = match k { Some(k) => k, None => REF | st.side(f[0], &sh) };
                            st.xcl.push(XCl { key, cts, camp: camp as u16, ck });
                        }
                    }
                });
                st.idx = idx;
            } else {
                let (fi, a, b) = ec[t - n_lc - n_cc];
                let slice = &event_files[fi][a..b];
                let end = event_files[fi].as_ptr_range().end;
                populate(slice);
                let (ws, we) = (ctx.ws, ctx.we);
                let Some(ek_) = *ek_cell.wait() else {
                    sh.abort.store(true, Relaxed);
                    return;
                };
                let mut idx = std::mem::replace(&mut st.idx, index::Index::new());
                scan::lines::<7>(slice, |f| {
                    if f[0].is_empty() {
                        return;
                    }
                    let ing_rel = if km & 2 != 0 {
                        f[3].len() as i64 + 100
                    } else {
                        match tp.ingest_rel(f[3], sh.ing_base) {
                            Some(v) => v,
                            None => {
                                sh.abort.store(true, Relaxed);
                                return;
                            }
                        }
                    };
                    if ing_rel <= 0 || ing_rel >= 1 << 22 {
                        sh.abort.store(true, Relaxed);
                        return;
                    }
                    let ing = (ing_rel + sh.ing_base) * 1000;
                    // Validity of this copy.
                    let mut lo = T_INVALID << 31;
                    let mut valid_ts = 0u32;
                    'v: {
                        if f[1].is_empty() || f[5].is_empty() {
                            break 'v;
                        }
                        let t = if km & 2 != 0 {
                            ws + f[2].len() as i64
                        } else {
                            let Some(t) = tp.parse(f[2]) else { break 'v };
                            t
                        };
                        if t < ws || t >= we || ing - t > DAYMS {
                            break 'v;
                        }
                        let ty4 = f[4];
                        let (ty, cat, price) = if ty4.len() == 4 {
                            let v = u32::from_le_bytes(ty4.try_into().unwrap());
                            if v == u32::from_le_bytes(*b"view") {
                                (0u64, 0u64, 0u64)
                            } else if v == u32::from_le_bytes(*b"cart") {
                                (1, 0, 0)
                            } else {
                                break 'v;
                            }
                        } else if ty4.len() == 8 && u64::from_le_bytes(ty4.try_into().unwrap()) == u64::from_le_bytes(*b"purchase") {
                            match crate::parse_price(f[6]) {
                                Some(p) => (2, ctx.products.get(f[5]).unwrap_or(ctx.unknown_cat) as u64, p as u64),
                                None => break 'v,
                            }
                        } else {
                            break 'v;
                        };
                        valid_ts = (t - ws) as u32;
                        lo = valid_ts as u64 | ty << 31 | cat << 33 | price << 38;
                    }
                    let valid = (lo >> 31) & 3 != T_INVALID;
                    let mut user = 0u64;
                    if valid && km & 4 != 0 {
                        user = f[1].len() as u64;
                    } else if valid {
                        match sh.user_at(f[1], end) {
                            Some((p, a)) if a < 8 => user = (p as u64) << 3 | a as u64,
                            u => {
                                if let Some((p, _)) = u {
                                    sh.flag(p);
                                }
                                user = ESC | st.side(f[1], &sh);
                            }
                        }
                    }
                    let slot = if km & 4 != 0 {
                        Some(f[0].len() * 4099 % sh.n_slots)
                    } else if f[0].len() > 1 && f[0][0] == b'e' {
                        scan::b36(&f[0][1..], scan::load8(&f[0][1..], end)).map(|o| dec(unx(o, sh.binv), ek_, sh.ainv) as usize).filter(|&v| v < sh.n_slots)
                    } else {
                        None
                    };
                    match slot {
                        Some(v) => {
                            let r = ERec { lo, hi: (v as u64 & 0xFFFF) | (ing_rel as u64) << 16 | user << 38 };
                            if km & 1 != 0 {
                                st.cpu[0] += std::hint::black_box(r.lo ^ r.hi) as f64 * 0.0;
                            } else {
                                st.ev.push(v >> EB, r);
                            }
                        }
                        None => {
                            st.odd.push(OddEv {
                                eid: f[0].to_vec(),
                                ingest: ing_rel as u32,
                                ts: valid_ts,
                                pay: (lo >> 31) as u32,
                                user,
                            });
                        }
                    }
                });
                st.idx = idx;
            }
            st.cpu[cat] += tt.elapsed().as_secs_f64();
        },
        |st, _| {
            st.ev.finish();
            st.cl.finish();
        },
    )
    });
    lap("parse");
    if profile {
        let c: Vec<f64> = (0..3).map(|i| p1.iter().map(|s| s.cpu[i]).sum()).collect();
        eprintln!("parse cpu: links {:.3} clicks {:.3} events {:.3}", c[0], c[1], c[2]);
    }
    if scan_only || km != 0 {
        if profile {
            let e = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
            eprintln!("epoch main end {:.3}", e.as_secs_f64());
        }
        std::process::exit(0);
    }
    if sh.abort.load(Relaxed) {
        return false;
    }
    let mut p1 = p1;
    p1.sort_by_key(|s| s.tid);

    // ------------------------------------------------ pass 2: clicks grouped by person
    let mut cb_base = vec![0usize; n_cb + 1];
    for b in 0..n_cb {
        cb_base[b + 1] = cb_base[b] + p1.iter().map(|s| s.cl.len(b)).sum::<usize>();
    }
    let n_clicks = cb_base[n_cb];
    let mut click_sorted: Vec<CRec> = Vec::with_capacity(n_clicks);
    #[allow(clippy::uninit_vec)]
    unsafe {
        click_sorted.set_len(n_clicks)
    };
    let cs_ptr = click_sorted.as_mut_ptr() as usize;
    let off: Vec<AtomicU32> = (0..=p_cap).map(|_| AtomicU32::new(0)).collect();
    struct P2 {
        xcl: Vec<(u64, u64)>,
        cnt: Vec<u32>,
    }
    let p2: Vec<P2> = pool_finish(
        threads,
        n_cb,
        || P2 { xcl: Vec::new(), cnt: vec![0; 1 << CB] },
        |st, b| {
            let pbase = b << CB;
            st.cnt.iter_mut().for_each(|c| *c = 0);
            for s in &p1 {
                for sl in s.cl.slices(b) {
                    for r in sl {
                        let p = (r.hi >> 34) as usize;
                        st.cnt[p - pbase] += 1;
                    }
                }
            }
            let mut acc = cb_base[b] as u32;
            for i in 0..(1usize << CB) {
                let c = st.cnt[i];
                st.cnt[i] = acc;
                if pbase + i < p_cap {
                    off[pbase + i].store(acc, Relaxed);
                }
                acc += c;
            }
            if b == n_cb - 1 {
                off[p_cap].store(acc, Relaxed);
            }
            let dst = cs_ptr as *mut CRec;
            for s in &p1 {
                for sl in s.cl.slices(b) {
                    for r in sl {
                        let p = (r.hi >> 34) as usize;
                        let i = &mut st.cnt[p - pbase];
                        unsafe { *dst.add(*i as usize) = *r };
                        *i += 1;
                    }
                }
            }
            // Clicks of complex persons go to the general path.
            for i in 0..(1usize << CB).min(p_cap.saturating_sub(pbase)) {
                let p = pbase + i;
                if sh.pstate[p].load(Relaxed) & COMPLEX != 0 {
                    let (a, z) = (off[p].load(Relaxed) as usize, st.cnt[i] as usize);
                    for j in a..z {
                        let r = unsafe { *dst.add(j) };
                        st.xcl.push((r.lo, r.hi));
                    }
                }
            }
        },
        |_, _| {},
    );
    lap("clicks");

    // Odd event IDs: dedup by string; attach to their person's run.
    let mut gen_ev: Vec<XEv> = Vec::new();
    let mut odd_map: HashMap<usize, Vec<Ev>> = HashMap::new();
    {
        let mut odd: HashMap<Vec<u8>, OddEv> = HashMap::new();
        for s in p1.iter_mut() {
            for o in s.odd.drain(..) {
                match odd.get(&o.eid) {
                    Some(k) if k.ingest <= o.ingest => {}
                    _ => {
                        odd.insert(o.eid.clone(), o);
                    }
                }
            }
        }
        for (_, o) in odd {
            if o.pay & 3 == T_INVALID as u32 {
                continue;
            }
            if o.user & ESC != 0 {
                gen_ev.push(XEv { key: REF | (o.user & (ESC - 1)), ts: o.ts, pay: o.pay });
            } else {
                let (p, a) = ((o.user >> 3) as usize, (o.user & 7) as u8);
                sh.pstate[p].fetch_or(ODD, Relaxed);
                odd_map.entry(p).or_default().push(Ev { ts: o.ts, pay: o.pay, alias: a });
            }
        }
    }

    // ------------------------------------------------ pass 3: per slot range, dedup then persons
    // Each range is deduped in a thread-local array (it fits in L2) and walked
    // in slot order. Runs that end inside the range are finished at once; the
    // first and last run may continue into the neighbouring ranges, so they
    // are kept and stitched afterwards.
    struct Run {
        p: u32,
        ev: Vec<Ev>,
    }
    struct Edge {
        b: usize,
        head: Run,
        tail: Option<Run>,
    }
    struct P3 {
        agg: Agg,
        xev: Vec<XEv>,
        edges: Vec<Edge>,
        local: Vec<ERec>,
    }
    let mut p3: Vec<P3> = pool_finish(
        threads,
        n_eb,
        || P3 { agg: Agg::new(ctx), xev: Vec::new(), edges: Vec::new(), local: vec![ERec::default(); 1 << EB] },
        |st, b| {
            let mut local = std::mem::take(&mut st.local);
            local.fill(ERec::default());
            for s in &p1 {
                for sl in s.ev.slices(b) {
                    for r in sl {
                        let at = unsafe { local.get_unchecked_mut((r.hi & 0xFFFF) as usize) };
                        if at.hi == 0 || (r.hi >> 16) & 0x3F_FFFF < (at.hi >> 16) & 0x3F_FFFF {
                            *at = *r;
                        }
                    }
                }
            }
            let mut cur = NONE;
            let mut head: Option<Run> = None;
            let mut buf: Vec<Ev> = Vec::new();
            for r in local.iter() {
                if r.hi == 0 || (r.lo >> 31) & 3 == T_INVALID {
                    continue;
                }
                let user = r.hi >> 38;
                if user & ESC != 0 {
                    st.xev.push(XEv { key: REF | (user & (ESC - 1)), ts: r.lo as u32 & 0x7FFF_FFFF, pay: (r.lo >> 31) as u32 });
                    continue;
                }
                let p = (user >> 3) as u32;
                if p != cur {
                    if cur != NONE {
                        if p < cur {
                            sh.abort.store(true, Relaxed);
                        }
                        if head.is_none() {
                            head = Some(Run { p: cur, ev: std::mem::take(&mut buf) });
                        } else {
                            finish_run(ctx, &sh, &click_sorted, &off, &odd_map, cur as usize, &mut buf, &mut st.agg, &mut st.xev);
                        }
                    }
                    cur = p;
                    buf.clear();
                }
                buf.push(Ev { ts: r.lo as u32 & 0x7FFF_FFFF, pay: (r.lo >> 31) as u32, alias: (user & 7) as u8 });
            }
            if cur != NONE {
                let last = Run { p: cur, ev: buf };
                match head {
                    None => st.edges.push(Edge { b, head: last, tail: None }),
                    Some(h) => st.edges.push(Edge { b, head: h, tail: Some(last) }),
                }
            }
            st.local = local;
        },
        |_, _| {},
    );
    // Stitch the runs that cross range boundaries.
    let mut edges: Vec<Edge> = p3.iter_mut().flat_map(|s| s.edges.drain(..)).collect();
    edges.sort_unstable_by_key(|e| e.b);
    let mut done: Vec<Run> = Vec::new();
    let mut open: Option<Run> = None;
    for e in edges {
        let h = e.head;
        open = match open {
            Some(mut o) if o.p == h.p => {
                o.ev.extend_from_slice(&h.ev);
                Some(o)
            }
            Some(o) => {
                if h.p < o.p {
                    return false;
                }
                done.push(o);
                Some(h)
            }
            None => Some(h),
        };
        if let Some(t) = e.tail {
            done.push(open.take().unwrap());
            open = Some(t);
        }
    }
    if let Some(o) = open {
        done.push(o);
    }
    let done: Vec<std::sync::Mutex<Run>> = done.into_iter().map(std::sync::Mutex::new).collect();
    let p3b: Vec<P3> = pool_finish(
        threads,
        done.len(),
        || P3 { agg: Agg::new(ctx), xev: Vec::new(), edges: Vec::new(), local: Vec::new() },
        |st, k| {
            let mut r = done[k].lock().unwrap();
            let p = r.p as usize;
            finish_run(ctx, &sh, &click_sorted, &off, &odd_map, p, &mut r.ev, &mut st.agg, &mut st.xev);
        },
        |_, _| {},
    );
    p3.extend(p3b);

    #[allow(clippy::too_many_arguments)]
    fn finish_run(
        ctx: &Ctx,
        sh: &Shared,
        cs: &[CRec],
        off: &[AtomicU32],
        odd_map: &HashMap<usize, Vec<Ev>>,
        p: usize,
        buf: &mut Vec<Ev>,
        agg: &mut Agg,
        xev: &mut Vec<XEv>,
    ) {
        let state = sh.pstate[p].load(Relaxed);
        if state & ODD != 0 {
            sh.pstate[p].fetch_and(!ODD, Relaxed);
            buf.extend_from_slice(&odd_map[&p]);
        }
        if buf.windows(2).any(|w| w[1].ts < w[0].ts) {
            buf.sort_by_key(|e| e.ts);
        }
        if state & COMPLEX != 0 {
            for e in buf.iter() {
                xev.push(XEv { key: (p as u64) << 9 | e.alias as u64, ts: e.ts, pay: e.pay });
            }
            return;
        }
        let present = state & 0xFFFF;
        let labels = sh.labels[p].load(Relaxed);
        let key = |a: u32| -> u32 {
            if present >> a & 1 != 0 {
                16 + ((((labels >> (4 * a)) & 15) as u32) ^ a)
            } else {
                a
            }
        };
        let (ca, cz) = (off[p].load(Relaxed) as usize, off[p + 1].load(Relaxed) as usize);
        agg.cl.clear();
        for r in &cs[ca..cz] {
            agg.cl.push(Cl { cts: r.hi as u32 & 0x7FFF_FFFF, camp: (r.lo >> 49) as u16, alias: ((r.hi >> 31) & 7) as u8, key: r.lo & CKM });
        }
        agg.cl.sort_unstable_by_key(|c| (c.cts, c.key));
        agg.cl.dedup_by_key(|c| (c.cts, c.key));
        let k0 = key(buf[0].alias as u32);
        let single = buf.iter().all(|e| key(e.alias as u32) == k0);
        let mut keys: Vec<u32> = if single { vec![k0] } else { buf.iter().map(|e| key(e.alias as u32)).collect() };
        keys.sort_unstable();
        keys.dedup();
        for &kk in &keys {
            agg.ev2.clear();
            agg.ev2.extend(buf.iter().filter(|e| single || key(e.alias as u32) == kk));
            agg.cl2.clear();
            let cl = std::mem::take(&mut agg.cl);
            agg.cl2.extend(cl.iter().filter(|c| key(c.alias as u32) == kk));
            agg.cl = cl;
            // Name: smallest ID of the component (or the lone ID).
            agg.name.clear();
            if kk >= 16 {
                for a in 0..16u32 {
                    if present >> a & 1 != 0 && key(a) == kk {
                        agg.tmp.clear();
                        sh.uname(p, a, &mut agg.tmp);
                        if agg.name.is_empty() || agg.tmp < agg.name {
                            std::mem::swap(&mut agg.name, &mut agg.tmp);
                        }
                    }
                }
            } else {
                sh.uname(p, kk, &mut agg.name);
            }
            let name = std::mem::take(&mut agg.name);
            agg.person(ctx, &name);
            agg.name = name;
        }
    }
    if sh.abort.load(Relaxed) {
        return false;
    }
    lap("persons");

    // ------------------------------------------------ general path for the rest
    let mut agg = Agg::new(ctx);
    {
        let side = |r: u64| -> &[u8] {
            let s = &p1[((r >> 21) & 15) as usize];
            let i = (r & ((1 << 21) - 1)) as usize;
            let a = if i == 0 { 0 } else { s.esc_e[i - 1] as usize };
            &s.esc_b[a..s.esc_e[i] as usize]
        };
        let mut xev = gen_ev;
        for s in p3.iter_mut() {
            xev.append(&mut s.xev);
        }
        for (&p, evs) in &odd_map {
            if sh.pstate[p].load(Relaxed) & ODD != 0 {
                xev.extend(evs.iter().map(|e| XEv { key: (p as u64) << 9 | e.alias as u64, ts: e.ts, pay: e.pay }));
            }
        }
        let mut xcl: Vec<XCl> = p1.iter().flat_map(|s| s.xcl.iter().copied()).collect();
        for s in &p2 {
            for &(lo, hi) in &s.xcl {
                let user = hi >> 31 & ((1 << 26) - 1);
                xcl.push(XCl { key: (user >> 3) << 9 | (user & 7), cts: hi as u32 & 0x7FFF_FFFF, camp: (lo >> 49) as u16, ck: lo & CKM });
            }
        }
        // Canonical node keys: user numbers, or interned strings (1 << 62 | i).
        let mut strs: HashMap<&[u8], u64> = HashMap::new();
        let mut str_list: Vec<&[u8]> = Vec::new();
        let mut canon = |k: u64| -> u64 {
            if k & REF == 0 {
                return k;
            }
            let s = side(k & !REF);
            if let Some((p, a)) = sh.user(s) {
                return (p as u64) << 9 | a as u64;
            }
            *strs.entry(s).or_insert_with(|| {
                str_list.push(s);
                (1u64 << 62) | (str_list.len() - 1) as u64
            })
        };
        struct Uf {
            ids: HashMap<u64, u32>,
            keys: Vec<u64>,
            parent: Vec<u32>,
        }
        impl Uf {
            fn id(&mut self, k: u64) -> u32 {
                if let Some(&i) = self.ids.get(&k) {
                    return i;
                }
                let i = self.parent.len() as u32;
                self.parent.push(i);
                self.keys.push(k);
                self.ids.insert(k, i);
                i
            }
            fn find(&mut self, mut x: u32) -> u32 {
                while self.parent[x as usize] != x {
                    let g = self.parent[self.parent[x as usize] as usize];
                    self.parent[x as usize] = g;
                    x = g;
                }
                x
            }
            fn unite(&mut self, a: u64, b: u64) {
                let (a, b) = (self.id(a), self.id(b));
                let (ra, rb) = (self.find(a), self.find(b));
                if ra != rb {
                    self.parent[ra.max(rb) as usize] = ra.min(rb);
                }
            }
        }
        let mut uf = Uf { ids: HashMap::new(), keys: Vec::new(), parent: Vec::new() };
        for s in &p1 {
            for &(a, b) in &s.hard {
                let (a, b) = (canon(a), canon(b));
                uf.unite(a, b);
            }
        }
        for p in 0..p_cap {
            let st = sh.pstate[p].load(Relaxed);
            if st & COMPLEX == 0 || st & 0xFFFF == 0 {
                continue;
            }
            for a in 0..16u32 {
                if st >> a & 1 != 0 {
                    let r = sh.label(p, a);
                    uf.unite((p as u64) << 9 | a as u64, (p as u64) << 9 | r as u64);
                }
            }
        }
        let mut evr: Vec<(u32, u32, u32)> = Vec::with_capacity(xev.len());
        for e in &xev {
            let k = canon(e.key);
            evr.push((uf.id(k), e.ts, e.pay));
        }
        let mut clr: Vec<(u32, u32, u64, u16)> = Vec::with_capacity(xcl.len());
        for c in &xcl {
            let k = canon(c.key);
            clr.push((uf.id(k), c.cts, c.ck, c.camp));
        }
        if profile {
            eprintln!("general: xev {} xcl {} nodes {}", xev.len(), xcl.len(), uf.parent.len());
        }
        let n = uf.parent.len();
        let root: Vec<u32> = (0..n as u32).map(|i| uf.find(i)).collect();
        let mut names: Vec<Vec<u8>> = vec![Vec::new(); n];
        let mut tmp = Vec::new();
        for i in 0..n {
            let k = uf.keys[i];
            tmp.clear();
            if k >> 62 & 1 != 0 {
                tmp.extend_from_slice(str_list[(k & ((1 << 62) - 1)) as usize]);
            } else {
                sh.uname((k >> 9) as usize, (k & 511) as u32, &mut tmp);
            }
            let r = root[i] as usize;
            if names[r].is_empty() || tmp < names[r] {
                names[r] = tmp.clone();
            }
        }
        for e in evr.iter_mut() {
            e.0 = root[e.0 as usize];
        }
        for c in clr.iter_mut() {
            c.0 = root[c.0 as usize];
        }
        evr.sort_unstable();
        let cid = |ck: u64| -> Vec<u8> { if ck & REF != 0 { side(ck & !REF).to_vec() } else { ckey_str(ck) } };
        let ccmp = |a: u64, b: u64| -> std::cmp::Ordering { if (a | b) & REF == 0 { a.cmp(&b) } else { cid(a).cmp(&cid(b)) } };
        clr.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(ccmp(a.2, b.2)));
        let mut ci = 0;
        let mut i = 0;
        while i < evr.len() {
            let r = evr[i].0;
            let mut j = i;
            agg.ev2.clear();
            while j < evr.len() && evr[j].0 == r {
                agg.ev2.push(Ev { ts: evr[j].1, pay: evr[j].2, alias: 0 });
                j += 1;
            }
            while ci < clr.len() && clr[ci].0 < r {
                ci += 1;
            }
            agg.cl2.clear();
            let mut rank = 0u64;
            let mut last: Option<(u32, u64)> = None;
            while ci < clr.len() && clr[ci].0 == r {
                let c = clr[ci];
                if last.is_none_or(|(t, k)| t != c.1 || ccmp(k, c.2) != std::cmp::Ordering::Equal) {
                    rank += 1;
                    agg.cl2.push(Cl { cts: c.1, camp: c.3, alias: 0, key: rank });
                    last = Some((c.1, c.2));
                }
                ci += 1;
            }
            let name = std::mem::take(&mut names[r as usize]);
            agg.person(ctx, &name);
            i = j;
        }
    }
    lap("general");

    // ------------------------------------------------ write
    let user_dir = outdir.join("user_daily");
    let camp_dir = outdir.join("campaign_daily");
    if std::fs::create_dir_all(&user_dir).is_err() || std::fs::create_dir_all(&camp_dir).is_err() {
        return false;
    }
    let mut aggs: Vec<Agg> = p3.into_iter().map(|s| s.agg).collect();
    aggs.push(agg);
    let nc = ctx.camp_ids.len() + 1;
    let nd = ctx.ndays;
    let mut camp = vec![[0i64; 4]; nc * nd];
    for a in &aggs {
        for (t, s) in camp.iter_mut().zip(&a.camp) {
            for q in 0..4 {
                t[q] += s[q];
            }
        }
    }
    thread::scope(|scope| {
        for (i, a) in aggs.into_iter().enumerate() {
            let user_dir = &user_dir;
            scope.spawn(move || {
                if a.days.is_empty() {
                    return;
                }
                let rows = a.days.len();
                let delta_names = std::env::var_os("FAST_DICTNAMES").is_none();
                // Names: a person's rows are consecutive, so the dictionary is
                // the sequence of distinct consecutive names.
                let (mut nb, mut ne, mut ni) = (Vec::new(), Vec::new(), Vec::with_capacity(rows));
                let mut prev: &[u8] = &[];
                let mut st = 0usize;
                for (r, &e) in a.name_ends.iter().enumerate() {
                    let nm = &a.names[st..e as usize];
                    if r == 0 || nm != prev {
                        nb.extend_from_slice(nm);
                        ne.push(nb.len() as u32);
                    }
                    ni.push(ne.len() as u32 - 1);
                    prev = nm;
                    st = e as usize;
                }
                let names_col = if delta_names {
                    out::DCol::DeltaStr(a.names, a.name_ends)
                } else {
                    out::DCol::Dict { dict: out::Col::Str(nb, ne), idx: ni, defs: None }
                };
                let small = |v: Vec<i32>| -> out::DCol {
                    let (lo, hi) = (v.iter().copied().min().unwrap_or(0), v.iter().copied().max().unwrap_or(0));
                    if (hi as i64 - lo as i64) >= 1 << 20 {
                        return out::DCol::Plain(out::Col::I32(v));
                    }
                    let mut map = vec![u32::MAX; (hi - lo + 1) as usize];
                    let (mut dict, mut idx) = (Vec::new(), Vec::with_capacity(v.len()));
                    for &x in &v {
                        let m = &mut map[(x - lo) as usize];
                        if *m == u32::MAX {
                            *m = dict.len() as u32;
                            dict.push(x);
                        }
                        idx.push(*m);
                    }
                    out::DCol::Dict { dict: out::Col::I32(dict), idx, defs: None }
                };
                let rev = if a.revenue.iter().all(|&x| x <= i32::MAX as i64) {
                    out::Col::I32(a.revenue.iter().map(|&x| x as i32).collect())
                } else {
                    out::Col::I64(a.revenue)
                };
                let (mut cb, mut ce) = (Vec::new(), Vec::new());
                for c in &ctx.cat_names {
                    cb.extend_from_slice(c);
                    ce.push(cb.len() as u32);
                }
                out::write_dict(
                    &user_dir.join(format!("part-{i:03}.parquet")),
                    out::USER_SCHEMA,
                    vec![
                        names_col,
                        small(a.days),
                        small(a.sessions),
                        small(a.events),
                        out::DCol::Delta(rev),
                        out::DCol::Dict { dict: out::Col::Str(cb, ce), idx: a.top.iter().map(|&c| c as u32).collect(), defs: Some(a.top_defs) },
                    ],
                    rows,
                )
                .expect("write user_daily");
            });
        }
        let (mut ids, mut id_ends, mut chans, mut chan_ends) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let (mut days, mut s, mut ps, mut rev, mut asst) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for c in 0..nc {
            for d in 0..nd {
                let r = camp[c * nd + d];
                if r[0] == 0 && r[3] == 0 {
                    continue;
                }
                let (id, ch): (&[u8], &[u8]) =
                    if c + 1 == nc { (b"organic", b"organic") } else { (&ctx.camp_ids[c], &ctx.channels[c]) };
                ids.extend_from_slice(id);
                id_ends.push(ids.len() as u32);
                chans.extend_from_slice(ch);
                chan_ends.push(chans.len() as u32);
                days.push((ctx.day0 + d as i64) as i32);
                s.push(r[0]);
                ps.push(r[1]);
                rev.push(r[2]);
                asst.push(r[3]);
            }
        }
        out::write(
            &camp_dir.join("part-000.parquet"),
            out::CAMPAIGN_SCHEMA,
            vec![
                out::Col::Str(ids, id_ends),
                out::Col::Str(chans, chan_ends),
                out::Col::I32(days),
                out::Col::I64(s),
                out::Col::I64(ps),
                out::Col::I64(rev),
                out::Col::I64(asst),
            ],
        )
        .expect("write campaign_daily");
    });
    lap("write");
    if profile {
        if let Ok(t) = std::fs::read_to_string("/proc/self/smaps_rollup") {
            for l in t.lines().filter(|l| l.starts_with("Rss") || l.starts_with("AnonHuge") || l.starts_with("Anonymous")) {
                eprintln!("{l}");
            }
        }
    }
    // Large buffers are left for the OS to reclaim at exit.
    std::mem::forget(p1);
    std::mem::forget(click_sorted);
    std::mem::forget(ev_region);
    std::mem::forget(cl_region);
    std::mem::forget(sh);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tsp_matches_reference() {
        let ws = crate::ts::parse(b"2028-02-20T00:00:00Z").unwrap();
        let we = ws + 14 * DAYMS;
        let tp = Tsp::new(ws, we);
        let mut st = 7u64;
        let mut next = || {
            st ^= st << 13;
            st ^= st >> 7;
            st ^= st << 17;
            st
        };
        let alphabet = b"0123456789-:TZ+ tz";
        for i in 0..3_000_000u64 {
            let s: Vec<u8> = if i % 3 == 0 {
                let t = ws - 9 * DAYMS + (next() % (25 * DAYMS as u64)) as i64;
                let mut v = Vec::new();
                let days = t.div_euclid(DAYMS);
                let secs = t.rem_euclid(DAYMS) / 1000;
                let (mut y, mut d) = (1970i64, days);
                while d >= crate::ts::days_from_civil(y + 1, 1, 1) - crate::ts::days_from_civil(y, 1, 1) {
                    d -= crate::ts::days_from_civil(y + 1, 1, 1) - crate::ts::days_from_civil(y, 1, 1);
                    y += 1;
                }
                let mut m = 1;
                while d >= crate::ts::days_in_month(y, m) {
                    d -= crate::ts::days_in_month(y, m);
                    m += 1;
                }
                v.extend_from_slice(format!("{y:04}-{m:02}-{:02}T{:02}:{:02}:{:02}", d + 1, secs / 3600, secs / 60 % 60, secs % 60).as_bytes());
                match next() % 3 {
                    0 => v.push(b'Z'),
                    1 => v.extend_from_slice(format!("{}{:02}:{:02}", if next() % 2 == 0 { '+' } else { '-' }, next() % 26, next() % 62).as_bytes()),
                    _ => {}
                }
                let k = (next() % 25) as usize;
                if next() % 4 == 0 && k < v.len() {
                    v[k] = alphabet[(next() % alphabet.len() as u64) as usize];
                }
                v
            } else {
                let n = (next() % 27) as usize;
                (0..n).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect()
            };
            let r = crate::ts::parse(&s).filter(|&t| t >= ws - 8 * DAYMS && t < we + 8 * DAYMS);
            let f = tp.parse(&s).filter(|&t| t >= ws - 8 * DAYMS && t < we + 8 * DAYMS);
            assert_eq!(r, f, "{:?}", String::from_utf8_lossy(&s));
        }
    }
}
