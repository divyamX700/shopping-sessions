//! One-pass CSV line splitting for the fast path, plus SWAR base-36 decoding.
//!
//! Each 64-byte block's newline, comma and quote masks are computed once. A
//! line starting at offset `o` of the current block sees a 128-byte view of
//! the masks of three consecutive blocks (funnel shifts), so any line shorter
//! than 128 bytes is split from bits alone: its length is the first newline,
//! its fields are its commas, and it is well formed when it has N - 1 commas
//! and every quote is one of a pair wrapping a whole field. Longer lines and
//! the text after the last newline go through `csv::split`.

use crate::csv::{masks, split};

#[inline(always)]
fn block(data: &[u8], at: usize) -> (u64, u64, u64) {
    if at + 64 <= data.len() {
        masks(data[at..at + 64].try_into().unwrap())
    } else if at < data.len() {
        let mut t = [0u8; 64];
        t[..data.len() - at].copy_from_slice(&data[at..]);
        masks(&t)
    } else {
        (0, 0, 0)
    }
}

/// The 128 mask bits starting at bit `o` (< 64) of the three words.
#[inline(always)]
fn view(w0: u64, w1: u64, w2: u64, o: u32) -> u128 {
    let lo = ((w1 as u128) << 64 | w0 as u128) >> o;
    if o == 0 {
        lo
    } else {
        lo | (w2 as u128) << (128 - o)
    }
}

/// Calls `f` with the N unquoted fields of every well-formed line of `data`
/// (same lines and fields as `csv::for_lines` + `csv::split`).
#[inline(always)]
pub fn lines<'a, const N: usize>(data: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    let len = data.len();
    let mut base = 0usize;
    let mut o = 0usize;
    let (mut n0, mut c0, mut q0) = block(data, 0);
    let (mut n1, mut c1, mut q1) = block(data, 64);
    let (mut n2, mut c2, mut q2) = block(data, 128);
    while base + o < len {
        let start = base + o;
        let nl = view(n0, n1, n2, o as u32);
        let l;
        if nl != 0 {
            l = nl.trailing_zeros() as usize;
            if start + l > len {
                // Text after the last newline (the tail is zero padded).
                let mut out: [&'a [u8]; N] = [&[]; N];
                if split(&data[start..], &mut out) {
                    f(&out);
                }
                return;
            }
            let keep = (1u128 << l) - 1;
            let cm = view(c0, c1, c2, o as u32) & keep;
            let qm = view(q0, q1, q2, o as u32) & keep;
            if l > 0 && cm.count_ones() as usize == N - 1 {
                let mut out: [&'a [u8]; N] = [&[]; N];
                let mut s = 0usize;
                let mut m = cm;
                let mut wrapped = 0u32;
                // SAFETY: every index read is in [start, start + l], and
                // data[start + l] is the line's newline.
                let p = unsafe { data.as_ptr().add(start) };
                for (k, slot) in out.iter_mut().enumerate() {
                    let e = if k + 1 < N {
                        let t = m.trailing_zeros() as usize;
                        m &= m - 1;
                        t
                    } else {
                        l
                    };
                    let (c0, c1) = unsafe { (*p.add(s), *p.add(e.max(s + 1) - 1)) };
                    let w = ((e >= s + 2) as usize) & ((c0 == b'"') as usize) & ((c1 == b'"') as usize);
                    wrapped += w as u32;
                    *slot = unsafe { std::slice::from_raw_parts(p.add(s + w), e - s - 2 * w) };
                    s = e + 1;
                }
                if qm.count_ones() == 2 * wrapped {
                    f(&out);
                }
            }
        } else {
            // A line of 128 bytes or more.
            l = match memchr::memchr(b'\n', &data[start..]) {
                Some(i) => i,
                None => len - start,
            };
            let mut out: [&'a [u8]; N] = [&[]; N];
            if split(&data[start..start + l], &mut out) {
                f(&out);
            }
        }
        let next = start + l + 1;
        let nb = next & !63;
        if nb == base + 64 {
            (n0, c0, q0) = (n1, c1, q1);
            (n1, c1, q1) = (n2, c2, q2);
            (n2, c2, q2) = block(data, nb + 128);
        } else if nb != base {
            (n0, c0, q0) = block(data, nb);
            (n1, c1, q1) = block(data, nb + 64);
            (n2, c2, q2) = block(data, nb + 128);
        }
        base = nb;
        o = next - nb;
    }
}

const L01: u64 = 0x0101_0101_0101_0101;
const L80: u64 = 0x8080_8080_8080_8080;

/// Canonical lowercase base 36 of 1..=8 characters, at most 2^40 - 1.
/// `s` must have 8 readable bytes from its start (`pad8` makes sure).
#[inline(always)]
pub fn b36(s: &[u8], w: u64) -> Option<u64> {
    let n = s.len();
    if n == 0 || n > 8 || (n > 1 && s[0] == b'0') {
        return None;
    }
    let used = if n == 8 { u64::MAX } else { (1u64 << (8 * n)) - 1 };
    let w = w & used;
    let ge0 = w.wrapping_add(0x50 * L01) & L80;
    let ge9 = w.wrapping_add(0x46 * L01) & L80;
    let gea = w.wrapping_add(0x1F * L01) & L80;
    let gez = w.wrapping_add(0x05 * L01) & L80;
    let dig = ge0 & !ge9;
    let let_ = gea & !gez;
    if (w & L80) != 0 || ((dig | let_) & used & L80) != (used & L80) {
        return None;
    }
    let v = w.wrapping_sub(0x30 * L01 & used).wrapping_sub((let_ >> 7) * 39);
    let v = if n == 8 { v } else { v << (8 * (8 - n)) };
    let t1 = (v & 0x00FF_00FF_00FF_00FF).wrapping_mul(36) + ((v >> 8) & 0x00FF_00FF_00FF_00FF);
    let t2 = (t1 & 0x0000_FFFF_0000_FFFF).wrapping_mul(1296) + ((t1 >> 16) & 0x0000_FFFF_0000_FFFF);
    let r = (t2 & 0xFFFF_FFFF).wrapping_mul(1_679_616) + (t2 >> 32);
    (r < 1 << 40).then_some(r)
}

/// The 8 bytes at the start of `s` (zero padded past the end of `all`).
#[inline(always)]
pub fn load8(s: &[u8], all_end: *const u8) -> u64 {
    let p = s.as_ptr();
    unsafe {
        if p.add(8) <= all_end {
            (p as *const u64).read_unaligned()
        } else {
            let mut t = [0u8; 8];
            let k = (all_end as usize - p as usize).min(8);
            std::ptr::copy_nonoverlapping(p, t.as_mut_ptr(), k);
            u64::from_le_bytes(t)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b36_ref(s: &[u8]) -> Option<u64> {
        let n = s.len();
        if n == 0 || n > 8 || (n > 1 && s[0] == b'0') {
            return None;
        }
        let mut v = 0u64;
        for &c in s {
            let d = match c {
                b'0'..=b'9' => c - b'0',
                b'a'..=b'z' => c - b'a' + 10,
                _ => return None,
            };
            v = v * 36 + d as u64;
        }
        (v < 1 << 40).then_some(v)
    }

    #[test]
    fn b36_matches() {
        let alphabet = b"0123456789abcdefghijklmnopqrstuvwxyzAZ/:`{\x7f\x80\xff ";
        let mut st = 99u64;
        let mut next = || {
            st ^= st << 13;
            st ^= st >> 7;
            st ^= st << 17;
            st
        };
        for _ in 0..2_000_000 {
            let n = (next() % 10) as usize;
            let mut buf = [0u8; 16];
            for b in buf.iter_mut() {
                *b = alphabet[(next() % alphabet.len() as u64) as usize];
            }
            let s = &buf[..n];
            let end = unsafe { buf.as_ptr().add(16) };
            assert_eq!(b36(s, load8(s, end)), b36_ref(s), "{:?}", String::from_utf8_lossy(s));
        }
        for v in [0u64, 1, 35, 36, (1 << 40) - 1, 1 << 40, 36u64.pow(8) - 1] {
            let mut t = Vec::new();
            let mut x = v;
            loop {
                t.push(b"0123456789abcdefghijklmnopqrstuvwxyz"[(x % 36) as usize]);
                x /= 36;
                if x == 0 {
                    break;
                }
            }
            t.reverse();
            t.extend_from_slice(&[0; 8]);
            let s = &t[..t.len() - 8];
            assert_eq!(b36(s, load8(s, t.as_ptr_range().end)), b36_ref(s));
        }
    }

    fn check<const N: usize>(data: &[u8]) {
        let mut a: Vec<Vec<Vec<u8>>> = Vec::new();
        crate::csv::for_lines(data, |line| {
            let mut o = [&b""[..]; N];
            if split::<N>(line, &mut o) {
                a.push(o.iter().map(|x| x.to_vec()).collect());
            }
        });
        let mut b: Vec<Vec<Vec<u8>>> = Vec::new();
        lines::<N>(data, |o| b.push(o.iter().map(|x| x.to_vec()).collect()));
        assert_eq!(a, b, "N={N} {:?}", String::from_utf8_lossy(data));
    }

    #[test]
    fn lines_match_split() {
        let alphabet = b"ab,,,\"\"\"\n\n\r 1x";
        let mut st = 0x1234_5678_9abc_def0u64;
        let mut next = || {
            st ^= st << 13;
            st ^= st >> 7;
            st ^= st << 17;
            st
        };
        for round in 0..60000 {
            let n = if round % 100 == 0 { 3000 } else { (next() % 500) as usize };
            let data: Vec<u8> = (0..n).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            check::<1>(&data);
            check::<2>(&data);
            check::<4>(&data);
            check::<7>(&data);
        }
        let mut big = Vec::new();
        for i in 0..100000u32 {
            let q = |s: &str, k: u32| if (i + k) % 5 == 0 { format!("\"{s}\"") } else { s.to_string() };
            let pad = "y".repeat((i % 150) as usize);
            big.extend_from_slice(format!("{},{},{},x{pad},{},a,b\n", q("e1", 1), q("u22", 2), q("2026", 3), q("", 4)).as_bytes());
            if i % 977 == 0 {
                big.extend_from_slice(b"bad\"line,1,2,3,4,5,6\n\n");
            }
        }
        check::<7>(&big);
        big.truncate(big.len() - 1);
        check::<7>(&big);
    }
}
