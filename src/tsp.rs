//! Timestamps with a day table for the years around the window, and a lean
//! `ingest_ts` parser (rules.md: always valid F1).

const DAYMS: i64 = 86_400_000;

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
    pub(crate) fn ingest_rel(&self, s: &[u8], base_s: i64) -> Option<i64> {
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
