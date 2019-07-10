// --- external ---
use regex::bytes::Regex;
// --- custom ---
use crate::server::engine::hash::KeyDirs;

pub struct HashScanner {
    pub range: i64,
    pub regex: Option<Regex>,
}

impl HashScanner {
    pub fn scan(&mut self, key_dirs: &KeyDirs) -> Vec<Vec<u8>> {
        let mut matched_keys = vec![];

        let mut iter = key_dirs.iter();
        while self.range != 0 {
            self.range -= 1;

            if let Some((k, _)) = iter.next() {
                if let Some(ref regex) = self.regex {
                    if regex.is_match(k) { matched_keys.push(k.to_owned()); }
                } else { matched_keys.push(k.to_owned()) }
            } else { break; }
        }

        matched_keys
    }
}
