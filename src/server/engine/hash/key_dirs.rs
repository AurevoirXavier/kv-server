// --- std ---
use std::{
    fs::File,
    io::Read,
};
// --- external ---
use hashbrown::HashMap;
use failure::Error;

#[derive(Debug)]
pub struct Entry {
    pub file_id: u64,
    pub timestamp: u64,
    pub value_size: u32,
    pub value_position: u64,
}

impl Entry {
    pub fn seek_value(&self, file: &mut File) -> Result<Vec<u8>, Error> {
        // --- std ---
        use std::io::{SeekFrom, Seek};

        file.seek(SeekFrom::Start(self.value_position))?;
        let mut buffer = vec![0; self.value_size as _];
        file.read_exact(&mut buffer)?;

        Ok(buffer)
    }
}

pub type KeyDirs = HashMap<Vec<u8>, Entry>;
