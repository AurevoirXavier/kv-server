// --- std ---
use std::{
    fs::File,
    io::{self, Read, Write},
    mem::transmute,
    sync::{Arc, RwLock},
};
// --- external ---
use chrono::Utc;
use failure::Error;
use hashbrown::HashMap;
// --- custom ---
use super::{Entry, KeyDirs};

// crc : timestamp : key size : value size :  key :  value
// u32 :       u64 :      u32 :        u32 : ?(8) : ?(256)
const DATA_HEADER_SIZE: usize = 20;
// timestamp : key size : value size : value position :  key
//       u64 :      u32 :        u32 :            u64 : ?(8)
const HINT_HEADER_SIZE: usize = 24;

struct DataHeader {
    timestamp: u64,
    key_size: u32,
    value_size: u32,
}

impl DataHeader {
    fn encode(&self, k: &[u8], v: &[u8]) -> Vec<u8> {
        // --- external ---
        use crc::crc32::checksum_ieee;

        let mut buffer = vec![0; 4];

        {
            let timestamp: [u8; 8] = unsafe { transmute(self.timestamp) };
            buffer.extend_from_slice(&timestamp);
        }
        {
            let key_size: [u8; 4] = unsafe { transmute(self.key_size) };
            buffer.extend_from_slice(&key_size);
        }
        {
            let crc: [u8; 4] = unsafe { transmute(checksum_ieee(&buffer[4..])) };
            buffer.extend_from_slice(&crc);
        }
        buffer.extend_from_slice(&k);
        buffer.extend_from_slice(&v);

        buffer
    }
}

struct HintHeader {
    timestamp: u64,
    key_size: u32,
    value_size: u32,
    value_position: u64,
}

impl HintHeader {
    fn encode(&self, k: &[u8]) -> Vec<u8> {
        let mut buffer = vec![];

        {
            let timestamp: [u8; 8] = unsafe { transmute(self.timestamp) };
            buffer.extend_from_slice(&timestamp);
        }
        {
            let key_size: [u8; 4] = unsafe { transmute(self.key_size) };
            buffer.extend_from_slice(&key_size);
        }
        {
            let value_size: [u8; 4] = unsafe { transmute(self.value_size) };
            buffer.extend_from_slice(&value_size);
        }
        {
            let value_position: [u8; 8] = unsafe { transmute(self.value_position) };
            buffer.extend_from_slice(&value_position);
        }
        buffer.extend_from_slice(&k);

        buffer
    }
}

impl From<&[u8]> for HintHeader {
    fn from(bytes: &[u8]) -> Self {
        unsafe {
            Self {
                timestamp: {
                    let mut timestamp = [0; 8];
                    timestamp.copy_from_slice(&bytes[..8]);
                    transmute(timestamp)
                },
                key_size: {
                    let mut key_size = [0; 4];
                    key_size.copy_from_slice(&bytes[8..12]);
                    transmute(key_size)
                },
                value_size: {
                    let mut value_size = [0; 4];
                    value_size.copy_from_slice(&bytes[12..16]);
                    transmute(value_size)
                },
                value_position: {
                    let mut value_position = [0; 8];
                    value_position.copy_from_slice(&bytes[16..]);
                    transmute(value_position)
                },
            }
        }
    }
}

#[derive(Clone)]
pub struct DHFile {
    pub write_offset: u64,

    pub file_id: u64,
    pub data_file: Arc<RwLock<File>>,
    pub hint_file: Arc<RwLock<File>>,
}

impl DHFile {
    pub fn set_active_file(
        storage_dir: &str,
        file_id: u64,
        extension: &str,
    ) -> Result<File, Error> {
        // --- std ---
        use std::fs::OpenOptions;

        Ok(OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(format!("{}/{}.{}", storage_dir, file_id, extension))?)
    }

    pub fn load_hint(path: &str, file_id: u64, key_dirs: &mut KeyDirs) -> Result<(), Error> {
        let mut file = File::open(path)?;
        let mut bytes = [0; HINT_HEADER_SIZE];

        loop {
            match file.read_exact(&mut bytes) {
                Ok(_) => {
                    let HintHeader {
                        timestamp,
                        key_size,
                        value_size,
                        value_position,
                    } = HintHeader::from(bytes.as_ref());

                    //                    println!("{}, {}, {}, {}",
                    //                             timestamp,
                    //                             key_size,
                    //                             value_size,
                    //                             value_position);

                    let mut bytes = vec![0; key_size as _];
                    file.read_exact(&mut bytes)?;

                    if value_position == 0 {
                        key_dirs.remove(&bytes);
                    } else {
                        key_dirs.insert(
                            bytes,
                            Entry {
                                file_id,
                                timestamp,
                                value_size,
                                value_position,
                            },
                        );
                    }
                }
                Err(e) => match e.kind() {
                    io::ErrorKind::UnexpectedEof => break,
                    _ => return Err(e.into()),
                },
            }
        }

        Ok(())
    }

    pub fn write(&mut self, k: &[u8], v: &[u8]) -> Result<Entry, Error> {
        let data_header = DataHeader {
            timestamp: Utc::now().timestamp_nanos() as _,
            key_size: k.len() as _,
            value_size: v.len() as _,
        };
        {
            let buffer = data_header.encode(k, v);
            let mut w = self.data_file.write().unwrap();
            w.write(&buffer)?;
            w.sync_data()?;
        }

        let value_position =
            self.write_offset + DATA_HEADER_SIZE as u64 + data_header.key_size as u64;
        let hint_header = HintHeader {
            timestamp: data_header.timestamp,
            key_size: data_header.key_size,
            value_size: data_header.value_size,
            value_position: if v.is_empty() { 0 } else { value_position },
        };
        {
            let buffer = hint_header.encode(k);
            let mut w = self.hint_file.write().unwrap();
            w.write(&buffer)?;
            w.sync_data()?;
        }

        self.write_offset = value_position + data_header.value_size as u64;

        Ok(Entry {
            file_id: self.file_id,
            timestamp: hint_header.timestamp,
            value_size: hint_header.value_size,
            value_position,
        })
    }
}

#[derive(Clone)]
pub struct DataFiles(Arc<RwLock<HashMap<u64, File>>>);

impl DataFiles {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }

    pub fn insert(&self, file_id: u64, file: File) {
        self.0.write().unwrap().insert(file_id, file);
    }

    pub fn try_get(&self, storage_dir: &str, file_id: u64) -> Result<Option<File>, Error> {
        let r = self.0.read().unwrap();
        if let Some(file) = r.get(&file_id) {
            Ok(Some(file.try_clone()?))
        } else {
            match File::open(format!("{}/{}.data", storage_dir, file_id)) {
                Ok(file) => {
                    drop(r);

                    self.0.write().unwrap().insert(file_id, file.try_clone()?);

                    Ok(Some(file))
                }
                Err(e) => match e.kind() {
                    io::ErrorKind::NotFound => Ok(None),
                    _ => Err(e.into()),
                },
            }
        }
    }
}
