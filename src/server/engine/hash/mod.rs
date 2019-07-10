mod error;
mod data_file;
mod key_dirs;
mod scanner;
mod options;

pub use error::HashEngineError;
pub use data_file::{DataFiles, DHFile};
pub use key_dirs::{Entry, KeyDirs};
pub use options::{Options, MergePolicy};
pub use scanner::HashScanner;

// --- std ---
use std::{
    path::Path,
    sync::{Arc, RwLock},
};
// --- external ---
use chrono::Utc;
use failure::Error;
// --- custom ---
use crate::Scanner;

pub struct HashEngineBuilder {
    options: Options,
    storage_dir: String,
}

impl HashEngineBuilder {
    pub fn new() -> Self {
        Self {
            options: Default::default(),
            storage_dir: "kv-server-hash-engine-data".to_string(),
        }
    }

    #[allow(dead_code)]
    pub fn options(mut self, options: Options) -> Self {
        self.options = options;
        self
    }

    #[allow(dead_code)]
    pub fn storage_dir(mut self, path: &str) -> Self {
        self.storage_dir = path.to_string();
        self
    }

    pub fn build(self) -> Result<HashEngine, Error> { HashEngine::init(self) }
}

#[derive(Clone)]
pub struct HashEngine {
    options: Options,

    storage_dir: String,
    key_dirs: Arc<RwLock<KeyDirs>>,

    active_file: DHFile,
    old_files: DataFiles,
}

impl HashEngine {
    fn check_dir(path: &str) -> Result<(), Error> {
        // --- std ---
        use std::fs::create_dir;

        let path = Path::new(path);
        if !path.is_dir() { create_dir(path)?; }

        Ok(())
    }

    fn scan_and_sort_dh_files(dir: &str, target_extension: &str) -> Result<(Vec<(String, u64)>, u64), Error> {
        // --- std ---
        use std::fs::read_dir;

        let mut active_file_id = 0;
        let mut files = vec![];

        for entry in read_dir(dir)? {
            let path = entry?.path();
            if let Some(extension) = path.extension() {
                if extension == target_extension {
                    let file_id = path
                        .file_stem()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .parse()?;

                    files.push((path.to_string_lossy().to_string(), file_id));

                    if file_id > active_file_id { active_file_id = file_id; }
                }
            }
        }

        files.sort_by_key(|(_, file_id)| *file_id);

        Ok((files, active_file_id))
    }

    fn load_hints(dir: &str, key_dirs: &mut KeyDirs) -> Result<u64, Error> {
        let (files, active_file_id) = HashEngine::scan_and_sort_dh_files(dir, "hint")?;

        for (path, file_id) in files.into_iter() { DHFile::load_hint(&path, file_id, key_dirs)?; }

        if active_file_id == 0 { Ok(Utc::now().timestamp_nanos() as _) } else { Ok(active_file_id) }
    }

    fn init(builder: HashEngineBuilder) -> Result<HashEngine, Error> {
        HashEngine::check_dir(&builder.storage_dir)?;

        let mut key_dirs = KeyDirs::default();
        let file_id = HashEngine::load_hints(&builder.storage_dir, &mut key_dirs)?;
        let data_file = DHFile::set_active_file(&builder.storage_dir, file_id, "data")?;
        let hint_file = DHFile::set_active_file(&builder.storage_dir, file_id, "hint")?;

        Ok(HashEngine {
            options: builder.options,
            storage_dir: builder.storage_dir,
            key_dirs: Arc::new(RwLock::new(key_dirs)),
            active_file: DHFile {
                write_offset: data_file.metadata()?.len(),
                file_id,
                data_file: Arc::new(RwLock::new(data_file)),
                hint_file: Arc::new(RwLock::new(hint_file)),
            },
            old_files: DataFiles::new(),
        })
    }

    fn check_file_size(&mut self) -> Result<(), Error> {
        if self.active_file.write_offset >= self.options.file_size_limit {
            let file_id = Utc::now().timestamp_nanos() as _;
            self.active_file = DHFile {
                write_offset: 0,
                file_id,
                data_file: Arc::new(RwLock::new(DHFile::set_active_file(&self.storage_dir, file_id, "data")?)),
                hint_file: Arc::new(RwLock::new(DHFile::set_active_file(&self.storage_dir, file_id, "hint")?)),
            };
        }

        Ok(())
    }
}

impl super::Engine for HashEngine {
    fn put(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<(), Error> {
        self.check_file_size()?;
        let entry = self.active_file.write(&k, &v)?;
        self.key_dirs
            .write()
            .unwrap()
            .insert(k, entry);

        Ok(())
    }

    fn get(&mut self, k: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        // --- std ---
        use std::io::{SeekFrom, Seek};

        if let Some(entry) = self.key_dirs
            .read()
            .unwrap()
            .get(k) {
            if self.active_file.file_id == entry.file_id {
                let buffer = entry.seek_value(&mut self.active_file.data_file
                    .write()
                    .unwrap())?;
                self.active_file.data_file
                    .write()
                    .unwrap()
                    .seek(SeekFrom::End(0))?;

                Ok(Some(buffer))
            } else {
                if let Some(mut file) = self.old_files.try_get(&self.storage_dir, entry.file_id)? { Ok(Some(entry.seek_value(&mut file)?)) } else {
                    Err(HashEngineError::FileNotFound {
                        path: format!("{}/{}", self.storage_dir, entry.file_id)
                    }.into())
//                    Ok(None)
                }
            }
        } else {
//            Err(HashEngineError::KeyNotFound {
//                k: k.to_vec()
//            }.into())
            Ok(None)
        }
    }

    fn del(&mut self, k: &[u8]) -> Result<(), Error> {
        if self.key_dirs
            .read()
            .unwrap()
            .get(k)
            .is_none() {
//            Err(HashEngineError::KeyNotFound {
//                k: k.to_vec()
//            }.into())
            Ok(())
        } else {
            self.check_file_size()?;
            self.active_file.write(k, &[])?;
            self.key_dirs
                .write()
                .unwrap()
                .remove(k);

            Ok(())
        }
    }

    // TODO Optimize
    fn scan(&mut self, mut scanner: Scanner) -> Result<(Scanner, Vec<(Vec<u8>, Vec<u8>)>), Error> {
        let keys = {
            let scanner = match scanner {
                Scanner::HashScanner(ref mut scanner) => scanner,
//            _ => unreachable!(),
            };

            scanner.scan(&self.key_dirs.read().unwrap())
        };
        let mut kvs = vec![];

        for k in keys {
            if let Some(v) = self.get(&k)? { kvs.push((k, v)); }
        }

        Ok((scanner, kvs))
    }

    fn merge(&mut self) -> Result<(), Error> {
        // --- std ---
        use std::{
            collections::HashMap,
            fs::{File, create_dir, remove_dir_all, rename},
        };

        const MERGE_DIR: &'static str = ".merge";

        create_dir(MERGE_DIR)?;
        let mut file_id = Utc::now().timestamp_nanos() as _;
        let mut dh_file = DHFile {
            write_offset: 0,
            file_id,
            data_file: Arc::new(RwLock::new(DHFile::set_active_file(MERGE_DIR, file_id, "data")?)),
            hint_file: Arc::new(RwLock::new(DHFile::set_active_file(MERGE_DIR, file_id, "hint")?)),
        };

        let mut w = self.key_dirs.write().unwrap();

        {
            let mut file_map = HashMap::new();
            let (files, _) = HashEngine::scan_and_sort_dh_files(&self.storage_dir, "data")?;
            for (path, file_id) in files.iter() { file_map.insert(file_id, File::open(path)?); }

            for (k, entry) in w.iter_mut() {
                if let Some(file) = file_map.get_mut(&entry.file_id) {
                    if dh_file.write_offset >= self.options.file_size_limit {
                        file_id = Utc::now().timestamp_nanos() as _;
                        dh_file = DHFile {
                            write_offset: 0,
                            file_id,
                            data_file: Arc::new(RwLock::new(DHFile::set_active_file(MERGE_DIR, file_id, "data")?)),
                            hint_file: Arc::new(RwLock::new(DHFile::set_active_file(MERGE_DIR, file_id, "hint")?)),
                        };
                    }

                    let v = entry.seek_value(file)?;
                    let new_entry = dh_file.write(k, &v)?;
                    *entry = new_entry;
                } else {
                    return Err(HashEngineError::FileNotFound {
                        path: format!("{}/{}", self.storage_dir, entry.file_id)
                    }.into());
                }
            }
        }

        self.active_file = dh_file;
        self.old_files = DataFiles::new();

        if self.options.keep_old_files {
            rename(
                &self.storage_dir,
                Path::new(&self.storage_dir)
                    .parent()
                    .unwrap()
                    .join(&format!("backup-data-{}", Utc::now().timestamp_nanos())),
            )?;
        } else { remove_dir_all(&self.storage_dir)?; }
        rename(MERGE_DIR, &self.storage_dir)?;

        drop(w);

        Ok(())
    }
}
