#[derive(Debug, Fail)]
pub enum HashEngineError {
    //    #[fail(display = "RwLock Poison")]
    //    RwLockPoisonError,
    #[fail(display = "Key: `{:?}`, not found", k)]
    KeyNotFound { k: Vec<u8> },
    #[fail(display = "File: `{}`, not found", path)]
    FileNotFound { path: String },
    #[fail(display = "Merge locked")]
    MergeLocked,
    //    #[fail(display = "Nothing to scan")]
    //    EmptyScanMap,
}
