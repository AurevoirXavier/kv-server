// TODO
pub mod btree;
// TODO
pub mod lsm_tree;
pub mod hash;

// --- external ---
use failure::Error;
// --- custom ---
use crate::Scanner;

pub trait Engine {
    fn put(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<(), Error>;
    fn get(&mut self, k: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    fn del(&mut self, k: &[u8]) -> Result<(), Error>;
    fn scan(&mut self, scanner: Scanner) -> Result<(Scanner, Vec<(Vec<u8>, Vec<u8>)>), Error>;
    fn merge(&mut self) -> Result<(), Error>;
}
