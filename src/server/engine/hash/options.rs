// TODO
#[derive(Clone)]
pub enum MergePolicy {
    Test
}

#[derive(Clone)]
pub struct Options {
    pub file_size_limit: u64,
    pub keep_old_files: bool,
    pub merge_policy: MergePolicy,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            file_size_limit: 100 * 0x100000,
            keep_old_files: true,
            merge_policy: MergePolicy::Test,
        }
    }
}
