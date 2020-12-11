use std::path::PathBuf;

use serde_derive::{
    Deserialize,
};

#[derive(Clone, Deserialize, Debug)]
pub struct BlockwheelKv {
    pub tree_block_size: usize,
    pub butcher_task_restart_sec: usize,
    pub manager_task_restart_sec: usize,
    pub search_tree_task_restart_sec: usize,
    pub search_tree_remove_tasks_limit: usize,
    pub search_tree_iter_send_buffer: usize,
}

#[derive(Clone, Deserialize, Debug)]
pub struct BlockwheelWheels {
    pub task_restart_sec: usize,
    pub fss: Vec<BlockwheelFs>,
}

#[derive(Clone, Deserialize, Debug)]
pub struct BlockwheelFs {
    pub wheel_filename: PathBuf,
    pub init_wheel_size_bytes: usize,
    pub wheel_task_restart_sec: usize,
    pub wheel_task_tasks_limit: usize,
    pub work_block_size_bytes: usize,
    pub lru_cache_size_bytes: usize,
    pub defrag_parallel_tasks_limit: usize,
}

#[derive(Clone, Deserialize, Debug)]
pub struct Sled {
    pub directory: PathBuf,
    pub cache_capacity: u64,
    pub mode: String,
    pub print_profile_on_drop: bool,
}

#[derive(Clone, Deserialize, Debug)]
pub struct Bench {
    pub db_size: usize,
    pub active_tasks: usize,
    pub actions: usize,
    pub key_size_bytes: usize,
    pub value_size_bytes: usize,
    pub timeout_insert_secs: u64,
    pub timeout_lookup_secs: u64,
    pub timeout_lookup_range_secs: u64,
    pub timeout_remove_secs: u64,
    pub timeout_flush_secs: u64,
}
