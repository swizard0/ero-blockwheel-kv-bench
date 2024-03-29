use std::path::PathBuf;

use serde_derive::{
    Deserialize,
};

#[derive(Clone, Deserialize, Debug)]
pub struct BlockwheelKv {
    pub butcher_block_size: usize,
    pub tree_block_size: usize,
    pub iter_send_buffer: usize,
    pub manager_task_restart_sec: usize,
    pub search_tree_values_inline_size_limit: usize,
    pub search_tree_bootstrap_search_trees_limit: usize,
}

#[derive(Clone, Deserialize, Debug)]
pub struct BlockwheelWheels {
    pub task_restart_sec: usize,
    pub fss: Vec<BlockwheelFs>,
}

#[derive(Clone, Deserialize, Debug)]
pub struct BlockwheelFs {
    pub interpreter: BlockwheelInterpreter,
    pub wheel_filename: PathBuf,
    pub init_wheel_size_bytes: usize,
    pub wheel_task_restart_sec: usize,
    pub work_block_size_bytes: usize,
    pub lru_cache_size_bytes: usize,
    pub defrag_parallel_tasks_limit: usize,
}

#[derive(Clone, Deserialize, Debug)]
pub enum BlockwheelInterpreter {
    #[serde(rename = "fixed_file")]
    FixedFile,
    #[serde(rename = "ram")]
    Ram,
    #[serde(rename = "dummy")]
    Dummy,
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
    pub active_tasks: usize,
    pub actions: usize,
    pub key_size_bytes: usize,
    pub value_size_bytes: usize,
    pub insert_or_remove_prob: f64,
    pub lookup_single_prob: f64,
    pub timeout_insert_secs: u64,
    pub timeout_lookup_secs: u64,
    pub timeout_lookup_range_secs: u64,
    pub timeout_remove_secs: u64,
    pub timeout_flush_secs: u64,
}

#[derive(Clone, Deserialize, Debug)]
pub struct Runtime {
    pub worker_threads: usize,
    pub max_blocking_threads: usize,
    pub thread_stack_size: usize,
    pub thread_keep_alive_ms: usize,
}

#[derive(Clone, Deserialize, Debug)]
pub struct Edeltraud {
    pub worker_threads: usize,
}
