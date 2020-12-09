use std::{
    io,
    fs,
    time::Instant,
    path::PathBuf,
    collections::HashMap,
};

use structopt::{
    clap::{
        AppSettings,
    },
    StructOpt,
};

use serde_derive::{
    Deserialize,
};

use futures::{
    channel::{
        mpsc,
    },
    select,
    pin_mut,
    Future,
    SinkExt,
    StreamExt,
};

use rand::Rng;

use alloc_pool::{
    bytes::{
        BytesPool,
    },
};

use ero::{
    supervisor::{
        SupervisorPid,
        SupervisorGenServer,
    },
};

use ero_blockwheel_kv::{
    kv,
};

mod toml_config;

#[derive(Clone, StructOpt, Debug)]
#[structopt(setting = AppSettings::DeriveDisplayOrder)]
struct CliArgs {
    /// program config path
    #[structopt(long = "config-path", short = "c", default_value = "configs/blockwheel_kv_bench.toml")]
    pub config_path: PathBuf,

    #[structopt(subcommand)]
    backend_cmd: BackendCmd,
}

#[derive(Clone, StructOpt, Debug)]
#[structopt(about = "kv backend to use")]
pub enum BackendCmd {
    BlockwheelKv,
    Sled,
}

#[derive(Clone, Deserialize, Debug)]
struct Config {
    blockwheel_kv: toml_config::BlockwheelKv,
    blockwheel_wheels: toml_config::BlockwheelWheels,
    sled: toml_config::Sled,
    bench: toml_config::Bench,
}

#[derive(Debug)]
enum Error {
    ConfigRead(io::Error),
    ConfigParse(toml::de::Error),
    TokioRuntime(io::Error),
    Sled(sled::Error),
    Insert(ero_blockwheel_kv::InsertError),
    Lookup(ero_blockwheel_kv::LookupError),
    LookupRange(ero_blockwheel_kv::LookupRangeError),
    Remove(ero_blockwheel_kv::RemoveError),
    Flush(ero_blockwheel_kv::FlushError),
    ExpectedValueNotFound {
        key: kv::Key,
        value_cell: kv::ValueCell,
    },
    UnexpectedValueFound {
        key: kv::Key,
        expected_value_cell: kv::ValueCell,
        found_value_cell: kv::ValueCell,
    },
    UnexpectedValueForLookupRange {
        key: kv::Key,
        key_value_pair: kv::KeyValuePair,
    },
    UnexpectedLookupRangeRxFinish,
}

fn main() -> Result<(), Error> {
    pretty_env_logger::init();
    let cli_args = CliArgs::from_args();
    log::info!("program starts as: {:?}", cli_args);

    let config_contents = fs::read_to_string(cli_args.config_path)
        .map_err(Error::ConfigRead)?;
    let config: Config = toml::from_str(&config_contents)
        .map_err(Error::ConfigParse)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(Error::TokioRuntime)?;

    let mut data = DataIndex {
        index: HashMap::new(),
        data: Vec::new(),
        current_version: 0,
    };
    let mut counter = Counter::default();

    match cli_args.backend_cmd {
        BackendCmd::BlockwheelKv => {
            for fs_config in &config.blockwheel_wheels.fss {
                fs::remove_file(&fs_config.wheel_filename).ok();
            }
            runtime.block_on(run_blockwheel_kv(config.clone(), &mut data, &mut counter))?;
        },
        BackendCmd::Sled => {
            fs::remove_dir_all(&config.sled.directory).ok();
            runtime.block_on(run_sled(config.clone(), &mut data, &mut counter))?;
        },
    }

    Ok(())
}

async fn run_blockwheel_kv(
    config: Config,
    data: &mut DataIndex,
    counter: &mut Counter,
)
    -> Result<(), Error>
{
    let supervisor_gen_server = SupervisorGenServer::new();
    let mut supervisor_pid = supervisor_gen_server.pid();
    tokio::spawn(supervisor_gen_server.run());

    let blocks_pool = BytesPool::new();
    let version_provider = ero_blockwheel_kv::version::Provider::from_unix_epoch_seed();

    let mut db_files = Vec::new();
    let mut wheel_refs = Vec::new();
    for fs_config in config.blockwheel_wheels.fss {
        let blockwheel_fs_params = ero_blockwheel_fs::Params {
            wheel_filename: fs_config.wheel_filename,
            init_wheel_size_bytes: fs_config.init_wheel_size_bytes,
            wheel_task_restart_sec: fs_config.wheel_task_restart_sec,
            wheel_task_tasks_limit: fs_config.wheel_task_tasks_limit,
            work_block_size_bytes: fs_config.work_block_size_bytes,
            lru_cache_size_bytes: fs_config.lru_cache_size_bytes,
            defrag_parallel_tasks_limit: fs_config.defrag_parallel_tasks_limit,
        };

        let blockwheel_fs_gen_server = ero_blockwheel_fs::GenServer::new();
        let blockwheel_fs_pid = blockwheel_fs_gen_server.pid();

        db_files.push(blockwheel_fs_params.wheel_filename.clone());
        wheel_refs.push(ero_blockwheel_kv::wheels::WheelRef {
            blockwheel_filename: blockwheel_fs_params
                .wheel_filename
                .clone()
                .into(),
            blockwheel_pid: blockwheel_fs_pid,
        });

        supervisor_pid.spawn_link_permanent(
            blockwheel_fs_gen_server.run(
                supervisor_pid.clone(),
                blocks_pool.clone(),
                blockwheel_fs_params,
            ),
        );
    }

    let wheels_gen_server = ero_blockwheel_kv::wheels::GenServer::new();
    let wheels_pid = wheels_gen_server.pid();

    supervisor_pid.spawn_link_permanent(
        wheels_gen_server.run(
            wheel_refs,
            ero_blockwheel_kv::wheels::Params {
                task_restart_sec: config.blockwheel_wheels.task_restart_sec,
            },
        ),
    );

    let blockwheel_kv_gen_server = ero_blockwheel_kv::GenServer::new();
    let blockwheel_kv_pid = blockwheel_kv_gen_server.pid();

    supervisor_pid.spawn_link_permanent(
        blockwheel_kv_gen_server.run(
            supervisor_pid.clone(),
            blocks_pool.clone(),
            version_provider.clone(),
            wheels_pid.clone(),
            ero_blockwheel_kv::Params {
                tree_block_size: config.blockwheel_kv.tree_block_size,
                butcher_task_restart_sec: config.blockwheel_kv.butcher_task_restart_sec,
                manager_task_restart_sec: config.blockwheel_kv.manager_task_restart_sec,
                search_tree_task_restart_sec: config.blockwheel_kv.search_tree_task_restart_sec,
                search_tree_remove_tasks_limit: config.blockwheel_kv.search_tree_remove_tasks_limit,
                search_tree_iter_send_buffer: config.blockwheel_kv.search_tree_iter_send_buffer,
            },
        ),
    );

    stress_loop(
        &mut supervisor_pid,
        Backend::BlockwheelKv { wheel_kv_pid: blockwheel_kv_pid.clone(), },
        &blocks_pool,
        data,
        counter,
        &config.bench,
    ).await?;

    Ok(())
}

async fn run_sled(
    config: Config,
    data: &mut DataIndex,
    counter: &mut Counter,
)
    -> Result<(), Error>
{
    let supervisor_gen_server = SupervisorGenServer::new();
    let mut supervisor_pid = supervisor_gen_server.pid();
    tokio::spawn(supervisor_gen_server.run());

    let sled_tree = sled::open(&config.sled.directory)
        .map_err(Error::Sled)?;

    unimplemented!()
}


#[derive(Clone, Copy, Default, Debug)]
struct Counter {
    lookups: usize,
    lookups_range: usize,
    inserts: usize,
    removes: usize,
}

impl Counter {
    fn sum(&self) -> usize {
        self.lookups + self.lookups_range + self.inserts + self.removes
    }

    // fn clear(&mut self) {
    //     self.lookups = 0;
    //     self.lookups_range = 0;
    //     self.inserts = 0;
    //     self.removes = 0;
    // }
}

struct DataIndex {
    index: HashMap<kv::Key, usize>,
    data: Vec<kv::KeyValuePair>,
    current_version: u64,
}

#[derive(Clone)]
enum Backend {
    BlockwheelKv { wheel_kv_pid: ero_blockwheel_kv::Pid, },
    Sled { database: sled::Db, },
}

async fn stress_loop(
    supervisor_pid: &mut SupervisorPid,
    mut backend: Backend,
    blocks_pool: &BytesPool,
    data: &mut DataIndex,
    counter: &mut Counter,
    limits: &toml_config::Bench,
)
    -> Result<(), Error>
{
    let mut rng = rand::thread_rng();
    let (done_tx, done_rx) = mpsc::channel(0);
    pin_mut!(done_rx);
    let mut active_tasks_counter = Counter::default();
    let mut actions_counter = 0;

    let bench_start = Instant::now();

    loop {
        if actions_counter >= limits.actions {
            std::mem::drop(done_tx);

            while active_tasks_counter.sum() > 0 {
                log::debug!("terminating, waiting for {} tasks to finish | active = {:?}", active_tasks_counter.sum(), active_tasks_counter);
                let done_task: TaskDone = done_rx.next().await.unwrap()?;
                done_task.process(data, counter, &mut active_tasks_counter)?;
            }
            break;
        }
        let maybe_task_result =
            if (active_tasks_counter.sum() >= limits.active_tasks) || (data.data.is_empty() && active_tasks_counter.inserts > 0) {
                Some(done_rx.next().await.unwrap())
            } else {
                select! {
                    task_result = done_rx.next() =>
                        Some(task_result.unwrap()),
                    default =>
                        None,
                }
            };
        match maybe_task_result {
            None =>
                (),
            Some(task_result) => {
                let done_task: TaskDone = task_result?;
                done_task.process(data, counter, &mut active_tasks_counter)?;
                continue;
            }
        }

        // construct action and run task
        if data.data.is_empty() || rng.gen_range(0.0, 1.0) < 0.5 {
            // insert or remove task
            let insert_prob = 1.0 - (data.data.len() as f64 / limits.db_size as f64);
            let dice = rng.gen_range(0.0, 1.0);
            if data.data.is_empty() || dice < insert_prob {
                // insert task
                let key_amount = rng.gen_range(1, limits.key_size_bytes);
                let value_amount = rng.gen_range(1, limits.value_size_bytes);

                log::debug!(
                    "{}. performing INSERT with {} bytes key and {} bytes value (dice = {:.3}, prob = {:.3}) | {:?}, active = {:?}",
                    actions_counter,
                    key_amount,
                    value_amount,
                    dice,
                    insert_prob,
                    counter,
                    active_tasks_counter,
                );

                backend.spawn_insert_task(supervisor_pid, &done_tx, &blocks_pool, key_amount, value_amount);
                active_tasks_counter.inserts += 1;
            } else {
                // remove task
                let (key, value) = loop {
                    let key_index = rng.gen_range(0, data.data.len());
                    let kv::KeyValuePair { key, value_cell, } = &data.data[key_index];
                    match &value_cell.cell {
                        kv::Cell::Value(value) =>
                            break (key, value),
                        kv::Cell::Tombstone =>
                            continue,
                    }
                };

                log::debug!(
                    "{}. performing REMOVE with {} bytes key and {} bytes value (dice = {:.3}, prob = {:.3}) | {:?}, active = {:?}",
                    actions_counter,
                    key.key_bytes.len(),
                    value.value_bytes.len(),
                    dice,
                    1.0 - insert_prob,
                    counter,
                    active_tasks_counter,
                );

                let key = key.clone();
                backend.spawn_remove_task(supervisor_pid, &done_tx, key);
                active_tasks_counter.removes += 1;
            }
        } else {
            // lookup task
            let key_index = rng.gen_range(0, data.data.len());
            let kv::KeyValuePair { key, value_cell, } = &data.data[key_index];
            let version_snapshot = data.current_version;

            let lookup_kind = if rng.gen_range(0.0, 1.0) < 0.5 {
                LookupKind::Single
            } else {
                LookupKind::Range
            };

            log::debug!(
                "{}. performing {:?} LOOKUP with {} bytes key and {} value | {:?}, active = {:?}",
                actions_counter,
                lookup_kind,
                key.key_bytes.len(),
                match &value_cell.cell {
                    kv::Cell::Value(value) =>
                        format!("{} bytes", value.value_bytes.len()),
                    kv::Cell::Tombstone =>
                        "tombstone".to_string(),
                },
                counter,
                active_tasks_counter,
            );

            let key = key.clone();
            let value_cell = value_cell.clone();

            match lookup_kind {
                LookupKind::Single => {
                    backend.spawn_lookup_task(supervisor_pid, &done_tx, key, value_cell, version_snapshot);
                    active_tasks_counter.lookups += 1;
                },
                LookupKind::Range => {
                    backend.spawn_lookup_range_task(supervisor_pid, &done_tx, key, value_cell, version_snapshot);
                    active_tasks_counter.lookups_range += 1;
                },
            }
        }
        actions_counter += 1;
    }

    assert!(done_rx.next().await.is_none());

    backend.flush().await?;

    log::info!("FINISHED bench: elapsed = {:?} | {:?}", bench_start.elapsed(), counter);

    Ok(())
}

impl Backend {
    fn spawn_insert_task(
        &mut self,
        supervisor_pid: &mut SupervisorPid,
        done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
        blocks_pool: &BytesPool,
        key_amount: usize,
        value_amount: usize,
    )
    {
        match self {
            Backend::BlockwheelKv { wheel_kv_pid, } => {
                let mut wheel_kv_pid = wheel_kv_pid.clone();
                let blocks_pool = blocks_pool.clone();
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let mut block = blocks_pool.lend();
                    block.resize(key_amount, 0);
                    rand::thread_rng().fill(&mut block[..]);
                    let key = kv::Key { key_bytes: block.freeze(), };
                    let mut block = blocks_pool.lend();
                    block.resize(value_amount, 0);
                    rand::thread_rng().fill(&mut block[..]);
                    let value = kv::Value { value_bytes: block.freeze(), };
                    match wheel_kv_pid.insert(key.clone(), value.clone()).await {
                        Ok(ero_blockwheel_kv::Inserted { version, }) =>
                            Ok(TaskDone::Insert { key, value, version, }),
                        Err(error) =>
                            Err(Error::Insert(error))
                    }
                });
            },
            Backend::Sled { database, } => {

                unimplemented!()
            },
        }
    }

    fn spawn_lookup_task(
        &mut self,
        supervisor_pid: &mut SupervisorPid,
        done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
        key: kv::Key,
        value_cell: kv::ValueCell,
        version_snapshot: u64,
    )
    {
        match self {
            Backend::BlockwheelKv { wheel_kv_pid, } => {
                let mut wheel_kv_pid = wheel_kv_pid.clone();
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    match wheel_kv_pid.lookup(key.clone()).await {
                        Ok(None) =>
                            Err(Error::ExpectedValueNotFound { key, value_cell, }),
                        Ok(Some(found_value_cell)) =>
                            Ok(TaskDone::Lookup { key, found_value_cell, version_snapshot, lookup_kind: LookupKind::Single, }),
                        Err(error) =>
                            Err(Error::Lookup(error))
                    }
                });
            },
            Backend::Sled { database, } => {

                unimplemented!()
            },
        }
    }

    fn spawn_lookup_range_task(
        &mut self,
        supervisor_pid: &mut SupervisorPid,
        done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
        key: kv::Key,
        value_cell: kv::ValueCell,
        version_snapshot: u64,
    )
    {
        match self {
            Backend::BlockwheelKv { wheel_kv_pid, } => {
                let mut wheel_kv_pid = wheel_kv_pid.clone();
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let mut lookup_range = wheel_kv_pid.lookup_range(key.clone() ..= key.clone()).await
                        .map_err(Error::LookupRange)?;
                    let result = match lookup_range.key_values_rx.next().await {
                        None =>
                            return Err(Error::UnexpectedLookupRangeRxFinish),
                        Some(ero_blockwheel_kv::KeyValueStreamItem::KeyValue(key_value_pair)) =>
                            TaskDone::Lookup {
                                key: key.clone(),
                                found_value_cell: key_value_pair.value_cell,
                                version_snapshot,
                                lookup_kind: LookupKind::Range,
                            },
                        Some(ero_blockwheel_kv::KeyValueStreamItem::NoMore) =>
                            return Err(Error::ExpectedValueNotFound { key, value_cell, }),
                    };
                    match lookup_range.key_values_rx.next().await {
                        None =>
                            return Err(Error::UnexpectedLookupRangeRxFinish),
                        Some(ero_blockwheel_kv::KeyValueStreamItem::KeyValue(key_value_pair)) =>
                            return Err(Error::UnexpectedValueForLookupRange { key, key_value_pair, }),
                        Some(ero_blockwheel_kv::KeyValueStreamItem::NoMore) =>
                            ()
                    }
                    assert!(lookup_range.key_values_rx.next().await.is_none());
                    Ok(result)
                });
            },
            Backend::Sled { database, } => {

                unimplemented!()
            },
        }
    }

    fn spawn_remove_task(
        &mut self,
        supervisor_pid: &mut SupervisorPid,
        done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
        key: kv::Key,
    )
    {
        match self {
            Backend::BlockwheelKv { wheel_kv_pid, } => {
                let mut wheel_kv_pid = wheel_kv_pid.clone();
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    match wheel_kv_pid.remove(key.clone()).await {
                        Ok(ero_blockwheel_kv::Removed { version, }) =>
                            Ok(TaskDone::Remove { key, version, }),
                        Err(error) =>
                            Err(Error::Remove(error))
                    }
                });
            },
            Backend::Sled { database, } => {

                unimplemented!()
            },
        }
    }

    async fn flush(&mut self) -> Result<(), Error> {
        match self {
            Backend::BlockwheelKv { wheel_kv_pid, } => {
                let ero_blockwheel_kv::Flushed = wheel_kv_pid.flush().await
                    .map_err(Error::Flush)?;
            },
            Backend::Sled { database, } => {

                unimplemented!()
            },
        }
        Ok(())
    }
}

enum TaskDone {
    Lookup { key: kv::Key, found_value_cell: kv::ValueCell, version_snapshot: u64, lookup_kind: LookupKind, },
    Insert { key: kv::Key, value: kv::Value, version: u64, },
    Remove { key: kv::Key, version: u64, },
}

#[derive(Debug)]
enum LookupKind { Single, Range, }

fn spawn_task<T>(
    supervisor_pid: &mut SupervisorPid,
    mut done_tx: mpsc::Sender<Result<TaskDone, Error>>,
    task: T,
)
where T: Future<Output = Result<TaskDone, Error>> + Send + 'static
{
    supervisor_pid.spawn_link_temporary(async move {
        let result = task.await;
        done_tx.send(result).await.ok();
    })
}

impl TaskDone {
    fn process(self, data: &mut DataIndex, counter: &mut Counter, active_tasks_counter: &mut Counter) -> Result<(), Error> {
        match self {
            TaskDone::Insert { key, value, version, } => {
                let data_cell = kv::KeyValuePair {
                    key: key.clone(),
                    value_cell: kv::ValueCell {
                        version,
                        cell: kv::Cell::Value(value.clone()),
                    },
                };
                if let Some(&offset) = data.index.get(&key) {
                    data.data[offset] = data_cell;
                } else {
                    let offset = data.data.len();
                    data.data.push(data_cell);
                    data.index.insert(key, offset);
                }
                data.current_version = version;
                counter.inserts += 1;
                active_tasks_counter.inserts -= 1;
            },
            TaskDone::Lookup { key, found_value_cell, version_snapshot, lookup_kind, } => {
                let &offset = data.index.get(&key).unwrap();
                let kv::KeyValuePair { value_cell: kv::ValueCell { version: version_current, cell: ref cell_current, }, .. } = data.data[offset];
                let kv::ValueCell { version: version_found, cell: ref cell_found, } = found_value_cell;
                if version_found == version_current {
                    if cell_found == cell_current {
                        // everything is up to date
                    } else {
                        // version matches, but actual values are not
                        return Err(Error::UnexpectedValueFound {
                            key,
                            expected_value_cell: data.data[offset].value_cell.clone(),
                            found_value_cell,
                        });
                    }
                } else if version_found < version_current {
                    if version_snapshot < version_current {
                        // deprecated lookup (ignoring)
                    } else {
                        // lookup started after value is actually updated, something wrong
                        return Err(Error::UnexpectedValueFound {
                            key,
                            expected_value_cell: data.data[offset].value_cell.clone(),
                            found_value_cell,
                        });
                    }
                } else {
                    unreachable!("key = {:?}, version_current = {}, version_found = {}", key, version_current, version_found);
                }
                match lookup_kind {
                    LookupKind::Single => {
                        counter.lookups += 1;
                        active_tasks_counter.lookups -= 1;
                    },
                    LookupKind::Range => {
                        counter.lookups_range += 1;
                        active_tasks_counter.lookups_range -= 1;
                    },
                }
            }
            TaskDone::Remove { key, version, } => {
                let data_cell = kv::KeyValuePair {
                    key: key.clone(),
                    value_cell: kv::ValueCell {
                        version,
                        cell: kv::Cell::Tombstone,
                    },
                };
                let &offset = data.index.get(&key).unwrap();
                data.data[offset] = data_cell;
                data.current_version = version;
                counter.removes += 1;
                active_tasks_counter.removes -= 1;
            },
        }
        Ok(())
    }
}
