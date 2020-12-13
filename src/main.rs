#![forbid(unsafe_code)]

use std::{
    io,
    fs,
    time::{
        Instant,
        Duration,
    },
    sync::Arc,
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
    Serialize,
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

use rand::{
    Rng,
    SeedableRng,
    rngs::SmallRng,
};

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
    SledInvalidMode { mode_provided: String, },
    TokioRuntime(io::Error),
    Sled(sled::Error),
    GenTaskJoin(tokio::task::JoinError),
    Insert(ero_blockwheel_kv::InsertError),
    InsertSled(sled::Error),
    InsertTaskJoin(tokio::task::JoinError),
    Lookup(ero_blockwheel_kv::LookupError),
    LookupSled(sled::Error),
    LookupTaskJoin(tokio::task::JoinError),
    LookupRange(ero_blockwheel_kv::LookupRangeError),
    LookupRangeSledNext(sled::Error),
    LookupRangeSledLast(sled::Error),
    Remove(ero_blockwheel_kv::RemoveError),
    RemoveSled(sled::Error),
    RemoveTaskJoin(tokio::task::JoinError),
    Flush(ero_blockwheel_kv::FlushError),
    FlushSled(sled::Error),
    InsertTimedOut { key: kv::Key, },
    InsertTimedOutNoKey,
    LookupTimedOut { key: kv::Key, },
    LookupRangeTimedOut { key_from: kv::Key, key_to: kv::Key, },
    RemoveTimedOut { key: kv::Key, },
    FlushTimedOut,
    ExpectedValueNotFound {
        key: kv::Key,
        value_cell: kv::ValueCell,
        lookup_kind: LookupKind,
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
    WheelsGoneDuringFlush,
    SledSerialize(bincode::Error),
    SledDeserialize(bincode::Error),
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
        Backend::BlockwheelKv {
            wheel_kv_pid: blockwheel_kv_pid.clone(),
            wheels_pid,
        },
        &blocks_pool,
        &version_provider,
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

    let blocks_pool = BytesPool::new();
    let version_provider = ero_blockwheel_kv::version::Provider::from_unix_epoch_seed();

    let sled_tree = sled::Config::new()
        .path(&config.sled.directory)
        .cache_capacity(config.sled.cache_capacity)
        .mode(match &*config.sled.mode {
            "fast" =>
                sled::Mode::HighThroughput,
            "small" =>
                sled::Mode::LowSpace,
            other =>
                return Err(Error::SledInvalidMode { mode_provided: other.to_string(), }),
        })
        .print_profile_on_drop(config.sled.print_profile_on_drop)
        .open()
        .map_err(Error::Sled)?;

    stress_loop(
        &mut supervisor_pid,
        Backend::Sled {
            database: Arc::new(sled_tree),
        },
        &blocks_pool,
        &version_provider,
        data,
        counter,
        &config.bench,
    ).await?;

    Ok(())
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
}

#[derive(Clone)]
enum Backend {
    BlockwheelKv {
        wheel_kv_pid: ero_blockwheel_kv::Pid,
        wheels_pid: ero_blockwheel_kv::wheels::Pid,
    },
    Sled { database: Arc<sled::Db>, },
}

async fn stress_loop(
    supervisor_pid: &mut SupervisorPid,
    mut backend: Backend,
    blocks_pool: &BytesPool,
    version_provider: &ero_blockwheel_kv::version::Provider,
    data: &mut DataIndex,
    counter: &mut Counter,
    limits: &toml_config::Bench,
)
    -> Result<(), Error>
{
    let mut rng = SmallRng::from_entropy();
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

                backend.spawn_insert_task(supervisor_pid, &done_tx, &blocks_pool, &version_provider, key_amount, value_amount, &limits);
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
                backend.spawn_remove_task(supervisor_pid, &done_tx, &blocks_pool, &version_provider, key, &limits);
                active_tasks_counter.removes += 1;
            }
        } else {
            // lookup task
            let key_index = rng.gen_range(0, data.data.len());
            let kv::KeyValuePair { key, value_cell, } = &data.data[key_index];

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
                    backend.spawn_lookup_task(supervisor_pid, &done_tx, &blocks_pool, key, value_cell, &limits);
                    active_tasks_counter.lookups += 1;
                },
                LookupKind::Range => {
                    backend.spawn_lookup_range_task(supervisor_pid, &done_tx, &blocks_pool, key, value_cell, &limits);
                    active_tasks_counter.lookups_range += 1;
                },
            }
        }
        actions_counter += 1;
    }

    assert!(done_rx.next().await.is_none());

    backend.flush(&limits).await?;

    log::info!("FINISHED bench: elapsed = {:?} | {:?}", bench_start.elapsed(), counter);

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct SledEntry<'a> {
    version: u64,
    #[serde(borrow)]
    value: SledValue<'a>,
}

#[derive(Serialize, Deserialize)]
enum SledValue<'a> {
    Value { data: &'a [u8], },
    Tombstone,
}

impl Backend {
    fn spawn_insert_task(
        &mut self,
        supervisor_pid: &mut SupervisorPid,
        done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
        blocks_pool: &BytesPool,
        version_provider: &ero_blockwheel_kv::version::Provider,
        key_amount: usize,
        value_amount: usize,
        limits: &toml_config::Bench,
    )
    {
        match self {
            Backend::BlockwheelKv { wheel_kv_pid, .. } => {
                let mut wheel_kv_pid = wheel_kv_pid.clone();
                let blocks_pool = blocks_pool.clone();
                let op_timeout = Duration::from_secs(limits.timeout_insert_secs);
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let mut key_block = blocks_pool.lend();
                    let mut value_block = blocks_pool.lend();
                    let gen_task = tokio::task::spawn_blocking(move || {
                        let mut rng = SmallRng::from_entropy();
                        key_block.reserve(key_amount);
                        for _ in 0 .. key_amount {
                            key_block.push(rng.gen());
                        }
                        value_block.reserve(value_amount);
                        for _ in 0 .. value_amount {
                            value_block.push(rng.gen());
                        }
                        (key_block.freeze(), value_block.freeze())
                    });
                    let (key_bytes, value_bytes) = gen_task.await
                        .map_err(Error::GenTaskJoin)?;
                    let key = kv::Key { key_bytes, };
                    let value = kv::Value { value_bytes, };
                    let insert_task = tokio::time::timeout(
                        op_timeout,
                        wheel_kv_pid.insert(key.clone(), value.clone()),
                    );
                    match insert_task.await {
                        Ok(Ok(ero_blockwheel_kv::Inserted { version, })) =>
                            Ok(TaskDone::Insert { key, value, version, }),
                        Ok(Err(error)) =>
                            Err(Error::Insert(error)),
                        Err(..) =>
                            Err(Error::InsertTimedOut { key, }),
                    }
                });
            },
            Backend::Sled { database, } => {
                let database = database.clone();
                let blocks_pool = blocks_pool.clone();
                let version_provider = version_provider.clone();
                let op_timeout = Duration::from_secs(limits.timeout_insert_secs);
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let insert_task = tokio::task::spawn_blocking(move || {
                        let mut key_block = blocks_pool.lend();
                        let mut value_block = blocks_pool.lend();
                        let mut rng = SmallRng::from_entropy();
                        key_block.reserve(key_amount);
                        for _ in 0 .. key_amount {
                            key_block.push(rng.gen());
                        }
                        value_block.reserve(value_amount);
                        for _ in 0 .. value_amount {
                            value_block.push(rng.gen());
                        }
                        let mut sled_value_block = blocks_pool.lend();
                        let version = version_provider.obtain();
                        bincode::serialize_into(&mut *sled_value_block, &SledEntry {
                            version,
                            value: SledValue::Value { data: &value_block, },
                        }).map_err(Error::SledSerialize)?;
                        let key = kv::Key { key_bytes: key_block.freeze(), };
                        database.insert(&**key.key_bytes, &**sled_value_block)
                            .map_err(Error::InsertSled)?;
                        let value = kv::Value { value_bytes: value_block.freeze(), };
                        Ok(TaskDone::Insert { key, value, version, })
                    });
                    match tokio::time::timeout(op_timeout, insert_task).await {
                        Ok(Ok(Ok(task_done))) =>
                            Ok(task_done),
                        Ok(Ok(Err(error))) =>
                            Err(error),
                        Ok(Err(error)) =>
                            Err(Error::InsertTaskJoin(error)),
                        Err(..) =>
                            Err(Error::InsertTimedOutNoKey),
                    }
                });
            },
        }
    }

    fn spawn_lookup_task(
        &mut self,
        supervisor_pid: &mut SupervisorPid,
        done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
        blocks_pool: &BytesPool,
        key: kv::Key,
        value_cell: kv::ValueCell,
        limits: &toml_config::Bench,
    )
    {
        match self {
            Backend::BlockwheelKv { wheel_kv_pid, .. } => {
                let mut wheel_kv_pid = wheel_kv_pid.clone();
                let op_timeout = Duration::from_secs(limits.timeout_lookup_secs);
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let lookup_task = tokio::time::timeout(
                        op_timeout,
                        wheel_kv_pid.lookup(key.clone()),
                    );
                    match lookup_task.await {
                        Ok(Ok(None)) =>
                            Err(Error::ExpectedValueNotFound { key, value_cell, lookup_kind: LookupKind::Single, }),
                        Ok(Ok(Some(found_value_cell))) =>
                            Ok(TaskDone::Lookup {
                                key,
                                found_value_cell,
                                version_snapshot: value_cell.version,
                                lookup_kind: LookupKind::Single,
                            }),
                        Ok(Err(error)) =>
                            Err(Error::Lookup(error)),
                        Err(..) =>
                            Err(Error::LookupTimedOut { key, }),
                    }
                });
            },
            Backend::Sled { database, } => {
                let database = database.clone();
                let blocks_pool = blocks_pool.clone();
                let op_timeout = Duration::from_secs(limits.timeout_lookup_secs);
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let sled_key = key.key_bytes.clone();
                    let lookup_task = tokio::task::spawn_blocking(move || {
                        database.get(&**sled_key)
                    });
                    match tokio::time::timeout(op_timeout, lookup_task).await {
                        Ok(Ok(Ok(None))) =>
                            Err(Error::ExpectedValueNotFound { key, value_cell, lookup_kind: LookupKind::Single, }),
                        Ok(Ok(Ok(Some(bin)))) => {
                            let sled_entry: SledEntry<'_> = bincode::deserialize(&bin)
                                .map_err(Error::SledDeserialize)?;
                            match sled_entry.value {
                                SledValue::Value { data, } => {
                                    let mut value_block = blocks_pool.lend();
                                    value_block.extend_from_slice(data);
                                    Ok(TaskDone::Lookup {
                                        key,
                                        found_value_cell: kv::ValueCell {
                                            version: sled_entry.version,
                                            cell: kv::Cell::Value(value_block.into()),
                                        },
                                        version_snapshot: value_cell.version,
                                        lookup_kind: LookupKind::Single,
                                    })
                                },
                                SledValue::Tombstone =>
                                    Ok(TaskDone::Lookup {
                                        key,
                                        found_value_cell: kv::ValueCell {
                                            version: sled_entry.version,
                                            cell: kv::Cell::Tombstone,
                                        },
                                        version_snapshot: value_cell.version,
                                        lookup_kind: LookupKind::Single,
                                    }),
                            }
                        },
                        Ok(Ok(Err(error))) =>
                            Err(Error::LookupSled(error)),
                        Ok(Err(error)) =>
                            Err(Error::LookupTaskJoin(error)),
                        Err(..) =>
                            Err(Error::LookupTimedOut { key, }),
                    }
                });
            },
        }
    }

    fn spawn_lookup_range_task(
        &mut self,
        supervisor_pid: &mut SupervisorPid,
        done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
        blocks_pool: &BytesPool,
        key: kv::Key,
        value_cell: kv::ValueCell,
        limits: &toml_config::Bench,
    )
    {
        match self {
            Backend::BlockwheelKv { wheel_kv_pid, .. } => {
                let mut wheel_kv_pid = wheel_kv_pid.clone();
                let op_timeout = Duration::from_secs(limits.timeout_lookup_range_secs);
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let lookup_range_task = tokio::time::timeout(
                        op_timeout,
                        wheel_kv_pid.lookup_range(key.clone() ..= key.clone()),
                    );
                    let mut lookup_range = match lookup_range_task.await {
                        Ok(result) =>
                            result.map_err(Error::LookupRange)?,
                        Err(..) =>
                            return Err(Error::LookupRangeTimedOut { key_from: key.clone(), key_to: key, }),
                    };
                    let lookup_range_next_task = tokio::time::timeout(
                        op_timeout,
                        lookup_range.key_values_rx.next(),
                    );
                    let result = match lookup_range_next_task.await {
                        Ok(None) =>
                            return Err(Error::UnexpectedLookupRangeRxFinish),
                        Ok(Some(ero_blockwheel_kv::KeyValueStreamItem::KeyValue(key_value_pair))) =>
                            TaskDone::Lookup {
                                key: key.clone(),
                                found_value_cell: key_value_pair.value_cell,
                                version_snapshot: value_cell.version,
                                lookup_kind: LookupKind::Range,
                            },
                        Ok(Some(ero_blockwheel_kv::KeyValueStreamItem::NoMore)) =>
                            return Err(Error::ExpectedValueNotFound { key, value_cell, lookup_kind: LookupKind::Range, }),
                        Err(..) =>
                            return Err(Error::LookupRangeTimedOut { key_from: key.clone(), key_to: key, }),
                    };
                    let lookup_range_next_task = tokio::time::timeout(
                        op_timeout,
                        lookup_range.key_values_rx.next(),
                    );
                    match lookup_range_next_task.await {
                        Ok(None) =>
                            return Err(Error::UnexpectedLookupRangeRxFinish),
                        Ok(Some(ero_blockwheel_kv::KeyValueStreamItem::KeyValue(key_value_pair))) =>
                            return Err(Error::UnexpectedValueForLookupRange { key, key_value_pair, }),
                        Ok(Some(ero_blockwheel_kv::KeyValueStreamItem::NoMore)) =>
                            (),
                        Err(..) =>
                            return Err(Error::LookupRangeTimedOut { key_from: key.clone(), key_to: key, }),
                    }
                    assert!(lookup_range.key_values_rx.next().await.is_none());
                    Ok(result)
                });
            },
            Backend::Sled { database, } => {
                let database = database.clone();
                let blocks_pool = blocks_pool.clone();
                let op_timeout = Duration::from_secs(limits.timeout_lookup_range_secs);
                let key_clone = key.clone();
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let sled_key = key.key_bytes.clone();
                    let lookup_range_task = tokio::task::spawn_blocking(move || {
                        let mut iter = database.range(&**sled_key ..= &**sled_key);
                        let task_done = match iter.next() {
                            None =>
                                return Err(Error::ExpectedValueNotFound { key, value_cell, lookup_kind: LookupKind::Range, }),
                            Some(Ok((_found_key_bin, found_bin))) => {
                                let sled_entry: SledEntry<'_> = bincode::deserialize(&found_bin)
                                    .map_err(Error::SledDeserialize)?;
                                match sled_entry.value {
                                    SledValue::Value { data, } => {
                                        let mut value_block = blocks_pool.lend();
                                        value_block.extend_from_slice(data);
                                        TaskDone::Lookup {
                                            key: key.clone(),
                                            found_value_cell: kv::ValueCell {
                                                version: sled_entry.version,
                                                cell: kv::Cell::Value(value_block.into()),
                                            },
                                            version_snapshot: value_cell.version,
                                            lookup_kind: LookupKind::Single,
                                        }
                                    },
                                    SledValue::Tombstone =>
                                        TaskDone::Lookup {
                                            key: key.clone(),
                                            found_value_cell: kv::ValueCell {
                                                version: sled_entry.version,
                                                cell: kv::Cell::Tombstone,
                                            },
                                            version_snapshot: value_cell.version,
                                            lookup_kind: LookupKind::Single,
                                        },
                                }
                            },
                            Some(Err(error)) =>
                                return Err(Error::LookupRangeSledNext(error)),
                        };
                        match iter.next() {
                            None =>
                                (),
                            Some(Ok((found_key_bin, found_value_bin))) => {
                                let mut key_block = blocks_pool.lend();
                                key_block.extend_from_slice(&found_key_bin);
                                let mut value_block = blocks_pool.lend();
                                value_block.extend_from_slice(&found_value_bin);
                                return Err(Error::UnexpectedValueForLookupRange {
                                    key: key,
                                    key_value_pair: kv::KeyValuePair {
                                        key: key_block.into(),
                                        value_cell: kv::ValueCell {
                                            version: 0,
                                            cell: kv::Cell::Value(value_block.into()),
                                        },
                                    },
                                });
                            },
                            Some(Err(error)) =>
                                return Err(Error::LookupRangeSledLast(error)),
                        }
                        Ok(task_done)
                    });
                    match tokio::time::timeout(op_timeout, lookup_range_task).await {
                        Ok(Ok(result)) =>
                            result,
                        Ok(Err(error)) =>
                            Err(Error::LookupTaskJoin(error)),
                        Err(..) =>
                            Err(Error::LookupTimedOut { key: key_clone, }),
                    }
                });
            },
        }
    }

    fn spawn_remove_task(
        &mut self,
        supervisor_pid: &mut SupervisorPid,
        done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
        blocks_pool: &BytesPool,
        version_provider: &ero_blockwheel_kv::version::Provider,
        key: kv::Key,
        limits: &toml_config::Bench,
    )
    {
        match self {
            Backend::BlockwheelKv { wheel_kv_pid, .. } => {
                let mut wheel_kv_pid = wheel_kv_pid.clone();
                let op_timeout = Duration::from_secs(limits.timeout_remove_secs);
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let remove_task = tokio::time::timeout(
                        op_timeout,
                        wheel_kv_pid.remove(key.clone()),
                    );
                    match remove_task.await {
                        Ok(Ok(ero_blockwheel_kv::Removed { version, })) =>
                            Ok(TaskDone::Remove { key, version, }),
                        Ok(Err(error)) =>
                            Err(Error::Remove(error)),
                        Err(..) =>
                            Err(Error::RemoveTimedOut { key, }),
                    }
                });
            },
            Backend::Sled { database, } => {
                let database = database.clone();
                let op_timeout = Duration::from_secs(limits.timeout_remove_secs);
                let blocks_pool = blocks_pool.clone();
                let version_provider = version_provider.clone();
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let sled_key = key.clone();
                    let remove_task = tokio::task::spawn_blocking(move || {
                        let mut sled_value_block = blocks_pool.lend();
                        let version = version_provider.obtain();
                        bincode::serialize_into(&mut *sled_value_block, &SledEntry {
                            version,
                            value: SledValue::Tombstone,
                        }).map_err(Error::SledSerialize)?;
                        database.insert(&**sled_key.key_bytes, &**sled_value_block)
                            .map_err(Error::RemoveSled)
                            .map(|_| TaskDone::Remove { key: sled_key, version, })
                    });
                    match tokio::time::timeout(op_timeout, remove_task).await {
                        Ok(Ok(result)) =>
                            result,
                        Ok(Err(error)) =>
                            Err(Error::RemoveTaskJoin(error)),
                        Err(..) =>
                            Err(Error::RemoveTimedOut { key, }),
                    }
                });
            },
        }
    }

    async fn flush(&mut self, limits: &toml_config::Bench) -> Result<(), Error> {
        let op_timeout = Duration::from_secs(limits.timeout_flush_secs);
        match self {
            Backend::BlockwheelKv { wheel_kv_pid, wheels_pid, } => {
                let flush_task = tokio::time::timeout(
                    op_timeout,
                    wheel_kv_pid.flush(),
                );
                let ero_blockwheel_kv::Flushed = flush_task.await
                    .map_err(|_| Error::FlushTimedOut)
                    .and_then(|result| result.map_err(Error::Flush))?;
                let flush_task = tokio::time::timeout(
                    op_timeout,
                    wheels_pid.flush(),
                );
                let ero_blockwheel_kv::wheels::Flushed = flush_task.await
                    .map_err(|_| Error::FlushTimedOut)
                    .and_then(|result| result.map_err(|ero::NoProcError| Error::WheelsGoneDuringFlush))?;
            },
            Backend::Sled { database, } => {
                let flush_task = tokio::time::timeout(
                    op_timeout,
                    database.flush_async(),
                );
                flush_task.await
                    .map_err(|_| Error::FlushTimedOut)
                    .and_then(|result| result.map_err(Error::FlushSled))?;
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
                    if data.data[offset].value_cell.version < data_cell.value_cell.version {
                        data.data[offset] = data_cell;
                    }
                } else {
                    let offset = data.data.len();
                    data.data.push(data_cell);
                    data.index.insert(key, offset);
                }
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
                } else if version_snapshot < version_current {
                    // deprecated lookup (ignoring)
                    log::warn!("deprecated lookup: awaiting version {} but there is {} already", version_snapshot, version_current);
                } else {
                    // premature lookup (ignoring, don't want to wait)
                    log::warn!(
                        "premature lookup: found version {} but current is still {} (awaiting {})",
                        version_found,
                        version_current,
                        version_snapshot,
                    );
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
                if data.data[offset].value_cell.version < data_cell.value_cell.version {
                    data.data[offset] = data_cell;
                }
                counter.removes += 1;
                active_tasks_counter.removes -= 1;
            },
        }
        Ok(())
    }
}
