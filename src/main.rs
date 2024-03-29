#![forbid(unsafe_code)]

use std::{
    io,
    fs,
    fmt,
    time::{
        Instant,
        Duration,
    },
    sync::{
        atomic,
        Arc,
    },
    path::{
        PathBuf,
    },
    collections::{
        hash_map,
        HashMap,
    },
};

use clap::{
    Parser,
    AppSettings,
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
    seq::IteratorRandom,
    distributions::Uniform,
};

use alloc_pool::{
    bytes::{
        Bytes,
        BytesPool,
    },
};

use ero::{
    supervisor::{
        SupervisorPid,
        SupervisorGenServer,
    },
};

use blockwheel_kv_ero::{
    wheels,
};

use blockwheel_kv::{
    kv,
};

use blockwheel_fs::{
    block,
};

mod burn;
mod toml_config;

pub static TOTAL_INSERTS: atomic::AtomicUsize = atomic::AtomicUsize::new(0);
pub static TOTAL_INSERTS_MS: atomic::AtomicUsize = atomic::AtomicUsize::new(0);
pub static TOTAL_REMOVES: atomic::AtomicUsize = atomic::AtomicUsize::new(0);
pub static TOTAL_REMOVES_MS: atomic::AtomicUsize = atomic::AtomicUsize::new(0);
pub static TOTAL_LOOKUPS: atomic::AtomicUsize = atomic::AtomicUsize::new(0);
pub static TOTAL_LOOKUPS_MS: atomic::AtomicUsize = atomic::AtomicUsize::new(0);

#[derive(Clone, Parser, Debug)]
#[clap(setting = AppSettings::DeriveDisplayOrder)]
struct CliArgs {
    /// program config path
    #[clap(long, short = 'c', default_value = "configs/blockwheel_kv_bench.toml")]
    pub config_path: PathBuf,

    #[clap(subcommand)]
    backend_cmd: BackendCmd,
}

#[derive(Clone, Parser, Debug)]
#[clap(about = "kv backend to use")]
pub enum BackendCmd {
    BlockwheelKv,
    BlockwheelKvBurn,
    Sled,
}

#[derive(Clone, Deserialize, Debug)]
struct Config {
    blockwheel_kv: toml_config::BlockwheelKv,
    blockwheel_wheels: toml_config::BlockwheelWheels,
    sled: toml_config::Sled,
    bench: toml_config::Bench,
    runtime: toml_config::Runtime,
    edeltraud: toml_config::Edeltraud,
}

#[derive(Debug)]
pub enum Error {
    ConfigRead(io::Error),
    ConfigParse(toml::de::Error),
    SledInvalidMode { mode_provided: String, },
    TokioRuntime(io::Error),
    Env(EnvError),
    Edeltraud(edeltraud::SpawnError),
    InsertedValueNotFoundInDbMirror { key: DebugKey, },
    RemovedValueNotFoundInDbMirror { key: DebugKey, },
    LookupValueNotFoundInDbMirror { key: DebugKey, serial: usize, },
    ValueNotFoundInMutations {
        key: DebugKey,
        serial: usize,
        version: u64,
    },
    LookupValueDoesNotMatchDbMirror {
        key: DebugKey,
        found_value_cell: Option<kv::ValueCell<ValueCrc64>>,
        serials_count: usize,
        snapshots: Vec<(usize, kv::ValueCell<ValueCrc64>)>,
    },
    BlockwheelKvInfo(blockwheel_kv_ero::InfoError),
    BlockwheelKvInsert(blockwheel_kv_ero::InsertError),
    BlockwheelKvRemove(blockwheel_kv_ero::RemoveError),
    BlockwheelKvLookup(blockwheel_kv_ero::LookupError),
    BlockwheelKvFlush(blockwheel_kv_ero::FlushError),
    Sled(sled::Error),
    InsertSled(sled::Error),
    InsertTaskJoin(tokio::task::JoinError),
    LookupSled(sled::Error),
    LookupTaskJoin(tokio::task::JoinError),
    RemoveSled(sled::Error),
    RemoveTaskJoin(tokio::task::JoinError),
    FlushSled(sled::Error),
    InsertTimedOutNoKey,
    LookupTimedOut { key: DebugKey, },
    RemoveTimedOut { key: DebugKey, },
    FlushTimedOut,
    SledSerialize(bincode::Error),
    SledDeserialize(bincode::Error),
    Burn(burn::Error),
}

fn main() -> Result<(), Error> {
    pretty_env_logger::init_timed();
    let cli_args = CliArgs::from_args();
    log::info!("program starts as: {:?}", cli_args);

    let config_contents = fs::read_to_string(cli_args.config_path)
        .map_err(Error::ConfigRead)?;
    let config: Config = toml::from_str(&config_contents)
        .map_err(Error::ConfigParse)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.runtime.worker_threads)
        .max_blocking_threads(config.runtime.max_blocking_threads)
        .thread_stack_size(config.runtime.thread_stack_size)
        .thread_keep_alive(Duration::from_millis(config.runtime.thread_keep_alive_ms as u64))
        .build()
        .map_err(Error::TokioRuntime)?;

    let blocks_pool = BytesPool::new();
    let mut counter = Counter::default();

    match cli_args.backend_cmd {
        BackendCmd::BlockwheelKv => {
            let env = Env::new(blocks_pool, config)
                .map_err(Error::Env)?;
            let mut db_mirror = DbMirror::new(env.huge_random_block.clone());
            for fs_config in &env.config.blockwheel_wheels.fss {
                fs::remove_file(&fs_config.wheel_filename).ok();
            }
            runtime.block_on(run_blockwheel_kv(env, &mut db_mirror, &mut counter))?;
        },
        BackendCmd::Sled => {
            let mut db_mirror = DbMirror::new(
                make_huge_random_block(&blocks_pool, &config.bench),
            );
            fs::remove_dir_all(&config.sled.directory).ok();
            runtime.block_on(run_sled(blocks_pool, config, &mut db_mirror, &mut counter))?;
        },
        BackendCmd::BlockwheelKvBurn => {
            let env = Env::new(blocks_pool, config)
                .map_err(Error::Env)?;
            return burn::run(env, runtime)
                .map_err(Error::Burn);
        },
    }

    let total_inserts = TOTAL_INSERTS.load(atomic::Ordering::Relaxed);
    let total_inserts_ms = TOTAL_INSERTS_MS.load(atomic::Ordering::Relaxed);
    log::info!(
        "TOTAL_INSERTS: {total_inserts}, TOTAL_INSERTS TIME: {:?}, AVG_INSERT TIME: {:?}",
        Duration::from_micros(total_inserts_ms as u64),
        Duration::from_micros((total_inserts_ms / total_inserts) as u64),
    );

    let total_removes = TOTAL_REMOVES.load(atomic::Ordering::Relaxed);
    let total_removes_ms = TOTAL_REMOVES_MS.load(atomic::Ordering::Relaxed);
    log::info!(
        "TOTAL_REMOVES: {total_removes}, TOTAL_REMOVES TIME: {:?}, AVG_REMOVE TIME: {:?}",
        Duration::from_micros(total_removes_ms as u64),
        Duration::from_micros((total_removes_ms / total_removes) as u64),
    );

    let total_lookups = TOTAL_LOOKUPS.load(atomic::Ordering::Relaxed);
    let total_lookups_ms = TOTAL_LOOKUPS_MS.load(atomic::Ordering::Relaxed);
    log::info!(
        "TOTAL_LOOKUPS: {total_lookups}, TOTAL_LOOKUPS TIME: {:?}, AVG_LOOKUP TIME: {:?}",
        Duration::from_micros(total_lookups_ms as u64),
        Duration::from_micros((total_lookups_ms / total_lookups) as u64),
    );

    Ok(())
}

#[allow(clippy::identity_op)]
const HUGE_RANDOM_BLOCK_AMOUNT: usize = 1 * 1024 * 1024;

fn make_huge_random_block(blocks_pool: &BytesPool, limits: &toml_config::Bench) -> Bytes {
    let mut random_block = blocks_pool.lend();
    let mut rng = SmallRng::from_entropy();
    let mut huge_random_block_amount = HUGE_RANDOM_BLOCK_AMOUNT;
    if huge_random_block_amount < limits.key_size_bytes {
        huge_random_block_amount = limits.key_size_bytes;
    }
    if huge_random_block_amount < limits.value_size_bytes {
        huge_random_block_amount = limits.value_size_bytes;
    }
    random_block.reserve(huge_random_block_amount);
    for _ in 0 .. huge_random_block_amount {
        random_block.push(rng.gen());
    }
    random_block.freeze()
}

struct Env {
    blocks_pool: BytesPool,
    edeltraud: edeltraud::Edeltraud<Job>,
    version_provider: blockwheel_kv::version::Provider,
    wheels: wheels::Wheels,
    limits: Arc<toml_config::Bench>,
    huge_random_block: Bytes,
    config: Config,
}

#[derive(Debug)]
pub enum EnvError {
    ThreadPool(edeltraud::BuildError),
    WheelsBuilder(wheels::Error),
}

impl Env {
    fn new(blocks_pool: BytesPool, config: Config) -> Result<Self, EnvError> {
        let edeltraud = edeltraud::Builder::new()
            .worker_threads(config.edeltraud.worker_threads)
            .build::<_, JobUnit<_>>()
            .map_err(EnvError::ThreadPool)?;
        let version_provider = blockwheel_kv::version::Provider::from_unix_epoch_seed();

        let mut wheels = wheels::WheelsBuilder::new();
        for fs_config in &config.blockwheel_wheels.fss {
            let blockwheel_fs_params = blockwheel_fs::Params {
                interpreter: match fs_config.interpreter {
                    toml_config::BlockwheelInterpreter::FixedFile =>
                        blockwheel_fs::InterpreterParams::FixedFile(blockwheel_fs::FixedFileInterpreterParams {
                            wheel_filename: fs_config.wheel_filename.clone(),
                            init_wheel_size_bytes: fs_config.init_wheel_size_bytes,
                        }),
                    toml_config::BlockwheelInterpreter::Ram =>
                        blockwheel_fs::InterpreterParams::Ram(blockwheel_fs::RamInterpreterParams {
                            init_wheel_size_bytes: fs_config.init_wheel_size_bytes,
                        }),
                    toml_config::BlockwheelInterpreter::Dummy =>
                        blockwheel_fs::InterpreterParams::Dummy(blockwheel_fs::DummyInterpreterParams {
                            init_wheel_size_bytes: fs_config.init_wheel_size_bytes,
                        }),
                },
                work_block_size_bytes: fs_config.work_block_size_bytes,
                lru_cache_size_bytes: fs_config.lru_cache_size_bytes,
                defrag_parallel_tasks_limit: fs_config.defrag_parallel_tasks_limit,
            };

            let blockwheel_filename = match &blockwheel_fs_params.interpreter {
                blockwheel_fs::InterpreterParams::FixedFile(interpreter_params) =>
                    interpreter_params.wheel_filename.clone(),
                blockwheel_fs::InterpreterParams::Ram(..) |
                blockwheel_fs::InterpreterParams::Dummy(..) => {
                    ('a' ..= 'z')
                        .choose_multiple(&mut rand::thread_rng(), 16)
                        .into_iter()
                        .collect::<String>()
                        .into()
                },
            };

            wheels.add_wheel_ref(wheels::WheelRef {
                blockwheel_filename: wheels::WheelFilename::from_path(
                    blockwheel_filename,
                    &blocks_pool,
                ),
                blockwheel_fs_params,
            });
        }

        let wheels = wheels.build()
            .map_err(EnvError::WheelsBuilder)?;

        let limits = Arc::new(config.bench.clone());

        let huge_random_block = make_huge_random_block(&blocks_pool, &config.bench);

        Ok(Self {
            blocks_pool,
            edeltraud,
            version_provider,
            wheels,
            limits,
            huge_random_block,
            config,
        })
    }
}

async fn run_blockwheel_kv(
    env: Env,
    db_mirror: &mut DbMirror,
    counter: &mut Counter,
)
    -> Result<(), Error>
{
    let supervisor_gen_server = SupervisorGenServer::new();
    let mut supervisor_pid = supervisor_gen_server.pid();
    tokio::spawn(supervisor_gen_server.run());

    let thread_pool = env.edeltraud.handle();
    let blockwheel_kv_gen_server = blockwheel_kv_ero::GenServer::new();
    let mut blockwheel_kv_pid = blockwheel_kv_gen_server.pid();

    supervisor_pid.spawn_link_permanent(
        blockwheel_kv_gen_server.run(
            supervisor_pid.clone(),
            blockwheel_kv::Params {
                butcher_block_size: env.config.blockwheel_kv.butcher_block_size,
                tree_block_size: env.config.blockwheel_kv.tree_block_size,
                search_tree_values_inline_size_limit: env.config.blockwheel_kv.search_tree_values_inline_size_limit,
                search_tree_bootstrap_search_trees_limit: env.config.blockwheel_kv.search_tree_bootstrap_search_trees_limit,
            },
            env.blocks_pool.clone(),
            env.version_provider.clone(),
            env.wheels,
            thread_pool.clone(),
        ),
    );

    stress_loop(
        &mut supervisor_pid,
        Backend::BlockwheelKv {
            blockwheel_kv_pid: blockwheel_kv_pid.clone(),
        },
        db_mirror,
        &env.blocks_pool,
        &env.version_provider,
        counter,
        env.limits.clone(),
        &thread_pool,
    ).await?;

    let info = blockwheel_kv_pid.info().await
        .map_err(Error::BlockwheelKvInfo)?;
    log::info!("{:?}", info);

    Ok(())
}

async fn run_sled(
    blocks_pool: BytesPool,
    config: Config,
    db_mirror: &mut DbMirror,
    counter: &mut Counter,
)
    -> Result<(), Error>
{
    let supervisor_gen_server = SupervisorGenServer::new();
    let mut supervisor_pid = supervisor_gen_server.pid();
    tokio::spawn(supervisor_gen_server.run());

    let edeltraud: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
        .worker_threads(config.edeltraud.worker_threads)
        .build::<_, JobUnit<_>>()
        .map_err(EnvError::ThreadPool)
        .map_err(Error::Env)?;
    let thread_pool = edeltraud.handle();
    let version_provider = blockwheel_kv::version::Provider::from_unix_epoch_seed();

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
        db_mirror,
        &blocks_pool,
        &version_provider,
        counter,
        Arc::new(config.bench),
        &thread_pool,
    ).await?;

    Ok(())
}


#[derive(Clone, Copy, Default, Debug)]
struct Counter {
    lookups: usize,
    inserts: usize,
    removes: usize,
}

impl Counter {
    fn sum(&self) -> usize {
        self.lookups + self.inserts + self.removes
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct ValueCrc64(u64);

struct DbMirror {
    data: HashMap<DbMirrorKey, DbMirrorValue>,
    data_vec: Vec<DbMirrorEntry>,
    huge_random_block: Bytes,
}

struct DbMirrorValue {
    db_mirror_entry_index: usize,
    key_value_crc64_pair: kv::KeyValuePair<ValueCrc64>,
}

#[derive(PartialEq, Eq, Clone, Hash)]
struct DbMirrorKey {
    key: kv::Key,
    serial: usize,
}

#[derive(Clone)]
struct DbMirrorEntry {
    key: kv::Key,
    next_serial: usize,
    committed: bool,
}

impl DbMirror {
    fn new(huge_random_block: Bytes) -> Self {
        Self {
            data: HashMap::new(),
            data_vec: Vec::new(),
            huge_random_block,
        }
    }

    fn enqueue_mutation(&mut self, key: kv::Key, cell: kv::Cell<ValueCrc64>) -> usize {
        let db_mirror_entry_index = match self.data.entry(DbMirrorKey { key: key.clone(), serial: 0, }) {
            hash_map::Entry::Vacant(ve) => {
                let db_mirror_entry_index = self.data_vec.len();
                self.data_vec.push(DbMirrorEntry {
                    key: key.clone(),
                    next_serial: 1,
                    committed: false,
                });
                ve.insert(DbMirrorValue {
                    db_mirror_entry_index,
                    key_value_crc64_pair: kv::KeyValuePair {
                        key,
                        value_cell: kv::ValueCell {
                            version: 0,
                            cell,
                        },
                    },
                });
                return 0;
            },
            hash_map::Entry::Occupied(oe) => {
                oe.get().db_mirror_entry_index
            },
        };
        let db_mirror_entry = &mut self.data_vec[db_mirror_entry_index];
        let serial = db_mirror_entry.next_serial;
        db_mirror_entry.next_serial += 1;
        self.data.insert(
            DbMirrorKey { key: key.clone(), serial, },
            DbMirrorValue {
                db_mirror_entry_index,
                key_value_crc64_pair: kv::KeyValuePair {
                    key,
                    value_cell: kv::ValueCell {
                        version: 0,
                        cell,
                    },
                },
            },
        );
        serial
    }

    fn commit_mutation(&mut self, key: kv::Key, serial: usize, version: u64) -> Result<(), Error> {
        let db_mirror_value = self.data.get_mut(&DbMirrorKey { key: key.clone(), serial, })
            .ok_or_else(|| Error::ValueNotFoundInMutations {
                key: key.into(),
                serial,
                version,
            })?;
        db_mirror_value.key_value_crc64_pair.value_cell.version = version;
        let db_mirror_entry = &mut self.data_vec[db_mirror_value.db_mirror_entry_index];
        db_mirror_entry.committed = true;
        Ok(())
    }
}

#[derive(Clone)]
enum Backend {
    BlockwheelKv {
        blockwheel_kv_pid: blockwheel_kv_ero::Pid,
    },
    Sled {
        database: Arc<sled::Db>,
    },
}

#[allow(clippy::too_many_arguments)]
async fn stress_loop<J>(
    supervisor_pid: &mut SupervisorPid,
    mut backend: Backend,
    db_mirror: &mut DbMirror,
    blocks_pool: &BytesPool,
    version_provider: &blockwheel_kv::version::Provider,
    counter: &mut Counter,
    limits: Arc<toml_config::Bench>,
    thread_pool: &edeltraud::Handle<J>,
)
    -> Result<(), Error>
where J: From<edeltraud::AsyncJob<PrepareInsertJob>>,
      J: From<edeltraud::AsyncJob<CalculateCrcJob>>,
      J: Send + 'static,
{
    let mut rng = SmallRng::from_entropy();
    let p_distribution = Uniform::new(0.0, 1.0);

    let (done_tx, done_rx) = mpsc::channel(0);
    pin_mut!(done_rx);
    let mut active_tasks_counter = Counter::default();
    let mut actions_counter = 0;

    let bench_start = Instant::now();

    loop {
        if actions_counter >= limits.actions {
            while active_tasks_counter.sum() > 0 {
                log::debug!("terminating, waiting for {} tasks to finish | active = {:?}", active_tasks_counter.sum(), active_tasks_counter);
                let done_task: TaskDone = done_rx.next().await.unwrap()?;
                done_task.process(
                    &mut backend,
                    db_mirror,
                    supervisor_pid,
                    &done_tx,
                    blocks_pool,
                    version_provider,
                    counter,
                    &mut active_tasks_counter,
                    &mut actions_counter,
                    &limits,
                    thread_pool,
                )?;
            }
            std::mem::drop(done_tx);
            break;
        }
        let maybe_task_result =
            if (active_tasks_counter.sum() >= limits.active_tasks) || (db_mirror.data_vec.is_empty() && active_tasks_counter.inserts > 0) {
                Some(done_rx.next().await.unwrap())
            } else {
                select! {
                    task_result = done_rx.next() =>
                        Some(task_result.unwrap()),
                    default =>
                        None,
                }
            };
        if let Some(task_result) = maybe_task_result {
            let done_task: TaskDone = task_result?;
            done_task.process(
                &mut backend,
                db_mirror,
                supervisor_pid,
                &done_tx,
                blocks_pool,
                version_provider,
                counter,
                &mut active_tasks_counter,
                &mut actions_counter,
                &limits,
                thread_pool,
            )?;
            continue;
        }

        let action_start = Instant::now();
        // construct action and run task
        if db_mirror.data_vec.is_empty() || rng.sample(p_distribution) < limits.insert_or_remove_prob {
            // insert or remove task
            let db_size = limits.actions / 2;
            let insert_prob = 1.0 - (db_mirror.data_vec.len() as f64 / db_size as f64);
            let dice = rng.sample(p_distribution);
            if db_mirror.data_vec.is_empty() || dice < insert_prob {
                // prepare insert task
                let prepare_insert_job = PrepareInsertJob {
                    huge_random_block: db_mirror.huge_random_block.clone(),
                    limits: limits.clone(),
                    action_start,
                };
                let thread_pool = thread_pool.clone();
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    edeltraud::job_async(&thread_pool, prepare_insert_job)
                        .map_err(Error::Edeltraud)?
                        .await
                        .map_err(Error::Edeltraud)?
                });
                active_tasks_counter.inserts += 1;
            } else {
                // remove task
                let index = rng.gen_range(0 .. db_mirror.data_vec.len());
                let db_mirror_entry = db_mirror.data_vec[index].clone();
                let serial = db_mirror.enqueue_mutation(db_mirror_entry.key.clone(), kv::Cell::Tombstone);

                log::debug!(
                    "{}. performing REMOVE with {:?} key (serial: {serial}) (dice = {:.3}, prob = {:.3}) | {:?}, active = {:?}",
                    actions_counter,
                    &db_mirror_entry.key.key_bytes[..],
                    dice,
                    1.0 - insert_prob,
                    counter,
                    active_tasks_counter,
                );

                backend.spawn_remove_task(
                    supervisor_pid,
                    &done_tx,
                    blocks_pool,
                    version_provider,
                    db_mirror_entry.key,
                    serial,
                    &limits,
                    action_start,
                );
                active_tasks_counter.removes += 1;
            }
        } else {
            // lookup task
            let index = rng.gen_range(0 .. db_mirror.data_vec.len());
            let db_mirror_entry = &db_mirror.data_vec[index];

            log::debug!(
                "{}. performing LOOKUP with {:?} key ({} serials, committed = {:?}) | {:?}, active = {:?}",
                actions_counter,
                &db_mirror_entry.key.key_bytes[..],
                db_mirror_entry.next_serial,
                db_mirror_entry.committed,
                counter,
                active_tasks_counter,
            );

            backend.spawn_lookup_task(
                supervisor_pid,
                &done_tx,
                blocks_pool,
                db_mirror_entry.key.clone(),
                db_mirror_entry.committed,
                &limits,
                action_start,
            );
            active_tasks_counter.lookups += 1;
        }
        actions_counter += 1;
    }

    assert!(done_rx.next().await.is_none());

    log::info!("FINISHED main bench: elapsed = {:?} | {:?}", bench_start.elapsed(), counter);

    backend.flush(&limits).await?;

    log::info!("FINISHED db flush: elapsed = {:?} | {:?}", bench_start.elapsed(), counter);

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
    #[allow(clippy::too_many_arguments)]
    fn spawn_insert_task(
        &mut self,
        supervisor_pid: &mut SupervisorPid,
        done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
        blocks_pool: &BytesPool,
        version_provider: &blockwheel_kv::version::Provider,
        key: kv::Key,
        value_block: Bytes,
        serial: usize,
        limits: &toml_config::Bench,
        action_start: Instant,
    )
    {
        match self {
            Backend::BlockwheelKv { blockwheel_kv_pid, .. } => {
                let mut blockwheel_kv_pid = blockwheel_kv_pid.clone();
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let value = kv::Value { value_bytes: value_block, };
                    let blockwheel_kv::Inserted { version, } = blockwheel_kv_pid.insert(key.clone(), value).await
                        .map_err(Error::BlockwheelKvInsert)?;
                    Ok(TaskDone::Insert { key, serial, version, action_start, })
                });
            },
            Backend::Sled { database, } => {
                let database = database.clone();
                let blocks_pool = blocks_pool.clone();
                let version_provider = version_provider.clone();
                let op_timeout = Duration::from_secs(limits.timeout_insert_secs);
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let insert_task = tokio::task::spawn_blocking(move || {
                        let mut sled_value_block = blocks_pool.lend();
                        let version = version_provider.obtain();
                        bincode::serialize_into(&mut **sled_value_block, &SledEntry {
                            version,
                            value: SledValue::Value { data: &value_block, },
                        }).map_err(Error::SledSerialize)?;
                        database.insert(&*key.key_bytes, &***sled_value_block)
                            .map_err(Error::InsertSled)?;
                        Ok(TaskDone::Insert { key, serial, version, action_start, })
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

    #[allow(clippy::too_many_arguments)]
    fn spawn_lookup_task(
        &mut self,
        supervisor_pid: &mut SupervisorPid,
        done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
        blocks_pool: &BytesPool,
        key: kv::Key,
        committed: bool,
        limits: &toml_config::Bench,
        action_start: Instant,
    )
    {
        match self {
            Backend::BlockwheelKv { blockwheel_kv_pid, .. } => {
                let mut blockwheel_kv_pid = blockwheel_kv_pid.clone();
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let found_value_cell = blockwheel_kv_pid.lookup(key.clone()).await
                        .map_err(Error::BlockwheelKvLookup)?;
                    Ok(TaskDone::Lookup { key, found_value_cell, committed, action_start, })
                });
            },
            Backend::Sled { database, } => {
                let database = database.clone();
                let blocks_pool = blocks_pool.clone();
                let op_timeout = Duration::from_secs(limits.timeout_lookup_secs);
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let sled_key = key.key_bytes.clone();
                    let lookup_task = tokio::task::spawn_blocking(move || {
                        database.get(&*sled_key)
                    });
                    match tokio::time::timeout(op_timeout, lookup_task).await {
                        Ok(Ok(Ok(None))) =>
                            Ok(TaskDone::Lookup { key, found_value_cell: None, committed, action_start, }),
                        Ok(Ok(Ok(Some(bin)))) => {
                            let sled_entry: SledEntry<'_> = bincode::deserialize(&bin)
                                .map_err(Error::SledDeserialize)?;
                            match sled_entry.value {
                                SledValue::Value { data, } => {
                                    let mut value_block = blocks_pool.lend();
                                    value_block.extend_from_slice(data);
                                    Ok(TaskDone::Lookup {
                                        key,
                                        found_value_cell: Some(kv::ValueCell {
                                            version: sled_entry.version,
                                            cell: kv::Cell::Value(value_block.into()),
                                        }),
                                        committed,
                                        action_start,
                                    })
                                },
                                SledValue::Tombstone =>
                                    Ok(TaskDone::Lookup {
                                        key,
                                        found_value_cell: Some(kv::ValueCell {
                                            version: sled_entry.version,
                                            cell: kv::Cell::Tombstone,
                                        }),
                                        committed,
                                        action_start,
                                    }),
                            }
                        },
                        Ok(Ok(Err(error))) =>
                            Err(Error::LookupSled(error)),
                        Ok(Err(error)) =>
                            Err(Error::LookupTaskJoin(error)),
                        Err(..) =>
                            Err(Error::LookupTimedOut { key: key.into(), }),
                    }
                });
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_remove_task(
        &mut self,
        supervisor_pid: &mut SupervisorPid,
        done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
        blocks_pool: &BytesPool,
        version_provider: &blockwheel_kv::version::Provider,
        key: kv::Key,
        serial: usize,
        limits: &toml_config::Bench,
        action_start: Instant,
    )
    {
        match self {
            Backend::BlockwheelKv { blockwheel_kv_pid, .. } => {
                let mut blockwheel_kv_pid = blockwheel_kv_pid.clone();
                spawn_task(supervisor_pid, done_tx.clone(), async move {
                    let blockwheel_kv::Removed { version, } = blockwheel_kv_pid.remove(key.clone()).await
                        .map_err(Error::BlockwheelKvRemove)?;
                    Ok(TaskDone::Remove { key, serial, version, action_start, })
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
                        bincode::serialize_into(&mut **sled_value_block, &SledEntry {
                            version,
                            value: SledValue::Tombstone,
                        }).map_err(Error::SledSerialize)?;
                        database.insert(&*sled_key.key_bytes, &***sled_value_block)
                            .map_err(Error::RemoveSled)
                            .map(|_| TaskDone::Remove { key: sled_key, serial, version, action_start, })
                    });
                    match tokio::time::timeout(op_timeout, remove_task).await {
                        Ok(Ok(result)) =>
                            result,
                        Ok(Err(error)) =>
                            Err(Error::RemoveTaskJoin(error)),
                        Err(..) =>
                            Err(Error::RemoveTimedOut { key: key.into(), }),
                    }
                });
            },
        }
    }

    async fn flush(&mut self, limits: &toml_config::Bench) -> Result<(), Error> {
        let op_timeout = Duration::from_secs(limits.timeout_flush_secs);
        match self {
            Backend::BlockwheelKv { blockwheel_kv_pid, } => {
                let flush_task = tokio::time::timeout(
                    op_timeout,
                    blockwheel_kv_pid.flush_all(),
                );
                let blockwheel_kv::Flushed = flush_task.await
                    .map_err(|_| Error::FlushTimedOut)
                    .and_then(|result| result.map_err(Error::BlockwheelKvFlush))?;
                log::info!("blockwheel_kv flushed");
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

#[allow(clippy::large_enum_variant)]
enum Job {
    BlockwheelKvEro(blockwheel_kv_ero::job::Job),
    PrepareInsert(edeltraud::AsyncJob<PrepareInsertJob>),
    CalculateCrc(edeltraud::AsyncJob<CalculateCrcJob>),
}

impl From<blockwheel_kv_ero::job::Job> for Job {
    fn from(job: blockwheel_kv_ero::job::Job) -> Self {
        Self::BlockwheelKvEro(job)
    }
}

impl From<blockwheel_kv_ero::job::BlockwheelFsJob> for Job {
    fn from(job: blockwheel_kv_ero::job::BlockwheelFsJob) -> Self {
        Self::BlockwheelKvEro(job.into())
    }
}

impl From<blockwheel_kv_ero::job::BlockwheelFsSklaveJob> for Job {
    fn from(job: blockwheel_kv_ero::job::BlockwheelFsSklaveJob) -> Self {
        Self::BlockwheelKvEro(job.into())
    }
}

impl From<blockwheel_kv_ero::job::BlockwheelFsBlockPrepareWriteJob> for Job {
    fn from(job: blockwheel_kv_ero::job::BlockwheelFsBlockPrepareWriteJob) -> Self {
        Self::BlockwheelKvEro(job.into())
    }
}

impl From<blockwheel_kv_ero::job::BlockwheelFsBlockPrepareDeleteJob> for Job {
    fn from(job: blockwheel_kv_ero::job::BlockwheelFsBlockPrepareDeleteJob) -> Self {
        Self::BlockwheelKvEro(job.into())
    }
}

impl From<blockwheel_kv_ero::job::BlockwheelFsBlockProcessReadJob> for Job {
    fn from(job: blockwheel_kv_ero::job::BlockwheelFsBlockProcessReadJob) -> Self {
        Self::BlockwheelKvEro(job.into())
    }
}

impl From<blockwheel_kv_ero::job::BlockwheelKvJob> for Job {
    fn from(job: blockwheel_kv_ero::job::BlockwheelKvJob) -> Self {
        Self::BlockwheelKvEro(job.into())
    }
}

impl From<blockwheel_kv_ero::job::BlockwheelKvFlushButcherSklaveJob> for Job {
    fn from(job: blockwheel_kv_ero::job::BlockwheelKvFlushButcherSklaveJob) -> Self {
        Self::BlockwheelKvEro(job.into())
    }
}

impl From<blockwheel_kv_ero::job::BlockwheelKvLookupRangeMergeSklaveJob> for Job {
    fn from(job: blockwheel_kv_ero::job::BlockwheelKvLookupRangeMergeSklaveJob) -> Self {
        Self::BlockwheelKvEro(job.into())
    }
}

impl From<blockwheel_kv_ero::job::BlockwheelKvMergeSearchTreesSklaveJob> for Job {
    fn from(job: blockwheel_kv_ero::job::BlockwheelKvMergeSearchTreesSklaveJob) -> Self {
        Self::BlockwheelKvEro(job.into())
    }
}

impl From<blockwheel_kv_ero::job::BlockwheelKvDemolishSearchTreeSklaveJob> for Job {
    fn from(job: blockwheel_kv_ero::job::BlockwheelKvDemolishSearchTreeSklaveJob) -> Self {
        Self::BlockwheelKvEro(job.into())
    }
}

impl From<blockwheel_kv_ero::job::BlockwheelKvPerformerSklaveJob> for Job {
    fn from(job: blockwheel_kv_ero::job::BlockwheelKvPerformerSklaveJob) -> Self {
        Self::BlockwheelKvEro(job.into())
    }
}

impl From<blockwheel_kv_ero::job::FtdSklaveJob> for Job {
    fn from(job: blockwheel_kv_ero::job::FtdSklaveJob) -> Self {
        Self::BlockwheelKvEro(job.into())
    }
}

impl From<edeltraud::AsyncJob<PrepareInsertJob>> for Job {
    fn from(job: edeltraud::AsyncJob<PrepareInsertJob>) -> Self {
        Self::PrepareInsert(job)
    }
}

impl From<edeltraud::AsyncJob<CalculateCrcJob>> for Job {
    fn from(job: edeltraud::AsyncJob<CalculateCrcJob>) -> Self {
        Self::CalculateCrc(job)
    }
}

pub struct JobUnit<J>(edeltraud::JobUnit<J, Job>);

impl<J> From<edeltraud::JobUnit<J, Job>> for JobUnit<J> {
    fn from(job_unit: edeltraud::JobUnit<J, Job>) -> Self {
        Self(job_unit)
    }
}

impl<J> edeltraud::Job for JobUnit<J>
where J: From<blockwheel_kv_ero::job::BlockwheelFsSklaveJob>,
      J: From<blockwheel_kv_ero::job::BlockwheelFsBlockPrepareWriteJob>,
      J: From<blockwheel_kv_ero::job::BlockwheelFsBlockPrepareDeleteJob>,
      J: From<blockwheel_kv_ero::job::BlockwheelFsBlockProcessReadJob>,
      J: From<blockwheel_kv_ero::job::BlockwheelKvFlushButcherSklaveJob>,
      J: From<blockwheel_kv_ero::job::BlockwheelKvLookupRangeMergeSklaveJob>,
      J: From<blockwheel_kv_ero::job::BlockwheelKvMergeSearchTreesSklaveJob>,
      J: From<blockwheel_kv_ero::job::BlockwheelKvDemolishSearchTreeSklaveJob>,
      J: From<blockwheel_kv_ero::job::BlockwheelKvPerformerSklaveJob>,
      J: From<blockwheel_kv_ero::job::FtdSklaveJob>,
      J: Send + 'static,
{
    fn run(self) {
        match self.0.job {
            Job::BlockwheelKvEro(job) => {
                let job_unit = blockwheel_kv_ero::job::JobUnit::from(edeltraud::JobUnit {
                    handle: self.0.handle,
                    job,
                });
                job_unit.run();
            },
            Job::PrepareInsert(job) => {
                let job_unit = edeltraud::JobUnit {
                    handle: self.0.handle,
                    job,
                };
                job_unit.run();
            },
            Job::CalculateCrc(job) => {
                let job_unit = edeltraud::JobUnit {
                    handle: self.0.handle,
                    job,
                };
                job_unit.run();
            },
        }
    }
}

struct PrepareInsertJob {
    huge_random_block: Bytes,
    limits: Arc<toml_config::Bench>,
    action_start: Instant,
}

impl edeltraud::Computation for PrepareInsertJob {
    type Output = Result<TaskDone, Error>;

    fn run(self) -> Self::Output {
        let mut rng = SmallRng::from_entropy();
        let key_distribution = Uniform::new(1, self.limits.key_size_bytes);
        let value_distribution = Uniform::new(1, self.limits.value_size_bytes);

        let key_amount = rng.sample(key_distribution);
        let key_offset = rng.gen_range(0 .. self.huge_random_block.len() - key_amount);
        let key_slice = &self.huge_random_block[key_offset .. key_offset + key_amount];
        let key_block = self.huge_random_block.clone_subslice(key_slice);

        let value_amount = rng.sample(value_distribution);
        let value_offset = rng.gen_range(0 .. self.huge_random_block.len() - value_amount);
        let value_slice = &self.huge_random_block[value_offset .. value_offset + value_amount];
        let value_block = self.huge_random_block.clone_subslice(value_slice);

        let value_crc64 = ValueCrc64(block::crc(&value_block));

        Ok(TaskDone::PrepareInsert { key_block, value_block, value_crc64, action_start: self.action_start, })
    }
}

struct CalculateCrcJob {
    key: kv::Key,
    found_version: u64,
    found_value: kv::Value,
    committed: bool,
    action_start: Instant,
}

impl edeltraud::Computation for CalculateCrcJob {
    type Output = Result<TaskDone, Error>;

    fn run(self) -> Self::Output {
        let value_crc64 = ValueCrc64(block::crc(&self.found_value.value_bytes));
        Ok(TaskDone::LookupReady {
            key: self.key,
            found_value_cell: Some(kv::ValueCell {
                version: self.found_version,
                cell: kv::Cell::Value(value_crc64),
            }),
            committed: self.committed,
            action_start: self.action_start,
        })
    }
}


enum TaskDone {
    PrepareInsert {
        key_block: Bytes,
        value_block: Bytes,
        value_crc64: ValueCrc64,
        action_start: Instant,
    },
    Insert {
        key: kv::Key,
        serial: usize,
        version: u64,
        action_start: Instant,
    },
    Remove {
        key: kv::Key,
        serial: usize,
        version: u64,
        action_start: Instant,
    },
    Lookup {
        key: kv::Key,
        found_value_cell: Option<kv::ValueCell<kv::Value>>,
        committed: bool,
        action_start: Instant,
    },
    LookupReady {
        key: kv::Key,
        found_value_cell: Option<kv::ValueCell<ValueCrc64>>,
        committed: bool,
        action_start: Instant,
    },
}

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
    #[allow(clippy::too_many_arguments)]
    fn process<J>(
        self,
        backend: &mut Backend,
        db_mirror: &mut DbMirror,
        supervisor_pid: &mut SupervisorPid,
        done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
        blocks_pool: &BytesPool,
        version_provider: &blockwheel_kv::version::Provider,
        counter: &mut Counter,
        active_tasks_counter: &mut Counter,
        actions_counter: &mut usize,
        limits: &toml_config::Bench,
        thread_pool: &edeltraud::Handle<J>,
    )
        -> Result<(), Error>
    where J: From<edeltraud::AsyncJob<CalculateCrcJob>>,
          J: Send + 'static,
    {
        match self {

            TaskDone::PrepareInsert { key_block, value_block, value_crc64, action_start, } => {
                let key = kv::Key { key_bytes: key_block, };
                let serial = db_mirror.enqueue_mutation(key.clone(), kv::Cell::Value(value_crc64));

                log::debug!(
                    "{}. performing INSERT with {:?} key (serial: {serial}) and {} bytes value | {:?}, active = {:?}",
                    actions_counter,
                    &key.key_bytes[..],
                    value_block.len(),
                    counter,
                    active_tasks_counter,
                );

                backend.spawn_insert_task(
                    supervisor_pid,
                    done_tx,
                    blocks_pool,
                    version_provider,
                    key,
                    value_block,
                    serial,
                    limits,
                    action_start,
                );
                Ok(())
            },

            TaskDone::Insert { key, serial, version, action_start, } => {
                db_mirror.commit_mutation(key, serial, version)?;
                // log::debug!(" ;; inserted: {:?}, serial: {serial}", &key.key_bytes[..]);
                counter.inserts += 1;
                active_tasks_counter.inserts -= 1;
                TOTAL_INSERTS.fetch_add(1, atomic::Ordering::Relaxed);
                TOTAL_INSERTS_MS.fetch_add(action_start.elapsed().as_micros() as usize, atomic::Ordering::Relaxed);
                Ok(())
            },

            TaskDone::Remove { key, serial, version, action_start, } => {
                db_mirror.commit_mutation(key, serial, version)?;
                // log::debug!(" ;; removed: {:?}, serial: {serial}", &key.key_bytes[..]);
                counter.removes += 1;
                active_tasks_counter.removes -= 1;
                TOTAL_REMOVES.fetch_add(1, atomic::Ordering::Relaxed);
                TOTAL_REMOVES_MS.fetch_add(action_start.elapsed().as_micros() as usize, atomic::Ordering::Relaxed);
                Ok(())
            },

            TaskDone::Lookup { key, found_value_cell, committed, action_start, } => {
                match found_value_cell {
                    None =>
                        process_lookup_ready(
                            db_mirror,
                            counter,
                            active_tasks_counter,
                            key,
                            None,
                            committed,
                            action_start,
                        ),
                    Some(kv::ValueCell { version, cell: kv::Cell::Tombstone, }) =>
                        process_lookup_ready(
                            db_mirror,
                            counter,
                            active_tasks_counter,
                            key,
                            Some(kv::ValueCell { version, cell: kv::Cell::Tombstone, }),
                            committed,
                            action_start,
                        ),
                    Some(kv::ValueCell { version, cell: kv::Cell::Value(value), }) => {
                        // calculate crc task
                        let calculate_crc_job = CalculateCrcJob {
                            key,
                            found_version: version,
                            found_value: value,
                            committed,
                            action_start,
                        };
                        let thread_pool = thread_pool.clone();
                        spawn_task(supervisor_pid, done_tx.clone(), async move {
                            edeltraud::job_async(&thread_pool, calculate_crc_job)
                                .map_err(Error::Edeltraud)?
                                .await
                                .map_err(Error::Edeltraud)?
                        });
                        Ok(())
                    },
                }
            },

            TaskDone::LookupReady { key, found_value_cell, committed, action_start, } => {
                process_lookup_ready(
                    db_mirror,
                    counter,
                    active_tasks_counter,
                    key,
                    found_value_cell,
                    committed,
                    action_start,
                )
            },

        }
    }
}

fn process_lookup_ready(
    db_mirror: &mut DbMirror,
    counter: &mut Counter,
    active_tasks_counter: &mut Counter,
    key: kv::Key,
    maybe_found_value_cell: Option<kv::ValueCell<ValueCrc64>>,
    committed: bool,
    action_start: Instant,
)
    -> Result<(), Error>
{
    let mut current_serial = 0;
    let mut db_mirror_value = db_mirror.data.get(&DbMirrorKey { key: key.clone(), serial: current_serial, })
        .ok_or_else(|| Error::LookupValueNotFoundInDbMirror { key: (&key).into(), serial: current_serial, })?;
    let db_mirror_entry = &db_mirror.data_vec[db_mirror_value.db_mirror_entry_index];
    let serials_count = db_mirror_entry.next_serial;
    let mut found_uncommitted = false;
    let found = if let Some(found_value_cell) = &maybe_found_value_cell {
        loop {
            match (found_value_cell, &db_mirror_value.key_value_crc64_pair.value_cell) {
                (
                    kv::ValueCell { version: found_version, cell: found_cell, },
                    kv::ValueCell { version: snapshot_version, cell: snapshot_cell, },
                ) if found_version == snapshot_version && found_cell == snapshot_cell =>
                    break true,
                (
                    kv::ValueCell { cell: found_cell, .. },
                    kv::ValueCell { version: 0, cell: snapshot_cell, },
                ) if found_cell == snapshot_cell =>
                    found_uncommitted = true,
                (kv::ValueCell { .. }, kv::ValueCell { .. }) =>
                    (),
            }

            current_serial += 1;
            if current_serial >= serials_count {
                break false;
            }
            db_mirror_value = db_mirror.data.get(&DbMirrorKey { key: key.clone(), serial: current_serial, })
                .ok_or_else(|| Error::LookupValueNotFoundInDbMirror { key: (&key).into(), serial: current_serial, })?;
        }
    } else if !committed {
        log::warn!("skipping lookup not found result on uncommitted db mirror value for {:?}", &key.key_bytes[..]);
        true
    } else {
        false
    };

    if found {
        // log::debug!(" ;; looked up: {:?}", &key.key_bytes[..]);
    } else if found_uncommitted {
        log::warn!("skipping lookup uncommitted result found for {:?}", &key.key_bytes[..]);
    } else {
        let mut snapshots = Vec::with_capacity(serials_count);
        for serial in 0 .. serials_count {
            let db_mirror_value = db_mirror.data.get(&DbMirrorKey { key: key.clone(), serial, }).unwrap();
            snapshots.push((serial, db_mirror_value.key_value_crc64_pair.value_cell.clone()));
        }

        return Err(Error::LookupValueDoesNotMatchDbMirror {
            key: key.into(),
            found_value_cell: maybe_found_value_cell,
            serials_count,
            snapshots,
        });
    }

    counter.lookups += 1;
    active_tasks_counter.lookups -= 1;
    TOTAL_LOOKUPS.fetch_add(1, atomic::Ordering::Relaxed);
    TOTAL_LOOKUPS_MS.fetch_add(action_start.elapsed().as_micros() as usize, atomic::Ordering::Relaxed);
    Ok(())
}

pub struct DebugKey {
    key: kv::Key,
}

impl<'a> From<&'a kv::Key> for DebugKey {
    fn from(key: &'a kv::Key) -> DebugKey {
        DebugKey { key: key.clone(), }
    }
}

impl From<kv::Key> for DebugKey {
    fn from(key: kv::Key) -> DebugKey {
        DebugKey { key, }
    }
}

impl fmt::Debug for DebugKey {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_tuple("kv::Key")
            .field(&&self.key.key_bytes[..])
            .finish()
    }
}
