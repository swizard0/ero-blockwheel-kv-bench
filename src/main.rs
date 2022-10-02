#![forbid(unsafe_code)]

use std::{
    io,
    fs,
    mem,
    time::{
        Instant,
        Duration,
    },
    sync::{
        Arc,
    },
    path::{
        PathBuf,
    },
    collections::{
        HashMap,
        BinaryHeap,
    },
    cmp::{
        Ordering,
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
    kv,
    wheels,
};

use blockwheel_fs::{
    block,
};

mod toml_config;

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
    // SledInvalidMode { mode_provided: String, },
    TokioRuntime(io::Error),
    ThreadPool(edeltraud::BuildError),
    Edeltraud(edeltraud::SpawnError),
    InsertedValueNotFoundInDbMirror { key: kv::Key, },
    RemovedValueNotFoundInDbMirror { key: kv::Key, },
    ValueNotFoundInMutations { key: kv::Key, serial: usize, },
    // Sled(sled::Error),
    // GenTaskJoin(tokio::task::JoinError),
    // Insert(blockwheel_kv_ero::InsertError),
    // InsertSled(sled::Error),
    // InsertTaskJoin(tokio::task::JoinError),
    // Lookup(blockwheel_kv_ero::LookupError),
    // LookupSled(sled::Error),
    // LookupTaskJoin(tokio::task::JoinError),
    // LookupRange(blockwheel_kv_ero::LookupRangeError),
    // LookupRangeSledNext(sled::Error),
    // LookupRangeSledLast(sled::Error),
    // Remove(blockwheel_kv_ero::RemoveError),
    // RemoveSled(sled::Error),
    // RemoveTaskJoin(tokio::task::JoinError),
    // Flush(blockwheel_kv_ero::FlushError),
    // FlushSled(sled::Error),
    // InsertTimedOut { key: kv::Key, },
    // InsertTimedOutNoKey,
    // LookupTimedOut { key: kv::Key, value_cell: kv::ValueCell<kv::Value>, },
    // LookupRangeTimedOut { key_from: kv::Key, key_to: kv::Key, value_cell: kv::ValueCell<kv::Value>, },
    // LookupRangeTimedOutInit { key_from: kv::Key, key_to: kv::Key, value_cell: kv::ValueCell<kv::Value>, },
    // LookupRangeTimedOutFirst { key_from: kv::Key, key_to: kv::Key, value_cell: kv::ValueCell<kv::Value>, },
    // LookupRangeTimedOutLast { key_from: kv::Key, key_to: kv::Key, value_cell: kv::ValueCell<kv::Value>, },
    // RemoveTimedOut { key: kv::Key, },
    // FlushTimedOut,
    // ExpectedValueNotFound {
    //     key: kv::Key,
    //     lookup_kind: LookupKind,
    // },
    // UnexpectedValueFound {
    //     key: kv::Key,
    //     expected_value_cell: kv::ValueCell<kv::Value>,
    //     found_value_cell: kv::ValueCell<kv::Value>,
    // },
    // UnexpectedValueForLookupRange {
    //     key: kv::Key,
    //     key_value_pair: kv::KeyValuePair<kv::Value>,
    // },
    // UnexpectedLookupRangeRx0Finish,
    // UnexpectedLookupRangeRx1Finish,
    // SledSerialize(bincode::Error),
    // SledDeserialize(bincode::Error),
    // WheelGoneDuringInfo { blockwheel_filename: wheels::WheelFilename, },
    // WheelsFlush(wheels::FlushError),
    WheelsBuilder(wheels::Error),
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
    let mut db_mirror = DbMirror::new(&blocks_pool, &config.bench);
    let mut counter = Counter::default();

    match cli_args.backend_cmd {
        BackendCmd::BlockwheelKv => {
            for fs_config in &config.blockwheel_wheels.fss {
                fs::remove_file(&fs_config.wheel_filename).ok();
            }
            runtime.block_on(run_blockwheel_kv(blocks_pool, config.clone(), &mut db_mirror, &mut counter))?;
        },
        BackendCmd::Sled => {
            fs::remove_dir_all(&config.sled.directory).ok();
            // runtime.block_on(run_sled(blocks_pool, config.clone(), &mut data, &mut counter))?;
        },
    }

    Ok(())
}

async fn run_blockwheel_kv(
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

    let thread_pool: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
        .worker_threads(config.edeltraud.worker_threads)
        .build()
        .map_err(Error::ThreadPool)?;
    let version_provider = blockwheel_kv_ero::version::Provider::from_unix_epoch_seed();

    // let mut db_files = Vec::new();
    // let mut wheel_refs = Vec::new();
    let mut wheels = wheels::WheelsBuilder::new();
    for fs_config in config.blockwheel_wheels.fss {
        let blockwheel_fs_params = blockwheel_fs::Params {
            interpreter: match fs_config.interpreter {
                toml_config::BlockwheelInterpreter::FixedFile =>
                    blockwheel_fs::InterpreterParams::FixedFile(blockwheel_fs::FixedFileInterpreterParams {
                        wheel_filename: fs_config.wheel_filename,
                        init_wheel_size_bytes: fs_config.init_wheel_size_bytes,
                    }),
                toml_config::BlockwheelInterpreter::Ram =>
                    blockwheel_fs::InterpreterParams::Ram(blockwheel_fs::RamInterpreterParams {
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
            blockwheel_fs::InterpreterParams::Ram(..) => {
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
        .map_err(Error::WheelsBuilder)?;

    let blockwheel_kv_gen_server = blockwheel_kv_ero::GenServer::new();
    let blockwheel_kv_pid = blockwheel_kv_gen_server.pid();

    supervisor_pid.spawn_link_permanent(
        blockwheel_kv_gen_server.run(
            supervisor_pid.clone(),
            blockwheel_kv_ero::Params {
                butcher_block_size: config.blockwheel_kv.butcher_block_size,
                tree_block_size: config.blockwheel_kv.tree_block_size,
                search_tree_values_inline_size_limit: config.blockwheel_kv.search_tree_values_inline_size_limit,
                search_tree_bootstrap_search_trees_limit: config.blockwheel_kv.search_tree_bootstrap_search_trees_limit,
            },
            blocks_pool.clone(),
            version_provider.clone(),
            wheels,
            edeltraud::ThreadPoolMap::new(thread_pool.clone()),
        ),
    );

    stress_loop(
        &mut supervisor_pid,
        Backend::BlockwheelKv {
            blockwheel_kv_pid: blockwheel_kv_pid.clone(),
        },
        db_mirror,
        &blocks_pool,
        &version_provider,
        counter,
        Arc::new(config.bench),
        &thread_pool,
    ).await?;

    // for wheels::WheelRef { blockwheel_filename, mut blockwheel_pid, } in wheel_refs {
    //     let info = blockwheel_pid.info().await
    //         .map_err(|ero::NoProcError| Error::WheelGoneDuringInfo { blockwheel_filename: blockwheel_filename.clone(), })?;
    //     log::info!("{:?} | {:?}", blockwheel_filename, info);
    // }

    Ok(())
}

// async fn run_sled(
//     config: Config,
//     data: &mut DataIndex,
//     counter: &mut Counter,
// )
//     -> Result<(), Error>
// {
//     let supervisor_gen_server = SupervisorGenServer::new();
//     let mut supervisor_pid = supervisor_gen_server.pid();
//     tokio::spawn(supervisor_gen_server.run());

//     let blocks_pool = BytesPool::new();
//     let version_provider = blockwheel_kv_ero::version::Provider::from_unix_epoch_seed();

//     let sled_tree = sled::Config::new()
//         .path(&config.sled.directory)
//         .cache_capacity(config.sled.cache_capacity)
//         .mode(match &*config.sled.mode {
//             "fast" =>
//                 sled::Mode::HighThroughput,
//             "small" =>
//                 sled::Mode::LowSpace,
//             other =>
//                 return Err(Error::SledInvalidMode { mode_provided: other.to_string(), }),
//         })
//         .print_profile_on_drop(config.sled.print_profile_on_drop)
//         .open()
//         .map_err(Error::Sled)?;

//     stress_loop(
//         &mut supervisor_pid,
//         Backend::Sled {
//             database: Arc::new(sled_tree),
//         },
//         &blocks_pool,
//         &version_provider,
//         data,
//         counter,
//         &config.bench,
//     ).await?;

//     Ok(())
// }


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

    fn clear(&mut self) {
        self.lookups = 0;
        self.lookups_range = 0;
        self.inserts = 0;
        self.removes = 0;
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct ValueCrc64(u64);

struct DbMirror {
    data: HashMap<kv::Key, DbMirrorValue>,
    huge_random_block: Bytes,
}

#[allow(clippy::identity_op)]
const HUGE_RANDOM_BLOCK_AMOUNT: usize = 1 * 1024 * 1024;

impl DbMirror {
    fn new(blocks_pool: &BytesPool, limits: &toml_config::Bench) -> Self {
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

        Self {
            data: HashMap::new(),
            huge_random_block: random_block.freeze(),
        }
    }

    fn enqueue_mutation(&mut self, key: kv::Key, cell: kv::Cell<ValueCrc64>) {
        let db_mirror_value = self.data
            .entry(key.clone())
            .or_insert_with(DbMirrorValue::default);
        let serial = db_mirror_value.serial;
        db_mirror_value.serial += 1;
        db_mirror_value.mutations.push(Mutation {
            serial,
            key_value_crc64_pair: kv::KeyValuePair {
                key,
                value_cell: kv::ValueCell {
                    version: 0,
                    cell,
                },
            },
        });
    }
}

#[derive(Default)]
struct DbMirrorValue {
    serial: usize,
    snapshot: Option<Mutation>,
    mutations: Vec<Mutation>,
    mutations_swap: Vec<Mutation>,
}

impl DbMirrorValue {
    fn commit(&mut self, key: kv::Key, serial: usize, version: u64) -> Result<(), Error> {
        let mut found = false;
        for mutation in &mut self.mutations {
            if mutation.serial == serial {
                mutation.key_value_crc64_pair.value_cell.version = version;
                assert_eq!(key, mutation.key_value_crc64_pair.key);
                found = true;
            }
        }
        if !found {
            return Err(Error::ValueNotFoundInMutations { key, serial, });
        }

        loop {
            assert!(self.mutations_swap.is_empty());
            let mut committed = false;
            while let Some(mutation) = self.mutations.pop() {
                match &mut self.snapshot {
                    maybe_snapshot @ None =>
                        *maybe_snapshot = Some(mutation),
                    Some(snapshot) if mutation.serial == snapshot.serial + 1 =>
                        *snapshot = mutation,
                    Some(..) => {
                        self.mutations_swap.push(mutation);
                        continue;
                    },
                }
                committed = true;
                break;
            }
            self.mutations_swap.append(&mut self.mutations);
            mem::swap(&mut self.mutations, &mut self.mutations_swap);
            if !committed {
                break;
            }
        }

        Ok(())
    }
}

struct Mutation {
    serial: usize,
    key_value_crc64_pair: kv::KeyValuePair<ValueCrc64>,
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

async fn stress_loop<P>(
    supervisor_pid: &mut SupervisorPid,
    mut backend: Backend,
    db_mirror: &mut DbMirror,
    blocks_pool: &BytesPool,
    version_provider: &blockwheel_kv_ero::version::Provider,
    counter: &mut Counter,
    limits: Arc<toml_config::Bench>,
    thread_pool: &P,
)
    -> Result<(), Error>
where P: edeltraud::ThreadPool<Job> + Clone + Send + Sync + 'static,
{
    let mut rng = SmallRng::from_entropy();
    let p_distribution = Uniform::new(0.0, 1.0);
    let key_distribution = Uniform::new(1, limits.key_size_bytes);
    let value_distribution = Uniform::new(1, limits.value_size_bytes);

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
                    &thread_pool,
                )?;
            }
            std::mem::drop(done_tx);
            break;
        }
//         let maybe_task_result =
//             if (active_tasks_counter.sum() >= limits.active_tasks) || (data.data.is_empty() && active_tasks_counter.inserts > 0) {
//                 Some(done_rx.next().await.unwrap())
//             } else {
//                 select! {
//                     task_result = done_rx.next() =>
//                         Some(task_result.unwrap()),
//                     default =>
//                         None,
//                 }
//             };
//         match maybe_task_result {
//             None =>
//                 (),
//             Some(task_result) => {
//                 let done_task: TaskDone = task_result?;
//                 done_task.process(data, counter, &mut active_tasks_counter)?;
//                 continue;
//             }
//         }

        // construct action and run task
        if db_mirror.data.is_empty() || rng.sample(p_distribution) < limits.insert_or_remove_prob {
            // insert or remove task
            let db_size = limits.actions / 2;
            let insert_prob = 1.0 - (db_mirror.data.len() as f64 / db_size as f64);
            let dice = rng.sample(p_distribution);
            if db_mirror.data.is_empty() || dice < insert_prob {
                // prepare insert task
                let prepare_insert_job = PrepareInsertJob {
                    huge_random_block: db_mirror.huge_random_block.clone(),
                    limits: limits.clone(),
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

                todo!()
//                 let (key, value) = loop {
//                     let index = rng.gen_range(0 .. data.alive.len());
//                     let &key_index = data.alive
//                         .values()
//                         .nth(index)
//                         .unwrap();
//                     let kv::KeyValuePair { key, value_cell, } = &data.data[key_index];
//                     match &value_cell.cell {
//                         kv::Cell::Value(value) =>
//                             break (key, value),
//                         kv::Cell::Tombstone =>
//                             continue,
//                     }
//                 };

//                 log::debug!(
//                     "{}. performing REMOVE with {} bytes key and {} bytes value (dice = {:.3}, prob = {:.3}) | {:?}, active = {:?}",
//                     actions_counter,
//                     key.key_bytes.len(),
//                     value.value_bytes.len(),
//                     dice,
//                     1.0 - insert_prob,
//                     counter,
//                     active_tasks_counter,
//                 );

//                 backend.spawn_remove_task(supervisor_pid, &done_tx, &blocks_pool, &version_provider, key.clone(), &limits);
//                 active_tasks_counter.removes += 1;
            }
        } else {
            // lookup task

            todo!()
//             let key_index = rng.gen_range(0 .. data.data.len());
//             let kv::KeyValuePair { key, value_cell, } = &data.data[key_index];

//             let lookup_kind = if rng.sample(p_distribution) < limits.lookup_single_prob {
//                 LookupKind::Single
//             } else {
//                 LookupKind::Range
//             };

//             log::debug!(
//                 "{}. performing {:?} LOOKUP with {} bytes key and {} value | {:?}, active = {:?}",
//                 actions_counter,
//                 lookup_kind,
//                 key.key_bytes.len(),
//                 match &value_cell.cell {
//                     kv::Cell::Value(value) =>
//                         format!("{} bytes", value.value_bytes.len()),
//                     kv::Cell::Tombstone =>
//                         "tombstone".to_string(),
//                 },
//                 counter,
//                 active_tasks_counter,
//             );

//             let key = key.clone();
//             let value_cell = value_cell.clone();

//             match lookup_kind {
//                 LookupKind::Single => {
//                     backend.spawn_lookup_task(supervisor_pid, &done_tx, &blocks_pool, key, value_cell, &limits);
//                     active_tasks_counter.lookups += 1;
//                 },
//                 LookupKind::Range => {
//                     backend.spawn_lookup_range_task(supervisor_pid, &done_tx, &blocks_pool, key, value_cell, &limits);
//                     active_tasks_counter.lookups_range += 1;
//                 },
//             }
        }
        actions_counter += 1;
    }

    assert!(done_rx.next().await.is_none());

    log::info!("FINISHED main bench: elapsed = {:?} | {:?}", bench_start.elapsed(), counter);

     backend.flush(&limits).await?;

     log::info!("FINISHED db flush: elapsed = {:?} | {:?}", bench_start.elapsed(), counter);

     Ok(())
}

// #[derive(Serialize, Deserialize)]
// struct SledEntry<'a> {
//     version: u64,
//     #[serde(borrow)]
//     value: SledValue<'a>,
// }

// #[derive(Serialize, Deserialize)]
// enum SledValue<'a> {
//     Value { data: &'a [u8], },
//     Tombstone,
// }

impl Backend {
    fn spawn_insert_task<P>(
        &mut self,
        supervisor_pid: &mut SupervisorPid,
        done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
        blocks_pool: &BytesPool,
        version_provider: &blockwheel_kv_ero::version::Provider,
        key: kv::Key,
        value_block: Bytes,
        limits: &toml_config::Bench,
        thread_pool: &P,
    )
    where P: edeltraud::ThreadPool<Job> + Clone
    {
        match self {
            Backend::BlockwheelKv { blockwheel_kv_pid, .. } => {
                let blocks_pool = blocks_pool.clone();
                let thread_pool = thread_pool.clone();
//                 spawn_task(supervisor_pid, done_tx.clone(), async move {
//                     let mut key_block = blocks_pool.lend();
//                     let mut value_block = blocks_pool.lend();
//                     let gen_task = tokio::task::spawn_blocking(move || {
//                         let mut rng = SmallRng::from_entropy();
//                         key_block.reserve(key_amount);
//                         for _ in 0 .. key_amount {
//                             key_block.push(rng.gen());
//                         }
//                         value_block.reserve(value_amount);
//                         for _ in 0 .. value_amount {
//                             value_block.push(rng.gen());
//                         }
//                         (key_block.freeze(), value_block.freeze())
//                     });
//                     let (key_bytes, value_bytes) = gen_task.await
//                         .map_err(Error::GenTaskJoin)?;
//                     let key = kv::Key { key_bytes, };
//                     let value = kv::Value { value_bytes, };
//                     let insert_task = tokio::time::timeout(
//                         op_timeout,
//                         wheel_kv_pid.insert(key.clone(), value.clone()),
//                     );
//                     match insert_task.await {
//                         Ok(Ok(blockwheel_kv_ero::Inserted { version, })) =>
//                             Ok(TaskDone::Insert { key, value, version, }),
//                         Ok(Err(error)) =>
//                             Err(Error::Insert(error)),
//                         Err(..) =>
//                             Err(Error::InsertTimedOut { key, }),
//                     }
//                 });
            },
            Backend::Sled { database, } => {
//                 let database = database.clone();
//                 let blocks_pool = blocks_pool.clone();
//                 let version_provider = version_provider.clone();
//                 let op_timeout = Duration::from_secs(limits.timeout_insert_secs);
//                 spawn_task(supervisor_pid, done_tx.clone(), async move {
//                     let insert_task = tokio::task::spawn_blocking(move || {
//                         let mut key_block = blocks_pool.lend();
//                         let mut value_block = blocks_pool.lend();
//                         let mut rng = SmallRng::from_entropy();
//                         key_block.reserve(key_amount);
//                         for _ in 0 .. key_amount {
//                             key_block.push(rng.gen());
//                         }
//                         value_block.reserve(value_amount);
//                         for _ in 0 .. value_amount {
//                             value_block.push(rng.gen());
//                         }
//                         let mut sled_value_block = blocks_pool.lend();
//                         let version = version_provider.obtain();
//                         bincode::serialize_into(&mut **sled_value_block, &SledEntry {
//                             version,
//                             value: SledValue::Value { data: &value_block, },
//                         }).map_err(Error::SledSerialize)?;
//                         let key = kv::Key { key_bytes: key_block.freeze(), };
//                         database.insert(&*key.key_bytes, &***sled_value_block)
//                             .map_err(Error::InsertSled)?;
//                         let value = kv::Value { value_bytes: value_block.freeze(), };
//                         Ok(TaskDone::Insert { key, value, version, })
//                     });
//                     match tokio::time::timeout(op_timeout, insert_task).await {
//                         Ok(Ok(Ok(task_done))) =>
//                             Ok(task_done),
//                         Ok(Ok(Err(error))) =>
//                             Err(error),
//                         Ok(Err(error)) =>
//                             Err(Error::InsertTaskJoin(error)),
//                         Err(..) =>
//                             Err(Error::InsertTimedOutNoKey),
//                     }
//                 });
            },
        }
    }

//     fn spawn_lookup_task(
//         &mut self,
//         supervisor_pid: &mut SupervisorPid,
//         done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
//         blocks_pool: &BytesPool,
//         key: kv::Key,
//         value_cell: kv::ValueCell<kv::Value>,
//         limits: &toml_config::Bench,
//     )
//     {
//         log::debug!(" ;; lookup single for: {:?}", key);

//         match self {
//             Backend::BlockwheelKv { wheel_kv_pid, .. } => {
//                 let mut wheel_kv_pid = wheel_kv_pid.clone();
//                 let op_timeout = Duration::from_secs(limits.timeout_lookup_secs);
//                 spawn_task(supervisor_pid, done_tx.clone(), async move {
//                     let lookup_task = tokio::time::timeout(
//                         op_timeout,
//                         wheel_kv_pid.lookup(key.clone()),
//                     );
//                     match lookup_task.await {
//                         Ok(Ok(found_value_cell)) =>
//                             Ok(TaskDone::Lookup {
//                                 key,
//                                 found_value_cell,
//                                 version_snapshot: value_cell.version,
//                                 lookup_kind: LookupKind::Single,
//                             }),
//                         Ok(Err(error)) =>
//                             Err(Error::Lookup(error)),
//                         Err(..) =>
//                             Err(Error::LookupTimedOut { key, value_cell, }),
//                     }
//                 });
//             },
//             Backend::Sled { database, } => {
//                 let database = database.clone();
//                 let blocks_pool = blocks_pool.clone();
//                 let op_timeout = Duration::from_secs(limits.timeout_lookup_secs);
//                 spawn_task(supervisor_pid, done_tx.clone(), async move {
//                     let sled_key = key.key_bytes.clone();
//                     let lookup_task = tokio::task::spawn_blocking(move || {
//                         database.get(&*sled_key)
//                     });
//                     match tokio::time::timeout(op_timeout, lookup_task).await {
//                         Ok(Ok(Ok(None))) =>
//                             Err(Error::ExpectedValueNotFound { key, lookup_kind: LookupKind::Single, }),
//                         Ok(Ok(Ok(Some(bin)))) => {
//                             let sled_entry: SledEntry<'_> = bincode::deserialize(&bin)
//                                 .map_err(Error::SledDeserialize)?;
//                             match sled_entry.value {
//                                 SledValue::Value { data, } => {
//                                     let mut value_block = blocks_pool.lend();
//                                     value_block.extend_from_slice(data);
//                                     Ok(TaskDone::Lookup {
//                                         key,
//                                         found_value_cell: Some(kv::ValueCell {
//                                             version: sled_entry.version,
//                                             cell: kv::Cell::Value(value_block.into()),
//                                         }),
//                                         version_snapshot: value_cell.version,
//                                         lookup_kind: LookupKind::Single,
//                                     })
//                                 },
//                                 SledValue::Tombstone =>
//                                     Ok(TaskDone::Lookup {
//                                         key,
//                                         found_value_cell: Some(kv::ValueCell {
//                                             version: sled_entry.version,
//                                             cell: kv::Cell::Tombstone,
//                                         }),
//                                         version_snapshot: value_cell.version,
//                                         lookup_kind: LookupKind::Single,
//                                     }),
//                             }
//                         },
//                         Ok(Ok(Err(error))) =>
//                             Err(Error::LookupSled(error)),
//                         Ok(Err(error)) =>
//                             Err(Error::LookupTaskJoin(error)),
//                         Err(..) =>
//                             Err(Error::LookupTimedOut { key, value_cell, }),
//                     }
//                 });
//             },
//         }
//     }

//     fn spawn_lookup_range_task(
//         &mut self,
//         supervisor_pid: &mut SupervisorPid,
//         done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
//         blocks_pool: &BytesPool,
//         key: kv::Key,
//         value_cell: kv::ValueCell<kv::Value>,
//         limits: &toml_config::Bench,
//     )
//     {
//         log::debug!(" ;; lookup range for: {:?}", key);

//         match self {
//             Backend::BlockwheelKv { wheel_kv_pid, .. } => {
//                 let mut wheel_kv_pid = wheel_kv_pid.clone();
//                 let op_timeout = Duration::from_secs(limits.timeout_lookup_range_secs);
//                 spawn_task(supervisor_pid, done_tx.clone(), async move {
//                     let lookup_range_task = tokio::time::timeout(
//                         op_timeout,
//                         wheel_kv_pid.lookup_range(key.clone() ..= key.clone()),
//                     );
//                     let mut lookup_range = match lookup_range_task.await {
//                         Ok(result) =>
//                             result.map_err(Error::LookupRange)?,
//                         Err(..) =>
//                             return Err(Error::LookupRangeTimedOutInit { key_from: key.clone(), key_to: key, value_cell, }),
//                     };
//                     let lookup_range_next_task = tokio::time::timeout(
//                         op_timeout,
//                         lookup_range.key_values_rx.next(),
//                     );
//                     let result = match lookup_range_next_task.await {
//                         Ok(None) =>
//                             return Err(Error::UnexpectedLookupRangeRx0Finish),
//                         Ok(Some(blockwheel_kv_ero::KeyValueStreamItem::KeyValue(key_value_pair))) => {
//                             let lookup_range_next_task = tokio::time::timeout(
//                                 op_timeout,
//                                 lookup_range.key_values_rx.next(),
//                             );
//                             match lookup_range_next_task.await {
//                                 Ok(None) =>
//                                     return Err(Error::UnexpectedLookupRangeRx1Finish),
//                                 Ok(Some(blockwheel_kv_ero::KeyValueStreamItem::KeyValue(key_value_pair))) =>
//                                     return Err(Error::UnexpectedValueForLookupRange { key, key_value_pair, }),
//                                 Ok(Some(blockwheel_kv_ero::KeyValueStreamItem::NoMore)) =>
//                                     (),
//                                 Err(..) =>
//                                     return Err(Error::LookupRangeTimedOutLast { key_from: key.clone(), key_to: key, value_cell, }),
//                             }

//                             TaskDone::Lookup {
//                                 key: key.clone(),
//                                 found_value_cell: Some(key_value_pair.value_cell),
//                                 version_snapshot: value_cell.version,
//                                 lookup_kind: LookupKind::Range,
//                             }
//                         },
//                         Ok(Some(blockwheel_kv_ero::KeyValueStreamItem::NoMore)) => {
//                             TaskDone::Lookup {
//                                 key: key.clone(),
//                                 found_value_cell: None,
//                                 version_snapshot: value_cell.version,
//                                 lookup_kind: LookupKind::Range,
//                             }
//                         },
//                         Err(..) =>
//                             return Err(Error::LookupRangeTimedOutFirst { key_from: key.clone(), key_to: key, value_cell, }),
//                     };
//                     assert!(lookup_range.key_values_rx.next().await.is_none());
//                     Ok(result)
//                 });
//             },
//             Backend::Sled { database, } => {
//                 let database = database.clone();
//                 let blocks_pool = blocks_pool.clone();
//                 let op_timeout = Duration::from_secs(limits.timeout_lookup_range_secs);
//                 let key_clone = key.clone();
//                 let value_cell_clone = value_cell.clone();
//                 spawn_task(supervisor_pid, done_tx.clone(), async move {
//                     let sled_key = key.key_bytes.clone();
//                     let lookup_range_task = tokio::task::spawn_blocking(move || {
//                         let mut iter = database.range(&*sled_key ..= &*sled_key);
//                         let task_done = match iter.next() {
//                             None =>
//                                 return Err(Error::ExpectedValueNotFound { key, lookup_kind: LookupKind::Range, }),
//                             Some(Ok((_found_key_bin, found_bin))) => {
//                                 let sled_entry: SledEntry<'_> = bincode::deserialize(&found_bin)
//                                     .map_err(Error::SledDeserialize)?;
//                                 match sled_entry.value {
//                                     SledValue::Value { data, } => {
//                                         let mut value_block = blocks_pool.lend();
//                                         value_block.extend_from_slice(data);
//                                         TaskDone::Lookup {
//                                             key: key.clone(),
//                                             found_value_cell: Some(kv::ValueCell {
//                                                 version: sled_entry.version,
//                                                 cell: kv::Cell::Value(value_block.into()),
//                                             }),
//                                             version_snapshot: value_cell.version,
//                                             lookup_kind: LookupKind::Range,
//                                         }
//                                     },
//                                     SledValue::Tombstone =>
//                                         TaskDone::Lookup {
//                                             key: key.clone(),
//                                             found_value_cell: Some(kv::ValueCell {
//                                                 version: sled_entry.version,
//                                                 cell: kv::Cell::Tombstone,
//                                             }),
//                                             version_snapshot: value_cell.version,
//                                             lookup_kind: LookupKind::Range,
//                                         },
//                                 }
//                             },
//                             Some(Err(error)) =>
//                                 return Err(Error::LookupRangeSledNext(error)),
//                         };
//                         match iter.next() {
//                             None =>
//                                 (),
//                             Some(Ok((found_key_bin, found_value_bin))) => {
//                                 let mut key_block = blocks_pool.lend();
//                                 key_block.extend_from_slice(&found_key_bin);
//                                 let mut value_block = blocks_pool.lend();
//                                 value_block.extend_from_slice(&found_value_bin);
//                                 return Err(Error::UnexpectedValueForLookupRange {
//                                     key: key,
//                                     key_value_pair: kv::KeyValuePair {
//                                         key: key_block.into(),
//                                         value_cell: kv::ValueCell {
//                                             version: 0,
//                                             cell: kv::Cell::Value(value_block.into()),
//                                         },
//                                     },
//                                 });
//                             },
//                             Some(Err(error)) =>
//                                 return Err(Error::LookupRangeSledLast(error)),
//                         }
//                         Ok(task_done)
//                     });
//                     match tokio::time::timeout(op_timeout, lookup_range_task).await {
//                         Ok(Ok(result)) =>
//                             result,
//                         Ok(Err(error)) =>
//                             Err(Error::LookupTaskJoin(error)),
//                         Err(..) =>
//                             Err(Error::LookupRangeTimedOut {
//                                 key_from: key_clone.clone(),
//                                 key_to: key_clone.clone(),
//                                 value_cell: value_cell_clone,
//                             }),
//                     }
//                 });
//             },
//         }
//     }

//     fn spawn_remove_task(
//         &mut self,
//         supervisor_pid: &mut SupervisorPid,
//         done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
//         blocks_pool: &BytesPool,
//         version_provider: &blockwheel_kv_ero::version::Provider,
//         key: kv::Key,
//         limits: &toml_config::Bench,
//     )
//     {
//         match self {
//             Backend::BlockwheelKv { wheel_kv_pid, .. } => {
//                 let mut wheel_kv_pid = wheel_kv_pid.clone();
//                 let op_timeout = Duration::from_secs(limits.timeout_remove_secs);
//                 spawn_task(supervisor_pid, done_tx.clone(), async move {
//                     let remove_task = tokio::time::timeout(
//                         op_timeout,
//                         wheel_kv_pid.remove(key.clone()),
//                     );
//                     match remove_task.await {
//                         Ok(Ok(blockwheel_kv_ero::Removed { version, })) =>
//                             Ok(TaskDone::Remove { key, version, }),
//                         Ok(Err(error)) =>
//                             Err(Error::Remove(error)),
//                         Err(..) =>
//                             Err(Error::RemoveTimedOut { key, }),
//                     }
//                 });
//             },
//             Backend::Sled { database, } => {
//                 let database = database.clone();
//                 let op_timeout = Duration::from_secs(limits.timeout_remove_secs);
//                 let blocks_pool = blocks_pool.clone();
//                 let version_provider = version_provider.clone();
//                 spawn_task(supervisor_pid, done_tx.clone(), async move {
//                     let sled_key = key.clone();
//                     let remove_task = tokio::task::spawn_blocking(move || {
//                         let mut sled_value_block = blocks_pool.lend();
//                         let version = version_provider.obtain();
//                         bincode::serialize_into(&mut **sled_value_block, &SledEntry {
//                             version,
//                             value: SledValue::Tombstone,
//                         }).map_err(Error::SledSerialize)?;
//                         database.insert(&*sled_key.key_bytes, &***sled_value_block)
//                             .map_err(Error::RemoveSled)
//                             .map(|_| TaskDone::Remove { key: sled_key, version, })
//                     });
//                     match tokio::time::timeout(op_timeout, remove_task).await {
//                         Ok(Ok(result)) =>
//                             result,
//                         Ok(Err(error)) =>
//                             Err(Error::RemoveTaskJoin(error)),
//                         Err(..) =>
//                             Err(Error::RemoveTimedOut { key, }),
//                     }
//                 });
//             },
//         }
//     }

    async fn flush(&mut self, limits: &toml_config::Bench) -> Result<(), Error> {

        todo!()
//         let op_timeout = Duration::from_secs(limits.timeout_flush_secs);
//         match self {
//             Backend::BlockwheelKv { wheel_kv_pid, wheels, } => {
//                 let flush_task = tokio::time::timeout(
//                     op_timeout,
//                     wheel_kv_pid.flush(),
//                 );
//                 let blockwheel_kv_ero::Flushed = flush_task.await
//                     .map_err(|_| Error::FlushTimedOut)
//                     .and_then(|result| result.map_err(Error::Flush))?;
//                 log::info!("blockwheel_kv flushed");
//                 let flush_task = tokio::time::timeout(
//                     op_timeout,
//                     wheels.flush(),
//                 );
//                 let wheels::Flushed = flush_task.await
//                     .map_err(|_| Error::FlushTimedOut)
//                     .and_then(|result| result.map_err(Error::WheelsFlush))?;
//                 log::info!("blockwheel_fs flushed");
//             },
//             Backend::Sled { database, } => {
//                 let flush_task = tokio::time::timeout(
//                     op_timeout,
//                     database.flush_async(),
//                 );
//                 flush_task.await
//                     .map_err(|_| Error::FlushTimedOut)
//                     .and_then(|result| result.map_err(Error::FlushSled))?;
//             },
//         }
//         Ok(())
    }
}

enum Job {
    BlockwheelKvEro(blockwheel_kv_ero::job::Job),
    PrepareInsert(edeltraud::AsyncJob<PrepareInsertJob>),
}

impl From<blockwheel_kv_ero::job::Job> for Job {
    fn from(job: blockwheel_kv_ero::job::Job) -> Job {
        Job::BlockwheelKvEro(job)
    }
}

impl From<edeltraud::AsyncJob<PrepareInsertJob>> for Job {
    fn from(job: edeltraud::AsyncJob<PrepareInsertJob>) -> Job {
        Job::PrepareInsert(job)
    }
}

impl edeltraud::Job for Job {
    fn run<P>(self, thread_pool: &P) where P: edeltraud::ThreadPool<Self> {
        match self {
            Job::BlockwheelKvEro(job) => {
                job.run(&edeltraud::ThreadPoolMap::new(thread_pool));
            },
            Job::PrepareInsert(job) => {
                job.run(&edeltraud::ThreadPoolMap::new(thread_pool));
            },
        }
    }
}

struct PrepareInsertJob {
    huge_random_block: Bytes,
    limits: Arc<toml_config::Bench>,
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

        Ok(TaskDone::PrepareInsert { key_block, value_block, value_crc64, })
    }
}


enum TaskDone {
    PrepareInsert {
        key_block: Bytes,
        value_block: Bytes,
        value_crc64: ValueCrc64,
    },
    Insert {
        key: kv::Key,
        serial: usize,
        version: u64,
    },
    Remove {
        key: kv::Key,
        serial: usize,
        version: u64,
    },
    // Lookup {
    //     key: kv::Key,
    //     found_value_cell: Option<kv::ValueCell<kv::Value>>,
    // },
}

// #[derive(Debug)]
// pub enum LookupKind { Single, Range, }

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
    fn process<P>(
        self,
        backend: &mut Backend,
        db_mirror: &mut DbMirror,
        supervisor_pid: &mut SupervisorPid,
        done_tx: &mpsc::Sender<Result<TaskDone, Error>>,
        blocks_pool: &BytesPool,
        version_provider: &blockwheel_kv_ero::version::Provider,
        counter: &mut Counter,
        active_tasks_counter: &mut Counter,
        actions_counter: &mut usize,
        limits: &toml_config::Bench,
        thread_pool: &P,
    )
        -> Result<(), Error>
    where P: edeltraud::ThreadPool<Job> + Clone,
    {
        match self {

            TaskDone::PrepareInsert { key_block, value_block, value_crc64 } => {
                log::debug!(
                    "{}. performing INSERT with {} bytes key and {} bytes value | {:?}, active = {:?}",
                    actions_counter,
                    key_block.len(),
                    value_block.len(),
                    counter,
                    active_tasks_counter,
                );
                let key = kv::Key { key_bytes: key_block, };

                backend.spawn_insert_task(
                    supervisor_pid,
                    done_tx,
                    blocks_pool,
                    version_provider,
                    key.clone(),
                    value_block,
                    limits,
                    thread_pool,
                );

                db_mirror.enqueue_mutation(key, kv::Cell::Value(value_crc64));
            },

            TaskDone::Insert { key, serial, version, } => {
                let db_mirror_value = db_mirror.data.get_mut(&key)
                    .ok_or_else(|| Error::InsertedValueNotFoundInDbMirror { key: key.clone(), })?;
                db_mirror_value.commit(key.clone(), serial, version)?;
                log::debug!(" ;; inserted: {:?}", key);
                counter.inserts += 1;
                active_tasks_counter.inserts -= 1;
            },

            TaskDone::Remove { key, serial, version, } => {
                let db_mirror_value = db_mirror.data.get_mut(&key)
                    .ok_or_else(|| Error::RemovedValueNotFoundInDbMirror { key: key.clone(), })?;
                db_mirror_value.commit(key.clone(), serial, version)?;
                log::debug!(" ;; removed: {:?}", key);
                counter.removes += 1;
                active_tasks_counter.removes -= 1;
            },

//             TaskDone::Lookup { key, found_value_cell, version_snapshot, lookup_kind, } => {
//                 let &offset = data.index.get(&key).unwrap();
//                 let kv::KeyValuePair { value_cell: kv::ValueCell { version: version_current, cell: ref cell_current, }, .. } = data.data[offset];
//                 match (found_value_cell, cell_current) {
//                     (None, kv::Cell::Tombstone) =>
//                         log::warn!("deprecated lookup: already removed during {:?}", lookup_kind),
//                     (None, kv::Cell::Value(..)) =>
//                         return Err(Error::ExpectedValueNotFound { key, lookup_kind, }),
//                     (Some(found_value_cell), cell_current) => {
//                         let kv::ValueCell { version: version_found, cell: ref cell_found, } = found_value_cell;
//                         if version_found == version_current {
//                             if cell_found == cell_current {
//                                 // everything is up to date
//                             } else {
//                                 // version matches, but actual values are not
//                                 return Err(Error::UnexpectedValueFound {
//                                     key,
//                                     expected_value_cell: data.data[offset].value_cell.clone(),
//                                     found_value_cell,
//                                 });
//                             }
//                         } else if version_snapshot < version_current {
//                             // deprecated lookup (ignoring)
//                             log::debug!("deprecated lookup: awaiting version {} but there is {} already", version_snapshot, version_current);
//                         } else {
//                             // premature lookup (ignoring, don't want to wait)
//                             log::debug!(
//                                 "premature lookup: found version {} but current is still {} (awaiting {})",
//                                 version_found,
//                                 version_current,
//                                 version_snapshot,
//                             );
//                         }
//                     },
//                 }
//                 match lookup_kind {
//                     LookupKind::Single => {
//                         counter.lookups += 1;
//                         active_tasks_counter.lookups -= 1;
//                     },
//                     LookupKind::Range => {
//                         counter.lookups_range += 1;
//                         active_tasks_counter.lookups_range -= 1;
//                     },
//                 }
//             }
        }
        Ok(())
    }
}
