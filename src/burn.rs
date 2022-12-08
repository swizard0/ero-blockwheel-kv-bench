use std::{
    time::{
        Instant,
    },
};

use futures::{
    stream::{
        FuturesUnordered,
    },
    StreamExt,
};

use rand::{
    Rng,
    SeedableRng,
    rngs::{
        SmallRng,
    },
    seq::{
        SliceRandom,
    },
};

use alloc_pool::{
    bytes::{
        Bytes,
    },
};

use ero::{
    supervisor::{
        SupervisorGenServer,
    },
};

use crate::{
    Env,
    TaskDone,
    PrepareInsertJob,
};

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    BlockwheelKvInsert(blockwheel_kv_ero::InsertError),
    BlockwheelKvRemove(blockwheel_kv_ero::RemoveError),
    BlockwheelKvLookup(blockwheel_kv_ero::LookupError),
    BlockwheelKvFlushAll(blockwheel_kv_ero::FlushError),
}

pub(super) fn run(
    env: Env,
    runtime: tokio::runtime::Runtime,
)
    -> Result<(), Error>
{
    log::info!("generating burn input data");

    let thread_pool = env.edeltraud.handle();
    let blockwheel_kv_gen_server = blockwheel_kv_ero::GenServer::new();
    let mut blockwheel_kv_pid = blockwheel_kv_gen_server.pid();
    let mut rng = SmallRng::from_entropy();

    let mut entries = Vec::new();
    for _ in 0 .. env.config.bench.actions {
        let job = PrepareInsertJob {
            huge_random_block: env.huge_random_block.clone(),
            limits: env.limits.clone(),
            action_start: Instant::now(),
        };
        let Ok(TaskDone::PrepareInsert { key_block, value_block, .. }) = edeltraud::Computation::run(job) else {
            unreachable!();
        };
        entries.push((key_block, value_block));
    }
    entries.shuffle(&mut rng);

    enum Action {
        Insert { key_bytes: Bytes, value_bytes: Bytes, },
        Remove { key_bytes: Bytes, },
        Lookup { key_bytes: Bytes, },
    }

    let total_mutations = (env.config.bench.actions as f64 * env.config.bench.insert_or_remove_prob) as usize;
    let total_inserts = total_mutations / 2;
    let total_removes = total_mutations - total_inserts;
    let total_lookups = env.config.bench.actions - total_mutations;

    let mut actions = Vec::new();

    for _ in 0 .. total_inserts {
        let entry_index = rng.gen_range(0 .. entries.len());
        let (key_bytes, value_bytes) = entries[entry_index].clone();
        actions.push(Action::Insert { key_bytes, value_bytes, });
    }

    for _ in 0 .. total_removes {
        let entry_index = rng.gen_range(0 .. entries.len());
        let (key_bytes, _) = entries[entry_index].clone();
        actions.push(Action::Remove { key_bytes, });
    }

    for _ in 0 .. total_lookups {
        let entry_index = rng.gen_range(0 .. entries.len());
        let (key_bytes, _) = entries[entry_index].clone();
        actions.push(Action::Lookup { key_bytes, });
    }

    actions.shuffle(&mut rng);

    let mut futures: FuturesUnordered<_> = actions.into_iter()
        .map(|action| {
            let mut blockwheel_kv_pid = blockwheel_kv_pid.clone();
            async move {
                match action {
                    Action::Insert { key_bytes, value_bytes, } => {
                        let blockwheel_kv::Inserted { .. } = blockwheel_kv_pid
                            .insert(key_bytes.into(), value_bytes.into())
                            .await
                            .map_err(Error::BlockwheelKvInsert)?;
                        Ok::<_, Error>(())
                    },
                    Action::Remove { key_bytes, } => {
                        let blockwheel_kv::Removed { .. } = blockwheel_kv_pid
                            .remove(key_bytes.into())
                            .await
                            .map_err(Error::BlockwheelKvRemove)?;
                        Ok(())
                    },
                    Action::Lookup { key_bytes, } => {
                        let _found_value_cell = blockwheel_kv_pid.
                            lookup(key_bytes.into())
                            .await
                            .map_err(Error::BlockwheelKvLookup)?;
                        Ok(())
                    },
                }
            }
        })
        .collect();

    log::info!("generating done: total_inserts = {total_inserts}, total_removes = {total_removes}, total_lookups = {total_lookups}");

    let run_result = runtime.block_on(async {
        let supervisor_gen_server = SupervisorGenServer::new();
        let mut supervisor_pid = supervisor_gen_server.pid();
        tokio::spawn(supervisor_gen_server.run());

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

        while let Some(future_result) = futures.next().await {
            future_result?;
        }

        let blockwheel_kv::Flushed = blockwheel_kv_pid.flush_all().await
            .map_err(Error::BlockwheelKvFlushAll)?;

        Ok(())
    });

    let _ = env;
    run_result
}
