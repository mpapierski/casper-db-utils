use std::{sync::Arc, time::Instant};

use casper_execution_engine::{
    core::engine_state::{
        purge::{PurgeConfig, PurgeResult},
        EngineConfig, EngineState,
    },
    shared::newtypes::CorrelationId,
    storage::{global_state::lmdb::LmdbGlobalState, trie_store::lmdb::LmdbTrieStore},
};
use casper_hashing::Digest;
use casper_types::{EraId, Key};
use clap::{Arg, ArgMatches, Command};

use crate::common::execution_engine::{create_lmdb_environment, DEFAULT_MAX_READERS};

pub const COMMAND_NAME: &str = "purge";
const DB_PATH: &str = "file-path";
const _MAX_DB_SIZE: &str = "max-db-size";
const STATE_ROOT_HASH: &str = "state-root-hash";
const DEFAULT_MAX_DB_SIZE: usize = 483_183_820_800; // 450 gb

pub fn command(_display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .about(
            "Reduces the disk size of an LMDB database generated by a Casper node by removing \
            empty blocks of the sparse file.",
        )
        .arg(
            Arg::new(DB_PATH)
                .value_name("DB_PATH")
                .required(true)
                .help("Path to the storage.lmdb or data.lmdb file."),
        )
        .arg(
            Arg::new(STATE_ROOT_HASH)
                .value_name("STATE_ROOT_HASH")
                .required(true)
                .help("State root hash of the tip of the trie."),
        )
        .arg(
            Arg::new("latest-era")
                .long("latest-era")
                .required(true)
                .takes_value(true)
                .help("Purges eras from 0..latest-era"),
        )
        .arg(
            Arg::new("batch-size")
                .long("batch-size")
                .takes_value(true)

                .required(true)
                .help("Chunk size per purge"),
        )
}

pub fn run(matches: &ArgMatches) -> bool {
    let db_path = matches.value_of(DB_PATH).unwrap();
    let state_root_hash = matches.value_of(STATE_ROOT_HASH).unwrap();
    let mut state_root_hash = Digest::from_hex(state_root_hash).unwrap();
    let latest_era: u64 = matches.value_of("latest-era").unwrap().parse().unwrap();
    let batch_size: usize = matches.value_of("batch-size").unwrap().parse().unwrap();
    let lmdb_environment = create_lmdb_environment(&db_path, DEFAULT_MAX_DB_SIZE, DEFAULT_MAX_READERS, true)
        .expect("create lmdb environment");

    let lmdb_trie_store = Arc::new(LmdbTrieStore::open(&lmdb_environment, None).unwrap());

    let (empty_root_hash, _empty_trie) =
        casper_execution_engine::storage::global_state::lmdb::compute_empty_root_hash().unwrap();
    let global_state = LmdbGlobalState::new(
        Arc::clone(&lmdb_environment),
        lmdb_trie_store,
        empty_root_hash,
    );

    let state = EngineState::new(global_state, EngineConfig::default());

    let keys_to_purge: Vec<Key> = (0..latest_era).map(EraId::new).map(Key::EraInfo).collect();

    println!("Purging {} keys with batch size of {}... Total batches {}", keys_to_purge.len(), batch_size, keys_to_purge.len() / batch_size);

    let start = Instant::now();

    for (i, keys) in keys_to_purge.chunks(batch_size).enumerate() {
        let purge_config = PurgeConfig::new(state_root_hash, keys.to_owned());

        let commit_purge = Instant::now();
        let result = state
            .commit_purge(CorrelationId::new(), purge_config)
            .expect("should purge");
        let purge_elapsed = commit_purge.elapsed();

        match result {
            PurgeResult::Success { post_state_hash } => {
                // if post_state_hash == state_root_hash {
                //     println!("(!) No purge was performed.");
                // } else {
                // }

                // println!("State root hash after purge: {}", post_state_hash,);
                println!("Purge chunk {} elapsed {:?}", i, purge_elapsed);
                state_root_hash = post_state_hash;
            }
            other => panic!("Expected success but received {:?}", other),
        }
    }

    let elapsed = start.elapsed();

    println!("Purge finished in {:?}", elapsed);

    true
}
