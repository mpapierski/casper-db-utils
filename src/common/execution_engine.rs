use std::{path::Path, sync::Arc};

use casper_execution_engine::storage::transaction_source::lmdb::LmdbEnvironment;

/// LMDB max readers
///
/// The default value is chosen to be the same as the node itself.
pub const DEFAULT_MAX_READERS: u32 = 5;

/// Create an lmdb environment at a given path.
pub fn create_lmdb_environment(
    lmdb_path: impl AsRef<Path>,
    default_max_db_size: usize,
    max_readers: u32,
    manual_sync_enabled: bool,
) -> anyhow::Result<Arc<LmdbEnvironment>> {
    let lmdb_environment = Arc::new(LmdbEnvironment::new(
        &lmdb_path,
        default_max_db_size,
        max_readers,
        manual_sync_enabled,
    )?);
    Ok(lmdb_environment)
}
