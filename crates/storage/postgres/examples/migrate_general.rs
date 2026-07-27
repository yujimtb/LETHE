use lethe_core::domain::DataSpaceId;
use lethe_storage_postgres::PostgresPersistence;

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let arguments = std::env::args().collect::<Vec<_>>();
    let [_, dsn, schema, expected_role, data_space_id, read_pool_size] = arguments.as_slice()
    else {
        return Err("usage: migrate_general <dsn> <schema> <expected-role> <data-space-id> <read-pool-size>".to_owned());
    };
    let read_pool_size = read_pool_size
        .parse::<usize>()
        .map_err(|error| format!("read-pool-size must be an unsigned integer: {error}"))?;
    let store = PostgresPersistence::connect_database_only_for_tests(
        DataSpaceId::new(data_space_id),
        dsn,
        schema,
        expected_role,
        read_pool_size,
    )
    .map_err(|error| error.to_string())?;
    store
        .deep_check_connections()
        .map_err(|error| error.to_string())?;
    store.read_check().map_err(|error| error.to_string())?;
    println!(
        "applied_versions={:?} read_pool_size={}",
        store.migration_outcome().applied_versions,
        store.read_pool_size()
    );
    Ok(())
}
