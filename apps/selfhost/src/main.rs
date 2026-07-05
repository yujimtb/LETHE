use lethe_selfhost::self_host::app::AppService;
use lethe_selfhost::self_host::config::SelfHostConfig;
use lethe_selfhost::self_host::mcp::build_mcp_router;
use lethe_selfhost::self_host::server::build_router;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()?;
    let config = SelfHostConfig::from_env()?;
    let service = AppService::bootstrap(config.clone())?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let startup_service = service.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(err) = startup_service.sync_all() {
                tracing::error!(error = %err, "initial sync failed");
            }
        });

        service.spawn_polling_task();

        let router = build_router(service.clone());
        let mcp_router = build_mcp_router(service.clone());
        let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
        let mcp_listener = tokio::net::TcpListener::bind(&config.mcp_bind_addr).await?;
        tracing::info!(bind_addr = %config.bind_addr, "LETHE self-host internal API listening");
        tracing::info!(bind_addr = %config.mcp_bind_addr, "LETHE MCP read port listening");
        let internal_server =
            axum::serve(listener, router).with_graceful_shutdown(shutdown_signal());
        let mcp_server =
            axum::serve(mcp_listener, mcp_router).with_graceful_shutdown(shutdown_signal());
        tokio::try_join!(internal_server, mcp_server)?;

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
