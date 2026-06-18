use lethe::self_host::app::AppService;
use lethe::self_host::config::SelfHostConfig;
use lethe::self_host::server::build_router;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SelfHostConfig::from_env()?;
    let service = AppService::bootstrap(config.clone())?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let startup_service = service.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(err) = startup_service.sync_all() {
                eprintln!("initial sync failed: {err}");
            }
        });

        service.spawn_polling_task();

        let router = build_router(service.clone());
        let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
        println!("LETHE self-host listening on http://{}", config.bind_addr);
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;

        Ok::<(), Box<dyn std::error::Error>>(())
    })?;

    Ok(())
}
