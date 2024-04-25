use clap::Parser;
use tracing::info;
use tokio;
use tokio::runtime::Builder;
use tracing_subscriber;

mod executor;
mod distribution;


fn main() -> Result<(), Box<dyn std::error::Error>> {

    tracing_subscriber::fmt::init();

    let args = executor::ExecutorArgs::parse();

    let runtime = Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();

    info!("Starting executor...");
    runtime.block_on(executor::run(&args))?;
    info!("Executor completed.");

    Ok(())
}