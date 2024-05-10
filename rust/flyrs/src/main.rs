use anyhow::{bail, Result};
use clap::Parser;
<<<<<<< Updated upstream
use tracing::info;
use tokio;
use tokio::runtime::Builder;
use tracing_subscriber;
=======
use env_logger::Env;
use log::info;
use tokio;
use tokio::runtime::Builder;
>>>>>>> Stashed changes

mod executor;
mod distribution;


fn main() -> Result<()> {

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