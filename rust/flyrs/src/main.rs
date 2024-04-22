use clap::Parser;
use log::info;
use tokio;
use tokio::runtime::Builder;
use env_logger::Env;

mod executor;
mod distribution;


fn main() -> Result<(), Box<dyn std::error::Error>> {

    let env = Env::default()
        .filter_or("FLYRS_LOG_LEVEL", "trace")
        .write_style_or("FLYRS_LOG_STYLE", "always");

    env_logger::init_from_env(env);

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