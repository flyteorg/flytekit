use std::fmt::{Display, Formatter};

use clap::Parser;
use log::{debug, info, log_enabled};
use log::Level::Debug;
use log_derive::{logfn, logfn_inputs};
use pyo3::prelude::*;

use distribution::download_unarchive_distribution;

use crate::distribution;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct ExecutorArgs {
    #[arg(short, long, required = true)]
    inputs: String,
    #[arg(short, long, required = true)]
    output_prefix: String,
    #[arg(short, long, default_value = "false")]
    test: bool,
    #[arg(short='w', long, required = true)]
    raw_output_data_prefix: String,
    #[arg(short, long, required = true)]
    resolver: String,
    #[arg(last = true, required = true)]
    resolver_args: Vec<String>,
    #[arg(short, long)]
    checkpoint_path: Option<String>,
    #[arg(short, long)]
    prev_checkpoint: Option<String>,
    #[arg(short, long)]
    dynamic_addl_distro: Option<String>,
    #[arg(long)]
    dynamic_dest_dir: Option<String>,
}

impl Display for ExecutorArgs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ExecutorArgs {{ inputs: {}, output_prefix: {}, test: {}, raw_output_data_prefix: {}, resolver: {}, resolver_args: {:?}, checkpoint_path: {:?}, prev_checkpoint: {:?}, dynamic_addl_distro: {:?}, dynamic_dest_dir: {:?} }}",
               self.inputs, self.output_prefix, self.test, self.raw_output_data_prefix,
               self.resolver, self.resolver_args, self.checkpoint_path, self.prev_checkpoint,
               self.dynamic_addl_distro, self.dynamic_dest_dir)
    }
}

// def _execute_task(
//     inputs: str,
//     output_prefix: str,
//     test: bool,
//     raw_output_data_prefix: str,
//     resolver: str,
//     resolver_args: List[str],
//     checkpoint_path: Optional[str] = None,
//     prev_checkpoint: Optional[str] = None,
//     dynamic_addl_distro: Optional[str] = None,
//     dynamic_dest_dir: Optional[str] = None,
// ):

fn debug_python_setup(py: Python) {
    if log_enabled!(Debug) {
        let sys = PyModule::import_bound(py, "sys").unwrap();
        let path = sys.getattr("path").unwrap();
        let version = sys.getattr("version").unwrap();
        let modules = sys.getattr("modules").unwrap();
        let keys = modules.call_method0("keys").unwrap();
        debug!("Python path: {:?}", path);
        debug!("Python version: {:?}", version);
        debug!("Python modules: {:?}", keys);
    }
}

#[logfn_inputs(Info, fmt = "Invoking task with {}")]
#[logfn(ok = "INFO", err = "ERROR")]
pub async fn execute_task(args: &ExecutorArgs) -> Result<(), Box<dyn std::error::Error>>{
    pyo3::prepare_freethreaded_python();
    let _ = Python::with_gil(|py| -> Result<(), Box<dyn std::error::Error>> {
        debug_python_setup(py);
        let entrypoint = PyModule::import_bound(py, "flytekit.bin.entrypoint").unwrap();

        let resolver_args_py = args.resolver_args.clone().into_py(py);
        let dynamic_addl_distro_py = &args.dynamic_addl_distro.clone().into_py(py);
        let dynamic_dest_dir = &args.dynamic_dest_dir.clone().into_py(py);
        let checkppoint_path_py = &args.checkpoint_path.clone().into_py(py);
        let prev_checkpoint_py = &args.prev_checkpoint.clone().into_py(py);

        let args = (
            &args.inputs,
            &args.output_prefix,
            args.test,
            &args.raw_output_data_prefix,
            &args.resolver,
            resolver_args_py,
            checkppoint_path_py,
            prev_checkpoint_py,
            dynamic_addl_distro_py,
            dynamic_dest_dir
        );
        debug!("Invoking task with args {:?}", args);

        let result = entrypoint.call_method1("_execute_task", args).unwrap();

        if !result.is_none() {
            debug!("Task failed");
            return Err("Task failed".into());
        }
        debug!("Task completed");
        Ok(())
    });
    Ok(())

}

#[logfn_inputs(Info, fmt = "Executor invoked with args{}")]
#[logfn(ok = "INFO", err = "ERROR")]
pub async fn run(executor_args: &ExecutorArgs) -> Result<(), Box<dyn std::error::Error>> {
    if executor_args.dynamic_addl_distro.is_some() {
        info!("Found Dynamic distro {:?}", executor_args.dynamic_addl_distro);
        if executor_args.dynamic_dest_dir.is_none() {
            return Err("Dynamic distro requires a destination directory".into());
        }
        let src_url = url::Url::parse(executor_args.dynamic_addl_distro.clone().unwrap().as_str())?;
        download_unarchive_distribution(&src_url, &executor_args.dynamic_dest_dir.clone().unwrap()).await?;
    }

    execute_task(executor_args).await?;
    Ok(())
}