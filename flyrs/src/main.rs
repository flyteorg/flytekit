use keyring::{Entry, Result};

pub mod auth;
pub mod remote;

use crate::{auth::Auth, remote::Raw};

fn main() -> Result<()> {
    println!("Hello, Flyrs!");

    let mut remote_client =
        Raw::FlyteClient::new("localhost:30080", false).unwrap();

    // remote_client.py_get_task(ObjectGetRequest{id:Identifier{}})

    println!("Goodbye, Flyrs!");
    Ok(())
}
