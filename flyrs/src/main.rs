use keyring::{Entry, Result};

pub mod auth;
pub mod remote;

use crate::{auth::Auth, remote::Raw};

fn main() -> Result<()> {
    // let mut oauth_client = Auth::OAuthClient::new("dogfood-gcp.cloud-staging.union.ai");
    // oauth_client.authenticate();
    println!("Hello, Rust here!");

    // let credentials_for_endpoint = "flyte-default";
    // let credentials_access_token_key = "access_token";
    // let entry = Entry::new(credentials_for_endpoint, credentials_access_token_key)?;
    // let stored_access_token = match entry.get_password() {
    //     Ok(stored_access_token) => {
    //         println!("KeyRing get successfully.");
    //         stored_access_token
    //     }
    //     Err(err) => {
    //         println!("KeyRing get not available.");
    //         "".to_string()
    //     }
    // };

    // println!("Keyring retrieved g following token:\n{stored_access_token:?}\n");

    let mut remote_client = Raw::FlyteClient::new("dogfood-gcp.cloud-staging.union.ai"); //""
    println!("Goodbyt, Rust there!");
    Ok(())
}
