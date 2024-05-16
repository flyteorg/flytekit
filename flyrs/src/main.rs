use keyring::{Entry, Result};

pub mod authenticator;

fn main() -> Result<()> {
    authenticator::PKCEAuthentication();
    println!("Hallo, Rust here!");
    let credentials_for_endpoint = "flyte-default";
    let credentials_access_token_key = "access_token";
    let entry = Entry::new(credentials_for_endpoint, credentials_access_token_key)?;

    let stored_access_token = match entry.get_password() {
        Ok(stored_access_token) => {
            println!("KeyRing get successfully.");
            stored_access_token
        }
        Err(err) => {
            println!("KeyRing get not available.");
            "".to_string()
        }
    };

    println!("Keyring retrieved g following token:\n{stored_access_token:?}\n");

    Ok(())
}
