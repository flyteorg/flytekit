use anyhow::Result;
use oauth2::basic::BasicClient;
use oauth2::{
    AccessToken, AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, PkceCodeChallenge,
    RedirectUrl, Scope, TokenResponse, TokenUrl,
};
// Please make sure `ureq` feature flag is enabled. FYR: https://docs.rs/oauth2/latest/oauth2/#importing-oauth2-selecting-an-http-client-interface
use oauth2::ureq::http_client;
use url::Url;

use std::env;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

// TODO: The following section should reside in `src/lib.rs`
/*
pub mod authenticator;

// let access_token = authenticator::PKCEAuthentication();
// let token: MetadataValue<_> = "Bearer <access_token>".parse()?;

// Add Python binding for this `get_access_token` function may not be necessary.
// We can persisit credentials in local keyring.
// m.add_function(wrap_pyfunction!(authenticator::PKCEAuthentication, m)?).unwrap();
*/

// TODO: The following section should reside in `Cargo.toml`
/*
oauth2 ={ version = "4.4.2", features = ["ureq"] }
url = "2.5.0"
anyhow = "1.0.83"
*/

pub struct Authenticator {
}

pub fn PKCEAuthentication() -> Result<()> {
    // Create an OAuth2 client (auth0 from Okta) by specifying the client ID, client secret, authorization URL and token URL.
    let client = BasicClient::new(
        ClientId::new(env::var("CLIENT_ID").expect("Missing the CLIENT_ID environment variable.").to_string()),
        Some(ClientSecret::new(env::var("CLIENT_SECRET")
        .expect("Missing the CLIENT_SECRET environment variable.").to_string(),
        )),
        AuthUrl::new(format!("{}/authorize", env::var("BASE_DOMAIN").expect("Missing the BASE_DOMAIN environment variable.")).to_string()).unwrap(),
        // Be careful that the `TokenUrl` endpoint in the official documeantion is `<BASE_DOMAIN>/token`.
        // FYR: https://docs.rs/oauth2/latest/oauth2/#example-synchronous-blocking-api
        Some(
            TokenUrl::new(format!("{}/oauth/token", env::var("BASE_DOMAIN").expect("Missing the BASE_DOMAIN environment variable.")).to_string())
                .unwrap(),
        ),
    )
    // Set the URL the user will be redirected to after the authorization process.
    .set_redirect_uri(RedirectUrl::new("http://localhost:30080/callback".to_string()).unwrap());

    // Generate a PKCE challenge.
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

    // Generate the full authorization URL.
    let (auth_url, csrf_state) = client
        .authorize_url(CsrfToken::new_random)
        // Set the desired scopes.
        .add_scope(Scope::new("read".to_string()))
        .add_scope(Scope::new("write".to_string()))
        // Set the PKCE code challenge.
        .set_pkce_challenge(pkce_challenge)
        .url();

    // This is the URL you should redirect the user to, in order to trigger the authorization
    // process.
    println!("Browse to: {}", auth_url);

    // Once the user has been redirected to the redirect URL, you'll have access to the
    // authorization code. For security reasons, your code should verify that the `state`
    // parameter returned by the server matches `csrf_state`.

    let (code, state) = {
        // A very naive implementation of the redirect server.
        // Prepare the callback server in the background that listen to our flyeadmin endpoint.
        let listener = TcpListener::bind("127.0.0.1:30080").unwrap();

        // The server will terminate itself after collecting the first code.
        let Some(mut stream) = listener.incoming().flatten().next() else {
            panic!("listener terminated without accepting a connection");
        };

        let mut reader = BufReader::new(&stream);

        let mut request_line = String::new();
        reader.read_line(&mut request_line).unwrap();

        let redirect_url = request_line.split_whitespace().nth(1).unwrap();
        let url = Url::parse(&("http://localhost".to_string() + redirect_url)).unwrap();

        let code = url
            .query_pairs()
            .find(|(key, _)| key == "code")
            .map(|(_, code)| AuthorizationCode::new(code.into_owned()))
            .unwrap();

        let state = url
            .query_pairs()
            .find(|(key, _)| key == "state")
            .map(|(_, state)| CsrfToken::new(state.into_owned()))
            .unwrap();

        let message = "Go back to your terminal :)";
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{}",
            message.len(),
            message
        );
        stream.write_all(response.as_bytes()).unwrap();

        (code, state)
    };

    println!(
        "PKCE Authenticatior returned the following code:\n{}\n",
        code.secret()
    );
    println!(
        "PKCE Authenticatior returned the following state:\n{} (expected `{}`)\n",
        state.secret(),
        csrf_state.secret()
    );

    // Exchange the code with a token.
    // Now you can trade it for an access token.
    let token_result = client
        .exchange_code(code)
        // Send the PKCE code verifier in the token request
        .set_pkce_verifier(pkce_verifier)
        .request(&http_client);

    let Ok(token_response) = token_result else {
        todo!()
    };
    let access_token = token_response.access_token().secret();

    println!("PKCE Authenticatior returned the following token:\n{access_token:?}\n");

    // Should return `access_token` as string for gRPC interceptor.
    // Just like what we did in flytekit remote `auth_interceptor.py`
    // L#36 `auth_metadata = self._authenticator.fetch_grpc_call_auth_metadata()`
    Ok(())
}