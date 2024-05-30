pub mod Auth {
    use anyhow::Result;
    use flyteidl::flyteidl::service::{
        auth_metadata_service_client::AuthMetadataServiceClient, OAuth2MetadataRequest,
        PublicClientAuthConfigRequest,
    };
    use oauth2::basic::BasicClient;
    use oauth2::{
        AccessToken, AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken,
        PkceCodeChallenge, RedirectUrl, RevocationUrl, Scope, StandardRevocableToken,
        TokenResponse, TokenUrl,
    };
    use std::fmt;
    use std::sync::Arc;
    use tokio::runtime::{Builder, Runtime};
    use tonic::{
        metadata::MetadataValue,
        service::interceptor::InterceptedService,
        service::Interceptor,
        transport::{Channel, Endpoint, Uri},
        Request, Response, Status,
    };
    // Please make sure `ureq` feature flag is enabled. FYR: https://docs.rs/oauth2/latest/oauth2/#importing-oauth2-selecting-an-http-client-interface
    use oauth2::ureq::http_client;
    use url::Url;

    use std::env;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    use keyring::Entry;

    #[derive(Clone, Default)]
    pub struct Credential {
        access_token: String,
        refresh_token: String,
    }

    impl Credential {
        fn default() -> Credential {
            Credential {
                access_token: "".into(),
                refresh_token: "".into(),
            }
        }
        // pub fn store()-> () {

        // }
    }

    #[derive(Clone, Default)]
    struct ClientConfig {
        server_endpoint: String,
        token_endpoint: String,
        authorization_endpoint: String,
        redirect_uri: String,
        client_id: String,
        device_authorization_endpoint: String,
        scopes: Vec<String>,
        header_key: String,
        audience: String,
    }

    // #[derive(Clone, Default)]
    // struct OAuth2Metadata {
    //     token_endpoint: String,
    //     authorization_endpoint: String,
    // }

    // impl ClientConfig {
    //     fn default() -> ClientConfig {
    //         ClientConfig {
    //             token_endpoint: "".into(),
    //             authorization_endpoint: "".into(),
    //             redirect_uri: "".into(),
    //             client_id: "".into(),
    //             device_authorization_endpoint: None,
    //             scopes: Vec::<String>::with_capacity(10),
    //             header_key: "authorization ".into(),
    //             audience: "".into(),
    //         }
    //     }
    // }

    pub struct OAuthClient {
        auth_service: AuthMetadataServiceClient<Channel>,
        runtime: Runtime,
        public_client_config: ClientConfig,
        // oauth2_metadata: OAuth2Metadata,
        credentials: Credential,
    }

    impl OAuthClient {
        pub fn new(endpoint: &str) -> OAuthClient {
            // Check details for constructing Tokio asynchronous `runtime`: https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.new_current_thread
            let rt = match Builder::new_current_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(error) => panic!("Failed to initiate Tokio multi-thread runtime: {:?}", error),
            };

            let endpoint = Arc::new(endpoint);
            let endpoint_uri = match format!("https://{}", *endpoint.clone()).parse::<Uri>() {
                Ok(uri) => uri,
                Err(error) => panic!(
                    "Got invalid endpoint when parsing endpoint_uri: {:?}",
                    error
                ),
            };
            let channel = match rt.block_on(Channel::builder(endpoint_uri).connect()) {
                Ok(ch) => ch,
                Err(error) => panic!(
                    "Failed at connecting to endpoint when constructing channel: {:?}",
                    error
                ),
            };
            let mut auth_metadata_stub = AuthMetadataServiceClient::new(channel);

            let public_client_auth_config_request: PublicClientAuthConfigRequest =
                PublicClientAuthConfigRequest {};
            let req = Request::new(public_client_auth_config_request);
            let public_cfg_res =
                (match rt.block_on(auth_metadata_stub.get_public_client_config(req)) {
                    Ok(res) => res,
                    Err(error) => panic!(
                        "Failed at awaiting response from gRPC service server: {:?}",
                        error
                    ),
                })
                .into_inner();

            let oauth2_metadata_request: OAuth2MetadataRequest = OAuth2MetadataRequest {};
            let req = Request::new(oauth2_metadata_request);
            let oauth_mtdata_res = (match rt.block_on(auth_metadata_stub.get_o_auth2_metadata(req))
            {
                Ok(res) => res,
                Err(error) => panic!(
                    "Failed at awaiting response from gRPC service server: {:?}",
                    error
                ),
            })
            .into_inner();
            // oauth2_metadata = stub.GetOAuth2Metadata(OAuth2MetadataRequest())
            let client_config: ClientConfig = ClientConfig {
                server_endpoint: (*endpoint.clone()).to_string(),
                token_endpoint: oauth_mtdata_res.token_endpoint,
                authorization_endpoint: oauth_mtdata_res.authorization_endpoint,
                redirect_uri: public_cfg_res.redirect_uri,
                client_id: public_cfg_res.client_id,
                scopes: public_cfg_res.scopes,
                header_key: public_cfg_res.authorization_metadata_key,
                device_authorization_endpoint: oauth_mtdata_res.device_authorization_endpoint,
                audience: public_cfg_res.audience,
            };

            let mut credentials = Credential::default();

            OAuthClient {
                auth_service: auth_metadata_stub,
                runtime: rt,
                public_client_config: client_config,
                // oauth2_metadata:
                credentials: credentials.clone(),
            }
        }

        pub fn authenticate(&mut self) -> Result<()> {
            let pub_cfg: ClientConfig = self.public_client_config.clone();
            println!(
                "PublicClientAuthConfig.redirect_uri:\t{}\nPublicClientAuthConfig.client_id:\t{}\nOAuth2Metadata.token_endpoint:\t{}\nOAuth2Metadata.authorization_endpoint:\t{}",
                (pub_cfg.clone()).redirect_uri,
                (pub_cfg.clone()).client_id,
                (pub_cfg.clone()).token_endpoint,
                (pub_cfg.clone()).authorization_endpoint,
            );
            // Create an OAuth2 client (auth0 from Okta) by specifying the client ID, client secret, authorization URL and token URL.
            let client: oauth2::Client<
                oauth2::StandardErrorResponse<oauth2::basic::BasicErrorResponseType>,
                oauth2::StandardTokenResponse<
                    oauth2::EmptyExtraTokenFields,
                    oauth2::basic::BasicTokenType,
                >,
                oauth2::basic::BasicTokenType,
                oauth2::StandardTokenIntrospectionResponse<
                    oauth2::EmptyExtraTokenFields,
                    oauth2::basic::BasicTokenType,
                >,
                StandardRevocableToken,
                oauth2::StandardErrorResponse<oauth2::RevocationErrorResponseType>,
            > = BasicClient::new(
                ClientId::new(
                    (pub_cfg.clone()).client_id,
                ),
                Some(ClientSecret::new(
                    env::var("CLIENT_SECRET")
                        .expect("Missing the CLIENT_SECRET environment variable."),
                )),
                // None,
                AuthUrl::new(
                    (pub_cfg.clone()).authorization_endpoint,
                )
                .unwrap(),
                // Be careful that the `TokenUrl` endpoint in the official documeantion is `<BASE_DOMAIN>/token`.
                // FYR: https://docs.rs/oauth2/latest/oauth2/#example-synchronous-blocking-api
                // In Auth0, it's `<BASE_DOMAIN>/oauth/token`.
                // In Okta, it's `<BASE_DOMAIN>/v1/token`.
                Some(
                    TokenUrl::new(
                        (pub_cfg.clone()).token_endpoint,
                    )
                    .unwrap(),
                ),
            )
            // Set the URL the user will be redirected to after the authorization process.
            .set_redirect_uri(RedirectUrl::new((pub_cfg.clone()).redirect_uri).unwrap());
            // .set_revocation_uri(
            //     RevocationUrl::new(
            //         format!(
            //             "{}/v1/revoke",
            //             env::var("BASE_DOMAIN")
            //                 .expect("Missing the BASE_DOMAIN environment variable.")
            //         )
            //         .to_string(),
            //     )
            //     .expect("Invalid revocation endpoint URL"),
            // );

            // // PKCE flow
            // // Generate a PKCE challenge.
            // let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

            // // Generate the full authorization URL.
            // let (auth_url, csrf_state) = client
            //     .authorize_url(CsrfToken::new_random)
            //     // Set the desired scopes.
            //     .add_scope(Scope::new("all".to_string()))
            //     .add_scope(Scope::new("offline".to_string()))
            //     // Set the PKCE code challenge.
            //     .set_pkce_challenge(pkce_challenge)
            //     .url();

            // // This is the URL you should redirect the user to, in order to trigger the authorization
            // // process.
            // println!("Browse to: {}", auth_url);

            // // Once the user has been redirected to the redirect URL, you'll have access to the
            // // authorization code. For security reasons, your code should verify that the `state`
            // // parameter returned by the server matches `csrf_state`.

            // let (code, state) = {
            //     // A very naive implementation of the redirect server.
            //     // Prepare the callback server in the background that listen to our flyeadmin endpoint.
            //     let listener = TcpListener::bind("localhost:53593").unwrap();

            //     // The server will terminate itself after collecting the first code.
            //     let Some(mut stream) = listener.incoming().flatten().next() else {
            //         panic!("listener terminated without accepting a connection");
            //     };

            //     let mut reader = BufReader::new(&stream);

            //     let mut request_line = String::new();
            //     reader.read_line(&mut request_line).unwrap();

            //     let redirect_url = request_line.split_whitespace().nth(1).unwrap(); // TODO: add error handling
            //     let url = Url::parse(&("http://example.com".to_string() + redirect_url)).unwrap();

            //     let code = url
            //         .query_pairs()
            //         .find(|(key, _)| key == "code")
            //         .map(|(_, code)| AuthorizationCode::new(code.into_owned()))
            //         .unwrap();

            //     let state = url
            //         .query_pairs()
            //         .find(|(key, _)| key == "state")
            //         .map(|(_, state)| CsrfToken::new(state.into_owned()))
            //         .unwrap();

            //     let message = "Go back to your terminal :)";
            //     let response = format!(
            //         "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{}",
            //         message.len(),
            //         message
            //     );
            //     stream.write_all(response.as_bytes()).unwrap();

            //     (code, state)
            // };

            // println!(
            //     "PKCE Authenticator returned the following code:\n{}\n",
            //     code.secret()
            // );
            // println!(
            //     "PKCE Authenticator returned the following state:\n{} (expected `{}`)\n",
            //     state.secret(),
            //     csrf_state.secret()
            // );

            // // Exchange the code with a token.
            // // Now you can trade it for an access token.
            // let token_response = client
            //     .exchange_code(code)
            //     // Send the PKCE code verifier in the token request
            //     .set_pkce_verifier(pkce_verifier)
            //     .request(&http_client)
            //     .unwrap();

            // client credential flow
            
            let token_response = client
                .exchange_client_credentials()
                // .add_scope(Scope::new("okta.myAccount.read".to_string()))
                // .add_scope(Scope::new("all".to_string()))
                // .add_scope(Scope::new("offline".to_string()))
                .request(&http_client)
                .unwrap();

            // let Ok(token_response) = token_result else {
            //     todo!()
            // };
            // let token_to_revoke: StandardRevocableToken = match token_response.refresh_token() {
            //     Some(token) => token.into(),
            //     None => token_response.access_token().into(), // TODO: mitigate ambiguous token
            // };

            // client
            //     .revoke_token(token_to_revoke)
            //     .unwrap()
            //     .request(&http_client)
            //     .expect("Failed to revoke token");

            let access_token = token_response.access_token().secret();

            println!("PKCE Authenticator returned the following token:\n{access_token:?}\n");

            // Should return `access_token` as string for gRPC interceptor.
            // Just like what we did in flytekit remote `auth_interceptor.py`
            // L#36 `auth_metadata = self._authenticator.fetch_grpc_call_auth_metadata()`

            let credentials_for_endpoint = "localhost:30080"; // "dogfood-gcp.cloud-staging.union.ai"; //"flyte-default";
            let credentials_access_token_key = "access_token";
            let entry = Entry::new(credentials_for_endpoint, credentials_access_token_key)?;
            match entry.set_password(access_token) {
                Ok(()) => println!("KeyRing set successfully."),
                Err(err) => println!("KeyRing set not available."),
            };

            Ok(())
        }
    }
}
