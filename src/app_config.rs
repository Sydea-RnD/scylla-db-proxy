use serde::Deserialize;
use envy;
use color_eyre::Result;
use tracing::{info};
use tracing_subscriber::EnvFilter;

// use std::{fs::File, io::BufReader};
// use rustls::{Certificate, PrivateKey, ServerConfig};
// use rustls_pemfile::{certs, pkcs8_private_keys};

use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod, SslAcceptorBuilder, SslVerifyMode, SslContextBuilder};

#[derive(Debug, Deserialize, Clone, Default)]
pub struct Config {
    pub host: String,
    pub port: i32,
    pub region: String,
    pub rust_log: String,
    pub db_node0: String,
    pub db_node1: String,
    pub db_node2: String,
    pub db_user: String,
    pub db_password: String,
    pub db_dc: String,
    pub parallel_files: usize,
    pub db_parallelism: usize,
    pub payload_max_size: usize,
}

fn init_tracer() {
    #[cfg(debug_assertions)]
    let tracer = tracing_subscriber::fmt();
    #[cfg(not(debug_assertions))]
    let tracer = tracing_subscriber::fmt().json();
    tracer.with_env_filter(EnvFilter::from_default_env()).init();
}

impl Config {

    pub fn from_env() -> Result<Config> {

        init_tracer();
        info!("Loading configuration");
        let ret_env = match envy::from_env::<Config>() {
            Ok(environment) => environment,
            Err(_e) => return Ok(Config::default()),
        };
        Ok(ret_env)

    }
}

// pub fn load_rustls_config() -> rustls::ServerConfig {

//     let config = ServerConfig::builder()
//         .with_safe_defaults()
//         .with_no_client_auth();

//     let cert_file = &mut BufReader::new(File::open("cert.pem").unwrap());
//     let key_file = &mut BufReader::new(File::open("key.pem").unwrap());

//     let cert_chain = certs(cert_file)
//         .unwrap()
//         .into_iter()
//         .map(Certificate)
//         .collect();
//     let mut keys: Vec<PrivateKey> = pkcs8_private_keys(key_file)
//         .unwrap()
//         .into_iter()
//         .map(PrivateKey)
//         .collect();

//     if keys.is_empty() {
//         eprintln!("Could not locate PKCS 8 private keys.");
//         std::process::exit(1);
//     }

//     config.with_single_cert(cert_chain, keys.remove(0)).unwrap()
// }

pub fn load_openssl_config() -> SslAcceptorBuilder {

    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    builder
        .set_private_key_file("key.pem", SslFiletype::PEM)
        .unwrap();
    builder.set_certificate_chain_file("cert.pem").unwrap();

    builder
}

pub fn load_ssl_scylla_config() -> Result<SslContextBuilder, anyhow::Error> {

    let mut context_builder = SslContextBuilder::new(SslMethod::tls())?;
    context_builder.set_ca_file("scylla_cert.crt")?;
    context_builder.set_verify(SslVerifyMode::PEER);

    Ok(context_builder)
}

// use scylla::{Session, SessionBuilder};
// use openssl::ssl::{SslContextBuilder, SslMethod, SslVerifyMode};
// use std::path::PathBuf;

// let mut context_builder = SslContextBuilder::new(SslMethod::tls())?;
// context_builder.set_ca_file("ca.crt")?;
// context_builder.set_verify(SslVerifyMode::PEER);

// let session: Session = SessionBuilder::new()
//     .known_node("127.0.0.1:9142") // The the port is now 9142
//     .ssl_context(Some(context_builder.build()))
//     .build()
//     .await?;