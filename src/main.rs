use tokio::sync::Semaphore;
use std::sync::Arc;
use actix_web::{
    middleware, 
    web, 
    App, 
    HttpServer
};
use yaw_dbproxy::app_config::{Config, load_openssl_config};//load_rustls_config};
use yaw_dbproxy::common::{ScyllaService, AppState};
use tracing::{info};
use yaw_dbproxy::service::{execute_statement, direct_statement, health_check};

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    let config = Config::from_env().expect("Server configuration");

    let port = config.port;
    let host = config.host.clone();
    let num_cpus = num_cpus::get();
    let parallel_files = config.parallel_files;
    let db_parallelism = config.db_parallelism;
    let region = config.region;
    let payload_max_size = config.payload_max_size;

    info!(
        "Starting application. Num CPUs {}. Max Parallel Files {}. DB Parallelism {}.  Region {}",
        num_cpus, parallel_files, db_parallelism, region
    );

    let db = ScyllaService::new(
        config.db_node0,
        config.db_node1,
        config.db_node2,
        config.db_user,
        config.db_password,
        config.db_parallelism,
        config.db_dc
    ).await;

    let sem = Arc::new(Semaphore::new(parallel_files));
    let data = web::Data::new(AppState {
        db_svc: db,
        semaphore: sem,
        region
    });

    info!("Starting server at http://{}:{}/", host, port);

    // let ssl_config = load_rustls_config();
    let ssl_config = load_openssl_config();

    HttpServer::new(move || {

        App::new()
            .wrap(middleware::Logger::default())
            .app_data(web::JsonConfig::default().limit(payload_max_size))
            .app_data(data.clone())
            .route("/", web::get().to(health_check::index))
            .route("/v2/", web::get().to(health_check::index))
            .route("/v2/execute_statement", web::post().to(execute_statement::index))
            .route("/v2/direct_statement", web::post().to(direct_statement::index))
    })
    .bind_openssl(format!("{}:{}", host, port), ssl_config)?
    //.bind_rustls(format!("{}:{}", host, port), ssl_config)?
    //.bind(format!("{}:{}", host, port))?
    .workers(num_cpus * 2)
    .run()
    .await?;

    Ok(())
}