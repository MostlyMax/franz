use axum::{
    routing::{any, get},
    Router,
};
use clap::Parser;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::{select, signal};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

mod config;
use config::*;

mod server;
use server::*;

mod handshake;
use handshake::*;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: PathBuf,
}

async fn shutdown_signal(shutdown: CancellationToken) {
    let ctrlc = async {
        if signal::ctrl_c().await.is_err() {
            error!("failed to capture ctrl-c signal");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    select! {
        _ = ctrlc => {},
        _ = terminate => {},
    };

    shutdown.cancel();
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let config = parse_config(args.config).unwrap();

    debug!(?config);

    let shutdown = CancellationToken::new();
    let addr = SocketAddr::new(config.ip, config.port);

    let state = Arc::new(AppState::new(shutdown.clone(), config).await);
    let state_c = state.clone();

    //let app = Router::new()
    //    .route("/ping", get(|| async { "pong" }))
    //    .route("/consume/{topic}/{group}", any(consume))
    //    .route("/produce/{topic}", any(produce))
    //    .with_state(state_c);
    //
    let listener = tokio::net::TcpListener::bind(addr).await?;

    serve(listener, state.clone()).await.unwrap();

    //
    //axum::serve(listener, app)
    //    .with_graceful_shutdown(shutdown_signal(shutdown))
    //    .await?;
    //
    state.cleanup_with_timeout(Duration::from_secs(10)).await;

    Ok(())
}
