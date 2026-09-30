//! Standalone Serenity Gateway host for the playback worker.
//!
//! Run this binary as its own long-lived process, separate from the command bot. The
//! worker owns this bot's Gateway shard session; the command bot must not open another
//! session for the same shard and should use the worker's authenticated HTTP API instead.
//! Terminating this worker also terminates its Gateway session and interrupts playback.
//!
//! Required environment: `DISCORD_TOKEN`, `DISCORD_BOT_USER_ID`, `LAVALINK_HOST`
//! (`host:port`), `LAVALINK_PASSWORD`, and `WORKER_API_TOKEN` (at least 32 bytes).
//! Optional: `WORKER_BIND` (defaults to `127.0.0.1:8080`; non-loopback binds are rejected).
//!
//! Example: `cargo run -p straight-rs-worker --features serenity --example worker-serenity`

#[allow(dead_code)]
#[path = "queue-plugin.rs"]
mod queue_plugin;

use std::{env, net::SocketAddr, process::ExitCode};
use straight_rs::NodeConfig;
use straight_rs_model::UserId;
use straight_rs_worker::{
    SecretString, WorkerBuilder, WorkerConfigBuilder, adapters::serenity::SerenityGatewayDriver,
};
use tokio::sync::watch;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(()) => {
            // Avoid displaying error chains that could contain values from configuration.
            eprintln!("worker failed; error details suppressed to protect configuration values");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), ()> {
    let discord_token = required_env("DISCORD_TOKEN")?;
    let bot_user_id = parse_u64_env("DISCORD_BOT_USER_ID")?;
    let lavalink_host = required_env("LAVALINK_HOST")?;
    let lavalink_password = required_env("LAVALINK_PASSWORD")?;
    let api_token = required_env("WORKER_API_TOKEN")?;
    let bind_addr = optional_bind_addr()?;

    let config = WorkerConfigBuilder::new(
        UserId(bot_user_id),
        SecretString::new(discord_token),
        SecretString::new(api_token),
        vec![NodeConfig::new(lavalink_host, lavalink_password)],
    )
    .bind_addr(bind_addr, false)
    .build()
    .map_err(|_| ())?;

    // Queue continuation is an explicitly registered, trusted in-process example plugin.
    let mut worker = WorkerBuilder::new(config, SerenityGatewayDriver::new())
        .plugin(queue_plugin::QueuePlugin::new())
        .build()
        .await
        .map_err(|_| ())?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signal_task = tokio::spawn(async move {
        tokio::signal::ctrl_c().await.map_err(|_| ())?;
        let _ = shutdown_tx.send(true);
        Ok::<(), ()>(())
    });

    let serve_result = worker.serve(shutdown_rx).await;
    signal_task.abort();
    if serve_result.is_err() {
        // `serve` only calls shutdown after the listener has started successfully.
        let _ = worker.shutdown().await;
        return Err(());
    }
    Ok(())
}

fn required_env(name: &str) -> Result<String, ()> {
    match env::var(name) {
        Ok(value) if !value.is_empty() => Ok(value),
        _ => {
            eprintln!("required environment variable {name} is missing or empty");
            Err(())
        }
    }
}

fn parse_u64_env(name: &str) -> Result<u64, ()> {
    let value = required_env(name)?;
    value.parse::<u64>().map_err(|_| {
        eprintln!("environment variable {name} must be an unsigned 64-bit integer");
    })
}

fn optional_bind_addr() -> Result<SocketAddr, ()> {
    match env::var("WORKER_BIND") {
        Ok(value) => value.parse::<SocketAddr>().map_err(|_| {
            eprintln!("environment variable WORKER_BIND must be a socket address");
        }),
        Err(env::VarError::NotPresent) => Ok("127.0.0.1:8080".parse().expect("valid default bind")),
        Err(env::VarError::NotUnicode(_)) => {
            eprintln!("environment variable WORKER_BIND is not valid Unicode");
            Err(())
        }
    }
}
