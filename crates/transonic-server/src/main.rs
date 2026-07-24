//! Transonic transform service.
//!
//! A gRPC bidirectional-streaming service that runs GStreamer media
//! transformation pipelines as a service.

use transonic_server::{grpc, health, metrics};

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tracing::{error, info};

use transonic_core::config::RootConfig;
use transonic_core::Processor;
use transonic_proto::health::v1::health_server::HealthServer;
use transonic_proto::v1::transform_service_server::TransformServiceServer;

/// Command line options.
#[derive(Debug, Parser)]
#[command(name = "transonic", about = "Transonic transform service")]
struct Options {
    /// The JSON configuration file.
    #[arg(short = 'c', env = "TRANSONIC_CONFIG_PATH")]
    config: PathBuf,

    /// The log level threshold (one of: trace, debug, info, warn, err,
    /// critical).
    #[arg(short = 'd', env = "TRANSONIC_LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// The directory used for temporary storage.
    #[arg(short = 't', env = "TRANSONIC_TEMP_STORAGE_DIRECTORY")]
    temp_dir: Option<PathBuf>,

    /// Unused legacy option (file sink directory); accepted for
    /// compatibility.
    #[arg(short = 's', hide = true)]
    _file_sink_dir: Option<PathBuf>,

    /// The number of seconds to wait after receiving a SIGTERM/SIGINT signal
    /// before shutting down.
    #[arg(short = 'w', env = "TRANSONIC_SHUTDOWN_DELAY_SEC", default_value_t = 0)]
    shutdown_delay: u32,

    /// A gRPC endpoint string designating where to accept connections.
    #[arg(env = "TRANSONIC_ENDPOINT")]
    endpoint: String,

    /// The endpoint used to expose Prometheus metric data.
    #[arg(long, env = "PROMETHEUS_PORT", default_value = "0.0.0.0:8080")]
    prometheus_endpoint: String,

    /// The Prometheus metrics prefix.
    #[arg(long, env = "PROMETHEUS_METRICS_PREFIX")]
    prometheus_metrics_prefix: Option<String>,
}

fn init_logging(level: &str) -> Result<(), String> {
    use tracing_subscriber::filter::LevelFilter;

    let (tracing_level, gst_level) = match level {
        "trace" => (LevelFilter::TRACE, gstreamer::DebugLevel::Trace),
        "debug" => (LevelFilter::DEBUG, gstreamer::DebugLevel::Info),
        "info" => (LevelFilter::INFO, gstreamer::DebugLevel::Fixme),
        "warn" => (LevelFilter::WARN, gstreamer::DebugLevel::Fixme),
        "err" => (LevelFilter::ERROR, gstreamer::DebugLevel::Error),
        "critical" => (LevelFilter::ERROR, gstreamer::DebugLevel::None),
        _ => return Err(format!("'{level}': invalid log level")),
    };

    tracing_subscriber::fmt()
        .with_max_level(tracing_level)
        .with_writer(std::io::stderr)
        .init();

    gstreamer::log::set_default_threshold(gst_level);
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let options = Options::parse();

    if let Err(message) = init_logging(&options.log_level) {
        eprintln!("option error: {message}");
        return std::process::ExitCode::FAILURE;
    }

    match run(options).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(options: Options) -> Result<(), String> {
    // Initialize GStreamer (the config parser may reference element
    // factories) and register the custom elements.
    transonic_core::init().map_err(|e| format!("failed to initialize gstreamer: {e}"))?;
    transonic_plugins::plugin_register_static()
        .map_err(|e| format!("failed to register custom elements: {e}"))?;

    let root_config =
        RootConfig::load(&options.config).map_err(|e| format!("config error: {e}"))?;

    let temp_dir = match &options.temp_dir {
        Some(path) => {
            if !path.is_dir() {
                return Err(format!(
                    "option error: '{}': invalid temp directory path",
                    path.display()
                ));
            }
            path.clone()
        }
        None => std::env::temp_dir(),
    };

    let endpoint: SocketAddr = options.endpoint.parse().map_err(|e| {
        format!(
            "option error: '{}': invalid endpoint: {e}",
            options.endpoint
        )
    })?;
    let prometheus_endpoint: SocketAddr = options.prometheus_endpoint.parse().map_err(|e| {
        format!(
            "option error: '{}': invalid prometheus endpoint: {e}",
            options.prometheus_endpoint
        )
    })?;

    // Shared shutdown flag: transform streams send a SHUTTING_DOWN
    // notification and the health service reports NOT_SERVING once set.
    let shutting_down = Arc::new(AtomicBool::new(false));

    info!("initializing prometheus client");
    let metrics = metrics::Metrics::new(options.prometheus_metrics_prefix.as_deref());
    tokio::spawn(metrics::serve(Arc::clone(&metrics), prometheus_endpoint));

    info!("initializing grpc server");
    let health_config = root_config.health;
    let processor = Arc::new(Processor::new(root_config, temp_dir));
    let health_tracker = Arc::new(health::HealthTracker::new(
        health_config.filter_window_size as usize,
        health_config.unhealthy_threshold,
        Arc::clone(&shutting_down),
    ));
    let transform_service = grpc::TransformService::new(
        Arc::clone(&processor),
        Arc::clone(&shutting_down),
        Arc::clone(&health_tracker),
        Arc::clone(&metrics),
    );
    let health_service = health::HealthService::new(Arc::clone(&health_tracker));

    info!("setting up asynchronous signal processing");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let shutdown_delay = Duration::from_secs(u64::from(options.shutdown_delay));
    {
        let shutting_down = Arc::clone(&shutting_down);
        tokio::spawn(async move {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to install SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => info!("caught SIGINT"),
                _ = sigterm.recv() => info!("caught SIGTERM"),
            }
            info!(
                grace_period = shutdown_delay.as_secs(),
                "shutting down server"
            );
            shutting_down.store(true, Ordering::SeqCst);
            let _ = shutdown_tx.send(());

            // Force process termination after the grace period, mirroring
            // the original's shutdown deadline behavior.
            tokio::time::sleep(shutdown_delay).await;
            error!("shutdown grace period expired, terminating");
            std::process::exit(0);
        });
    }

    info!(endpoint = %endpoint, "entering I/O loop");
    tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(2 * 60)))
        .http2_keepalive_timeout(Some(Duration::from_secs(60)))
        .add_service(TransformServiceServer::new(transform_service))
        .add_service(HealthServer::new(health_service))
        .serve_with_shutdown(endpoint, async {
            let _ = shutdown_rx.await;
        })
        .await
        .map_err(|e| format!("grpc server error: {e}"))?;

    info!("tearing down application");
    Ok(())
}
