//! Prometheus metrics and the pull-based metrics endpoint.

use std::net::SocketAddr;
use std::sync::Arc;

use prometheus::{IntCounterVec, IntGauge, Opts, Registry, TextEncoder};
use tracing::{error, info};

/// Service metrics registered against a Prometheus registry.
pub struct Metrics {
    registry: Registry,
    operations_total: IntCounterVec,
    active_jobs: IntGauge,
}

impl Metrics {
    /// Creates the metric set.  `prefix` is prepended to every metric name
    /// (controlled by `PROMETHEUS_METRICS_PREFIX`).
    pub fn new(prefix: Option<&str>) -> Arc<Self> {
        let name = |base: &str| match prefix {
            Some(prefix) if !prefix.is_empty() => format!("{prefix}_{base}"),
            _ => base.to_string(),
        };

        let registry = Registry::new();
        let operations_total = IntCounterVec::new(
            Opts::new(
                name("transonic_transform_operations_total"),
                "Total number of finished media transform operations by gRPC status.",
            ),
            &["status"],
        )
        .expect("valid metric definition");
        let active_jobs = IntGauge::new(
            name("transonic_active_jobs"),
            "Number of media transform jobs currently being processed.",
        )
        .expect("valid metric definition");

        registry
            .register(Box::new(operations_total.clone()))
            .expect("metric registration");
        registry
            .register(Box::new(active_jobs.clone()))
            .expect("metric registration");

        Arc::new(Metrics {
            registry,
            operations_total,
            active_jobs,
        })
    }

    /// Marks a job as started.
    pub fn job_started(&self) {
        self.active_jobs.inc();
    }

    /// Marks a job as finished (successfully or not).
    pub fn job_finished(&self) {
        self.active_jobs.dec();
    }

    /// Records a finished operation outcome.
    pub fn record_outcome(&self, code: tonic::Code) {
        self.operations_total
            .with_label_values(&[&format!("{code:?}")])
            .inc();
    }

    /// Records a cancelled operation.
    pub fn record_cancelled(&self) {
        self.operations_total
            .with_label_values(&["Cancelled"])
            .inc();
    }

    fn render(&self) -> String {
        TextEncoder::new()
            .encode_to_string(&self.registry.gather())
            .unwrap_or_default()
    }
}

/// Serves the Prometheus pull endpoint (`/metrics`) on the given address.
pub async fn serve(metrics: Arc<Metrics>, address: SocketAddr) {
    let app = axum::Router::new().route(
        "/metrics",
        axum::routing::get(move || {
            let metrics = Arc::clone(&metrics);
            async move { metrics.render() }
        }),
    );

    match tokio::net::TcpListener::bind(address).await {
        Ok(listener) => {
            info!(address = %address, "prometheus endpoint listening");
            if let Err(e) = axum::serve(listener, app).await {
                error!(error = %e, "prometheus endpoint failed");
            }
        }
        Err(e) => error!(address = %address, error = %e, "failed to bind prometheus endpoint"),
    }
}
