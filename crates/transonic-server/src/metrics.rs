//! Prometheus metrics and the pull-based metrics endpoint.

use std::net::SocketAddr;
use std::sync::Arc;

use prometheus::{
    Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};
use tracing::{error, info};

/// Service metrics registered against a Prometheus registry.
pub struct Metrics {
    registry: Registry,
    operations_total: IntCounterVec,
    active_jobs: IntGauge,
    transform_duration: Histogram,
    input_bytes_total: IntCounter,
    output_bytes_total: IntCounter,
    flow_control_events_total: IntCounterVec,
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

        // Per-job wall-clock duration.  Buckets span single-digit
        // milliseconds (near-instant batch transcodes) to tens of seconds
        // (long realtime-paced streams).
        let transform_duration = Histogram::with_opts(
            HistogramOpts::new(
                name("transonic_transform_duration_seconds"),
                "Wall-clock duration of a media transform operation, in seconds.",
            )
            .buckets(vec![
                0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0,
            ]),
        )
        .expect("valid metric definition");

        let input_bytes_total = IntCounter::new(
            name("transonic_input_bytes_total"),
            "Total input (PCM) bytes received from clients.",
        )
        .expect("valid metric definition");

        let output_bytes_total = IntCounter::new(
            name("transonic_output_bytes_total"),
            "Total encoded bytes streamed back to clients.",
        )
        .expect("valid metric definition");

        let flow_control_events_total = IntCounterVec::new(
            Opts::new(
                name("transonic_flow_control_events_total"),
                "Flow-control notifications emitted to clients, by type \
                 (enough_data = slow down, need_data = resume).",
            ),
            &["type"],
        )
        .expect("valid metric definition");

        for collector in [
            Box::new(operations_total.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(active_jobs.clone()),
            Box::new(transform_duration.clone()),
            Box::new(input_bytes_total.clone()),
            Box::new(output_bytes_total.clone()),
            Box::new(flow_control_events_total.clone()),
        ] {
            registry.register(collector).expect("metric registration");
        }

        Arc::new(Metrics {
            registry,
            operations_total,
            active_jobs,
            transform_duration,
            input_bytes_total,
            output_bytes_total,
            flow_control_events_total,
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

    /// Records the wall-clock duration of a finished operation.
    pub fn observe_duration(&self, seconds: f64) {
        self.transform_duration.observe(seconds);
    }

    /// Adds to the input (PCM) byte counter.
    pub fn add_input_bytes(&self, n: u64) {
        self.input_bytes_total.inc_by(n);
    }

    /// Adds to the encoded-output byte counter.
    pub fn add_output_bytes(&self, n: u64) {
        self.output_bytes_total.inc_by(n);
    }

    /// Records a flow-control notification of the given type.
    pub fn record_flow_control(&self, kind: &str) {
        self.flow_control_events_total
            .with_label_values(&[kind])
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
