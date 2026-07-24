//! gRPC health checking service.
//!
//! The health service monitors the transform service through a moving
//! average filter: a 0 result is recorded for each expected transform
//! outcome and a 1 result for each unexpected outcome.  When the moving
//! average rises above the configured threshold the service reports
//! NOT_SERVING until it recovers.  While the server is shutting down the
//! service always reports NOT_SERVING.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tonic::{Request, Response, Status};

use transonic_proto::health::v1 as health_proto;

/// Fixed-window moving average over binary samples.
pub struct MovingAverageFilter {
    samples: Vec<u8>,
    index: usize,
    total: u32,
}

impl MovingAverageFilter {
    /// Creates a filter over `window_size` samples (initially all zero).
    pub fn new(window_size: usize) -> Self {
        MovingAverageFilter {
            samples: vec![0; window_size.max(1)],
            index: 0,
            total: 0,
        }
    }

    /// Pushes a sample and returns the updated average.
    pub fn push(&mut self, sample: u8) -> f32 {
        let size = self.samples.len();
        let oldest = &mut self.samples[self.index];
        self.total = self.total + u32::from(sample) - u32::from(*oldest);
        *oldest = sample;
        self.index = (self.index + 1) % size;
        self.total as f32 / size as f32
    }
}

/// Tracks transform outcomes and answers health checks.
pub struct HealthTracker {
    filter: Mutex<MovingAverageFilter>,
    last_result: Mutex<f32>,
    unhealthy_threshold: f32,
    shutting_down: Arc<AtomicBool>,
}

impl HealthTracker {
    /// Creates a tracker with the given filter window and threshold.
    pub fn new(
        window_size: usize,
        unhealthy_threshold: f32,
        shutting_down: Arc<AtomicBool>,
    ) -> Self {
        HealthTracker {
            filter: Mutex::new(MovingAverageFilter::new(window_size)),
            last_result: Mutex::new(0.0),
            unhealthy_threshold,
            shutting_down,
        }
    }

    /// Records a finished transform operation by gRPC status code.
    pub fn record_finished(&self, code: tonic::Code) {
        let sample = match code {
            tonic::Code::Ok | tonic::Code::InvalidArgument | tonic::Code::NotFound => 0,
            _ => 1,
        };
        self.push(sample);
    }

    /// Records a cancelled transform operation.
    pub fn record_cancelled(&self) {
        self.push(0);
    }

    fn push(&self, sample: u8) {
        let result = self.filter.lock().unwrap().push(sample);
        *self.last_result.lock().unwrap() = result;
    }

    fn is_unhealthy(&self) -> bool {
        *self.last_result.lock().unwrap() >= self.unhealthy_threshold
    }
}

/// The `grpc.health.v1.Health` service implementation.
pub struct HealthService {
    tracker: Arc<HealthTracker>,
}

impl HealthService {
    /// Creates the service around a shared tracker.
    pub fn new(tracker: Arc<HealthTracker>) -> Self {
        HealthService { tracker }
    }
}

#[tonic::async_trait]
impl health_proto::health_server::Health for HealthService {
    async fn check(
        &self,
        request: Request<health_proto::HealthCheckRequest>,
    ) -> Result<Response<health_proto::HealthCheckResponse>, Status> {
        use health_proto::health_check_response::ServingStatus;

        let respond = |status: ServingStatus| {
            Ok(Response::new(health_proto::HealthCheckResponse {
                status: status.into(),
            }))
        };

        if self.tracker.shutting_down.load(Ordering::SeqCst) {
            return respond(ServingStatus::NotServing);
        }

        let service_name = &request.get_ref().service;
        if !(service_name.is_empty() || service_name == "transonic.v1.TransformService") {
            return Err(Status::not_found(
                "the given service name is not associated with a service running on \
                 this server",
            ));
        }

        if self.tracker.is_unhealthy() {
            respond(ServingStatus::NotServing)
        } else {
            respond(ServingStatus::Serving)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn moving_average_tracks_window() {
        let mut filter = MovingAverageFilter::new(4);
        assert_eq!(filter.push(1), 0.25);
        assert_eq!(filter.push(1), 0.5);
        assert_eq!(filter.push(0), 0.5);
        assert_eq!(filter.push(0), 0.5);
        // The first `1` falls out of the window here.
        assert_eq!(filter.push(0), 0.25);
        assert_eq!(filter.push(0), 0.0);
    }
}
