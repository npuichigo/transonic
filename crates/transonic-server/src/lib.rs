//! Library surface of the Transonic server: the gRPC service, health
//! checking and metrics.  Exposed as a library so integration tests (and
//! embedders) can assemble the same services the `transonic` binary runs.

pub mod grpc;
pub mod health;
pub mod metrics;
