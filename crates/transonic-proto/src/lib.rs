//! Generated protobuf/gRPC types for the `transonic.v1` API surface and the
//! standard `grpc.health.v1` health checking protocol.

/// Types generated from the `transonic.v1` protobuf package.
pub mod v1 {
    tonic::include_proto!("transonic.v1");
}

/// Types generated from the `grpc.health.v1` protobuf package.
pub mod health {
    pub mod v1 {
        tonic::include_proto!("grpc.health.v1");
    }
}
