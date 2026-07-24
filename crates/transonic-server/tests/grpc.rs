//! End-to-end gRPC tests, mirroring the scope of the original
//! `test/grpc.cpp` suite: a real server is started on an ephemeral port and
//! exercised through a real tonic client.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Once};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use transonic_proto::health::v1 as health_proto;
use transonic_proto::v1 as proto;

const CONFIG: &str = r#"{
    "serviceParameters": [
        { "default": { "readTimeoutMilliseconds": 5000 } }
    ],
    "pipelines": {
        "passthrough": {
            "input": { "in": { "specs": "queue ! c." } },
            "output": { "out": { "specs": "concat name=c" } }
        }
    }
}"#;

fn init() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        transonic_core::init().expect("gstreamer initialization");
        transonic_plugins::plugin_register_static().expect("plugin registration");
    });
}

/// Starts a server on an ephemeral port and returns its address.
async fn start_server() -> String {
    init();

    let config = transonic_core::RootConfig::load_str(CONFIG).expect("test config");
    let health_config = config.health;
    let shutting_down = Arc::new(AtomicBool::new(false));
    let processor = Arc::new(transonic_core::Processor::new(config, std::env::temp_dir()));
    let health_tracker = Arc::new(transonic_server::health::HealthTracker::new(
        health_config.filter_window_size as usize,
        health_config.unhealthy_threshold,
        Arc::clone(&shutting_down),
    ));
    let metrics = transonic_server::metrics::Metrics::new(None);

    let transform_service = transonic_server::grpc::TransformService::new(
        Arc::clone(&processor),
        Arc::clone(&shutting_down),
        Arc::clone(&health_tracker),
        metrics,
    );
    let health_service = transonic_server::health::HealthService::new(health_tracker);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let address = listener.local_addr().expect("local address");

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                proto::transform_service_server::TransformServiceServer::new(transform_service),
            )
            .add_service(health_proto::health_server::HealthServer::new(
                health_service,
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("server runs");
    });

    format!("http://{address}")
}

async fn connect(
    address: &str,
) -> proto::transform_service_client::TransformServiceClient<tonic::transport::Channel> {
    proto::transform_service_client::TransformServiceClient::connect(address.to_string())
        .await
        .expect("client connects")
}

fn passthrough_config() -> proto::TransformConfig {
    proto::TransformConfig {
        pipeline_name: "passthrough".to_string(),
        inputs: vec![proto::ChannelSpecifier {
            channel_id: "I1".to_string(),
            pipeline_bin_id: "in".to_string(),
            pipeline_parameters: Default::default(),
            metadata: Default::default(),
        }],
        outputs: vec![proto::ChannelSpecifier {
            channel_id: "O1".to_string(),
            pipeline_bin_id: "out".to_string(),
            pipeline_parameters: Default::default(),
            metadata: Default::default(),
        }],
        parameter_overrides: vec![],
    }
}

struct TransformResult {
    output: Vec<u8>,
    completed: Option<proto::TransformCompleted>,
    status: Result<(), tonic::Status>,
}

async fn run_transform(address: &str, requests: Vec<proto::TransformRequest>) -> TransformResult {
    let mut client = connect(address).await;

    let (tx, rx) = mpsc::channel(16);
    let sender = tokio::spawn(async move {
        for request in requests {
            if tx.send(request).await.is_err() {
                break;
            }
        }
    });

    let mut output = Vec::new();
    let mut completed = None;
    let status = match client.transform(ReceiverStream::new(rx)).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            loop {
                match stream.message().await {
                    Ok(Some(response)) => match response.response {
                        Some(proto::transform_response::Response::Payload(payload)) => {
                            for chunk in &payload.chunk_list {
                                output.extend_from_slice(chunk);
                            }
                        }
                        Some(proto::transform_response::Response::TransformCompleted(done)) => {
                            completed = Some(done);
                        }
                        _ => {}
                    },
                    Ok(None) => break Ok(()),
                    Err(status) => break Err(status),
                }
            }
        }
        Err(status) => Err(status),
    };
    sender.await.expect("sender task");

    TransformResult {
        output,
        completed,
        status,
    }
}

fn config_request(config: proto::TransformConfig) -> proto::TransformRequest {
    proto::TransformRequest {
        channel_id: String::new(),
        request: Some(proto::transform_request::Request::Config(config)),
    }
}

fn payload_request(channel: &str, data: &[u8]) -> proto::TransformRequest {
    proto::TransformRequest {
        channel_id: channel.to_string(),
        request: Some(proto::transform_request::Request::Payload(
            proto::InputChannelPayload {
                chunk_list: vec![Bytes::copy_from_slice(data)],
            },
        )),
    }
}

fn completed_request(channel: &str) -> proto::TransformRequest {
    proto::TransformRequest {
        channel_id: channel.to_string(),
        request: Some(proto::transform_request::Request::ChannelCompleted(
            proto::ChannelCompleted {},
        )),
    }
}

///////////////////////////////////////////////////////////////////////////
// tests
///////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn transform_passthrough_end_to_end() {
    let address = start_server().await;
    let data: Vec<u8> = (0..20_000u32).map(|i| (i % 249) as u8).collect();

    let result = run_transform(
        &address,
        vec![
            config_request(passthrough_config()),
            payload_request("I1", &data),
            completed_request("I1"),
        ],
    )
    .await;

    assert!(result.status.is_ok(), "status: {:?}", result.status);
    assert_eq!(result.output, data);
    let completed = result.completed.expect("termination response");
    assert_eq!(
        completed.termination_reason(),
        proto::TerminationReason::EndOfStream
    );
}

#[tokio::test]
async fn first_message_must_be_config() {
    let address = start_server().await;
    let result = run_transform(&address, vec![payload_request("I1", &[0u8; 16])]).await;

    let status = result.status.expect_err("expected error status");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let completed = result.completed.expect("termination response");
    assert_eq!(
        completed.termination_reason(),
        proto::TerminationReason::InvalidArgument
    );
}

#[tokio::test]
async fn unknown_pipeline_is_not_found() {
    let address = start_server().await;
    let mut config = passthrough_config();
    config.pipeline_name = "unknown".to_string();

    let result = run_transform(&address, vec![config_request(config)]).await;

    let status = result.status.expect_err("expected error status");
    assert_eq!(status.code(), tonic::Code::NotFound);
    let completed = result.completed.expect("termination response");
    assert_eq!(
        completed.termination_reason(),
        proto::TerminationReason::InvalidArgument
    );
}

#[tokio::test]
async fn config_must_reference_inputs_and_outputs() {
    let address = start_server().await;
    let mut config = passthrough_config();
    config.inputs.clear();

    let result = run_transform(&address, vec![config_request(config)]).await;

    let status = result.status.expect_err("expected error status");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn duplicate_parameter_overrides_are_rejected() {
    let address = start_server().await;
    let mut config = passthrough_config();
    config.parameter_overrides = vec![
        proto::ServiceParameter {
            value: Some(proto::service_parameter::Value::Rate(1.0)),
        },
        proto::ServiceParameter {
            value: Some(proto::service_parameter::Value::Rate(2.0)),
        },
    ];

    let result = run_transform(&address, vec![config_request(config)]).await;

    let status = result.status.expect_err("expected error status");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn second_config_message_is_rejected() {
    let address = start_server().await;
    let result = run_transform(
        &address,
        vec![
            config_request(passthrough_config()),
            config_request(passthrough_config()),
        ],
    )
    .await;

    let status = result.status.expect_err("expected error status");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn health_check_reports_serving() {
    let address = start_server().await;
    let mut client = health_proto::health_client::HealthClient::connect(address.clone())
        .await
        .expect("health client connects");

    let response = client
        .check(health_proto::HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("health check succeeds");
    assert_eq!(
        response.get_ref().status(),
        health_proto::health_check_response::ServingStatus::Serving
    );

    let response = client
        .check(health_proto::HealthCheckRequest {
            service: "transonic.v1.TransformService".to_string(),
        })
        .await
        .expect("health check succeeds");
    assert_eq!(
        response.get_ref().status(),
        health_proto::health_check_response::ServingStatus::Serving
    );
}

#[tokio::test]
async fn health_check_rejects_unknown_service() {
    let address = start_server().await;
    let mut client = health_proto::health_client::HealthClient::connect(address)
        .await
        .expect("health client connects");

    let status = client
        .check(health_proto::HealthCheckRequest {
            service: "unknown.Service".to_string(),
        })
        .await
        .expect_err("expected error status");
    assert_eq!(status.code(), tonic::Code::NotFound);
}

///////////////////////////////////////////////////////////////////////////
// additional coverage (parity with the original grpc suite)
///////////////////////////////////////////////////////////////////////////

/// Starts a server with a custom config; returns the address and the shared
/// shutting-down flag.
async fn start_server_with(config_json: &str) -> (String, Arc<AtomicBool>) {
    init();

    let config = transonic_core::RootConfig::load_str(config_json).expect("test config");
    let health_config = config.health;
    let shutting_down = Arc::new(AtomicBool::new(false));
    let processor = Arc::new(transonic_core::Processor::new(config, std::env::temp_dir()));
    let health_tracker = Arc::new(transonic_server::health::HealthTracker::new(
        health_config.filter_window_size as usize,
        health_config.unhealthy_threshold,
        Arc::clone(&shutting_down),
    ));
    let metrics = transonic_server::metrics::Metrics::new(None);
    let transform_service = transonic_server::grpc::TransformService::new(
        Arc::clone(&processor),
        Arc::clone(&shutting_down),
        Arc::clone(&health_tracker),
        metrics,
    );
    let health_service = transonic_server::health::HealthService::new(health_tracker);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let address = listener.local_addr().expect("local address");

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                proto::transform_service_server::TransformServiceServer::new(transform_service),
            )
            .add_service(health_proto::health_server::HealthServer::new(
                health_service,
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("server runs");
    });

    (format!("http://{address}"), shutting_down)
}

/// Runs a transform keeping the request stream open after the scripted
/// messages (for timeout tests), with optional request metadata.
async fn run_transform_holding_stream(
    address: &str,
    requests: Vec<proto::TransformRequest>,
    metadata: Option<(&'static str, &str)>,
) -> TransformResult {
    let mut client = connect(address).await;

    // The sender half is intentionally kept alive so the request stream
    // stays open until the server terminates the call.
    let (tx, rx) = mpsc::channel::<proto::TransformRequest>(16);
    for request in requests {
        tx.send(request).await.expect("request queued");
    }

    let mut request = tonic::Request::new(ReceiverStream::new(rx));
    if let Some((key, value)) = metadata {
        request
            .metadata_mut()
            .insert(key, value.parse().expect("valid metadata value"));
    }

    let mut output = Vec::new();
    let mut completed = None;
    let status = match client.transform(request).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            loop {
                match stream.message().await {
                    Ok(Some(response)) => match response.response {
                        Some(proto::transform_response::Response::Payload(payload)) => {
                            for chunk in &payload.chunk_list {
                                output.extend_from_slice(chunk);
                            }
                        }
                        Some(proto::transform_response::Response::TransformCompleted(done)) => {
                            completed = Some(done);
                        }
                        _ => {}
                    },
                    Ok(None) => break Ok(()),
                    Err(status) => break Err(status),
                }
            }
        }
        Err(status) => Err(status),
    };
    drop(tx);

    TransformResult {
        output,
        completed,
        status,
    }
}

const SHORT_TIMEOUT_CONFIG: &str = r#"{
    "serviceParameters": [
        { "default": { "readTimeoutMilliseconds": 200 } }
    ],
    "pipelines": {
        "passthrough": {
            "input": { "in": { "specs": "queue ! c." } },
            "output": { "out": { "specs": "concat name=c" } }
        }
    }
}"#;

#[tokio::test]
async fn config_read_timeout_is_enforced() {
    let (address, _) = start_server_with(SHORT_TIMEOUT_CONFIG).await;
    // No config message at all: the server must give up after the default
    // read timeout.
    let result = run_transform_holding_stream(&address, vec![], None).await;

    let status = result.status.expect_err("expected error status");
    assert_eq!(status.code(), tonic::Code::Unknown);
    let completed = result.completed.expect("termination response");
    assert_eq!(
        completed.termination_reason(),
        proto::TerminationReason::ReadTimeout
    );
}

#[tokio::test]
async fn post_config_read_timeout_is_enforced() {
    let (address, _) = start_server_with(SHORT_TIMEOUT_CONFIG).await;
    let result =
        run_transform_holding_stream(&address, vec![config_request(passthrough_config())], None)
            .await;

    let status = result.status.expect_err("expected error status");
    assert_eq!(status.code(), tonic::Code::Unknown);
    let completed = result.completed.expect("termination response");
    assert_eq!(
        completed.termination_reason(),
        proto::TerminationReason::ReadTimeout
    );
}

#[tokio::test]
async fn invalid_job_id_metadata_is_rejected() {
    let address = start_server().await;
    let result = run_transform_holding_stream(
        &address,
        vec![config_request(passthrough_config())],
        // Spaces are not valid in job identifiers.
        Some(("request_id", "not a valid id")),
    )
    .await;

    let status = result.status.expect_err("expected error status");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let completed = result.completed.expect("termination response");
    assert_eq!(
        completed.termination_reason(),
        proto::TerminationReason::InvalidArgument
    );
    assert!(completed.termination_message.contains("job identifier"));
}

#[tokio::test]
async fn valid_job_id_metadata_is_accepted() {
    let address = start_server().await;
    let data = vec![4u8; 8_000];
    let mut client = connect(&address).await;

    let (tx, rx) = mpsc::channel::<proto::TransformRequest>(16);
    tx.send(config_request(passthrough_config())).await.unwrap();
    tx.send(payload_request("I1", &data)).await.unwrap();
    tx.send(completed_request("I1")).await.unwrap();
    drop(tx);

    let mut request = tonic::Request::new(ReceiverStream::new(rx));
    request
        .metadata_mut()
        .insert("request_id", "client-supplied-id-42".parse().unwrap());

    let mut stream = client
        .transform(request)
        .await
        .expect("call ok")
        .into_inner();
    let mut completed = None;
    while let Some(response) = stream.message().await.expect("stream ok") {
        if let Some(proto::transform_response::Response::TransformCompleted(done)) =
            response.response
        {
            completed = Some(done);
        }
    }
    assert_eq!(
        completed.expect("termination").termination_reason(),
        proto::TerminationReason::EndOfStream
    );
}

#[tokio::test]
async fn unknown_stream_id_is_rejected() {
    let address = start_server().await;
    let result = run_transform(
        &address,
        vec![
            config_request(passthrough_config()),
            payload_request("bogus", &[0u8; 64]),
        ],
    )
    .await;

    let status = result.status.expect_err("expected error status");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let completed = result.completed.expect("termination response");
    assert!(completed.termination_message.contains("stream identifier"));
}

#[tokio::test]
async fn payload_after_channel_completed_is_rejected() {
    let address = start_server().await;
    let result = run_transform(
        &address,
        vec![
            config_request(passthrough_config()),
            payload_request("I1", &[0u8; 64]),
            completed_request("I1"),
            payload_request("I1", &[0u8; 64]),
        ],
    )
    .await;

    let status = result.status.expect_err("expected error status");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let completed = result.completed.expect("termination response");
    assert!(completed.termination_message.contains("ChannelCompleted"));
}

#[tokio::test]
async fn input_buffer_overflow_is_reported() {
    let address = start_server().await;
    let mut config = passthrough_config();
    config.parameter_overrides = vec![
        proto::ServiceParameter {
            value: Some(
                proto::service_parameter::Value::FlowControlEnforcementPolicy(
                    proto::FlowControlEnforcementPolicy::Error.into(),
                ),
            ),
        },
        proto::ServiceParameter {
            value: Some(proto::service_parameter::Value::PipelineInputBuffer(1024)),
        },
        // Realtime pacing keeps the pipeline from draining as fast as the
        // oversized payload arrives.
        proto::ServiceParameter {
            value: Some(proto::service_parameter::Value::Rate(1.0)),
        },
    ];

    let result = run_transform(
        &address,
        vec![
            config_request(config),
            payload_request("I1", &vec![0u8; 64 * 1024]),
            completed_request("I1"),
        ],
    )
    .await;

    let status = result.status.expect_err("expected error status");
    assert_eq!(status.code(), tonic::Code::Unknown);
    let completed = result.completed.expect("termination response");
    assert_eq!(
        completed.termination_reason(),
        proto::TerminationReason::InputRateExceeded
    );
}

#[tokio::test]
async fn invalid_extension_metadata_is_rejected() {
    let address = start_server().await;
    let mut config = passthrough_config();
    config.inputs[0]
        .metadata
        .insert("extension".to_string(), "../etc/passwd".to_string());

    let result = run_transform(&address, vec![config_request(config)]).await;

    let status = result.status.expect_err("expected error status");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn valid_extension_metadata_is_accepted() {
    let address = start_server().await;
    let data = vec![6u8; 8_000];
    let mut config = passthrough_config();
    config.inputs[0]
        .metadata
        .insert("extension".to_string(), ".pcm".to_string());

    let result = run_transform(
        &address,
        vec![
            config_request(config),
            payload_request("I1", &data),
            completed_request("I1"),
        ],
    )
    .await;

    assert!(result.status.is_ok(), "status: {:?}", result.status);
    assert_eq!(result.output, data);
}

#[tokio::test]
async fn duplicate_channel_completed_is_rejected() {
    let address = start_server().await;
    let result = run_transform(
        &address,
        vec![
            config_request(passthrough_config()),
            completed_request("I1"),
            completed_request("I1"),
        ],
    )
    .await;

    let status = result.status.expect_err("expected error status");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn shutdown_notification_is_sent_while_shutting_down() {
    let (address, shutting_down) = start_server_with(CONFIG).await;
    shutting_down.store(true, std::sync::atomic::Ordering::SeqCst);

    let mut client = connect(&address).await;
    let (tx, rx) = mpsc::channel::<proto::TransformRequest>(16);
    tx.send(config_request(passthrough_config())).await.unwrap();
    tx.send(payload_request("I1", &[0u8; 4096])).await.unwrap();
    tx.send(completed_request("I1")).await.unwrap();
    drop(tx);

    let mut stream = client
        .transform(ReceiverStream::new(rx))
        .await
        .expect("call ok")
        .into_inner();
    let mut lifecycle_seen = false;
    while let Ok(Some(response)) = stream.message().await {
        if let Some(proto::transform_response::Response::ServerLifecycleNotification(n)) =
            response.response
        {
            assert_eq!(
                n.notification_type(),
                proto::ServerLifecycleNotificationType::ShuttingDown
            );
            lifecycle_seen = true;
        }
    }
    assert!(lifecycle_seen, "expected SHUTTING_DOWN notification");
}

#[tokio::test]
async fn health_reports_not_serving_while_shutting_down() {
    let (address, shutting_down) = start_server_with(CONFIG).await;
    shutting_down.store(true, std::sync::atomic::Ordering::SeqCst);

    let mut client = health_proto::health_client::HealthClient::connect(address)
        .await
        .expect("health client connects");
    let response = client
        .check(health_proto::HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("health check succeeds");
    assert_eq!(
        response.get_ref().status(),
        health_proto::health_check_response::ServingStatus::NotServing
    );
}

#[tokio::test]
async fn immediate_cancel_is_tolerated() {
    let address = start_server().await;
    let mut client = connect(&address).await;
    let (tx, rx) = mpsc::channel::<proto::TransformRequest>(16);
    tx.send(config_request(passthrough_config())).await.unwrap();

    let response = client
        .transform(ReceiverStream::new(rx))
        .await
        .expect("call ok");
    // Drop both halves right away: the server must clean the job up
    // without wedging (subsequent calls still work).
    drop(response);
    drop(tx);

    let data = vec![8u8; 4_000];
    let result = run_transform(
        &address,
        vec![
            config_request(passthrough_config()),
            payload_request("I1", &data),
            completed_request("I1"),
        ],
    )
    .await;
    assert!(result.status.is_ok(), "status: {:?}", result.status);
    assert_eq!(result.output, data);
}
