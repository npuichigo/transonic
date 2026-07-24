//! Backpressure observation tool.
//!
//! Drives the transform service with a *rate-limited* pipeline (so the
//! encoder drains at ~1x realtime) while feeding PCM *as fast as possible*
//! through a *small* input buffer, then reports the flow-control activity so
//! the appsrc backpressure machinery is visible:
//!
//!   * `ENOUGH_DATA` (server: queue exceeded the soft limit — stop sending)
//!   * `NEED_DATA`   (server: queue drained — okay to send again)
//!
//! Three scenarios are run against the same clip:
//!   A. baseline      — full-speed pipeline, no backpressure.
//!   B. BLOCK policy  — realtime pipeline, small buffer; the client's sends
//!                      are throttled by HTTP/2 flow control back to the
//!                      encoder's drain rate.  No data lost, memory bounded.
//!   C. NOTIFY + client ignores the notifications — the hard limit is hit and
//!                      the call is terminated with INPUT_RATE_EXCEEDED.
//!
//! Usage: backpressure <endpoint> <pcm-file> <input-bin> <output-bin> <source-seconds>

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio_stream::wrappers::ReceiverStream;

use transonic_proto::v1 as proto;

const SOFT_LIMIT_BYTES: u32 = 65_536; // hard limit is 4x = 256 KiB

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 6 {
        eprintln!(
            "usage: {} <endpoint> <pcm-file> <input-bin> <output-bin> <source-seconds>",
            args[0]
        );
        std::process::exit(2);
    }
    let endpoint = args[1].clone();
    let data = Bytes::from(std::fs::read(&args[2])?);
    let input_bin = args[3].clone();
    let output_bin = args[4].clone();
    let source_seconds: f64 = args[5].parse()?;

    let channel = tonic::transport::Endpoint::from_shared(endpoint)?
        .connect()
        .await?;

    println!(
        "clip: {:.0} KiB PCM, {:.1} s of audio | soft limit {} KiB, hard limit {} KiB\n",
        data.len() as f64 / 1024.0,
        source_seconds,
        SOFT_LIMIT_BYTES / 1024,
        SOFT_LIMIT_BYTES * 4 / 1024,
    );

    // A. Baseline: pipeline runs as fast as possible, generous buffer.
    println!("── A. baseline (rate=0 as-fast-as-possible, 2 MiB buffer) ──");
    run(
        &channel,
        &data,
        &input_bin,
        &output_bin,
        source_seconds,
        /* rate */ 0.0,
        /* input_buffer */ 2_097_152,
        Policy::Block,
    )
    .await?;

    // B. BLOCK policy: pipeline throttled to realtime, tiny buffer.  The
    //    client feeds full-speed; HTTP/2 flow control throttles it.
    println!(
        "\n── B. BLOCK policy (rate=1.0 realtime drain, {} KiB buffer, feed full speed) ──",
        SOFT_LIMIT_BYTES / 1024
    );
    run(
        &channel,
        &data,
        &input_bin,
        &output_bin,
        source_seconds,
        1.0,
        SOFT_LIMIT_BYTES,
        Policy::Block,
    )
    .await?;

    // C. NOTIFY policy, client ignores the ENOUGH_DATA notifications: the
    //    hard limit is reached and the call is terminated.
    println!(
        "\n── C. NOTIFY policy, client ignores notifications (rate=1.0, {} KiB buffer) ──",
        SOFT_LIMIT_BYTES / 1024
    );
    run(
        &channel,
        &data,
        &input_bin,
        &output_bin,
        source_seconds,
        1.0,
        SOFT_LIMIT_BYTES,
        Policy::Notify,
    )
    .await?;

    Ok(())
}

#[derive(Clone, Copy)]
enum Policy {
    Block,
    Notify,
}

#[allow(clippy::too_many_arguments)]
async fn run(
    channel: &tonic::transport::Channel,
    data: &Bytes,
    input_bin: &str,
    output_bin: &str,
    source_seconds: f64,
    rate: f64,
    input_buffer: u32,
    policy: Policy,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = proto::transform_service_client::TransformServiceClient::new(channel.clone());

    let mut overrides = vec![
        proto::ServiceParameter {
            value: Some(proto::service_parameter::Value::Rate(rate)),
        },
        proto::ServiceParameter {
            value: Some(proto::service_parameter::Value::PipelineInputBuffer(
                input_buffer,
            )),
        },
    ];
    if let Policy::Notify = policy {
        overrides.push(proto::ServiceParameter {
            value: Some(
                proto::service_parameter::Value::FlowControlEnforcementPolicy(
                    proto::FlowControlEnforcementPolicy::Notify.into(),
                ),
            ),
        });
    }

    let (tx, rx) = tokio::sync::mpsc::channel::<proto::TransformRequest>(4);
    tx.send(proto::TransformRequest {
        channel_id: String::new(),
        request: Some(proto::transform_request::Request::Config(
            proto::TransformConfig {
                pipeline_name: "tts".to_string(),
                inputs: vec![proto::ChannelSpecifier {
                    channel_id: "in".to_string(),
                    pipeline_bin_id: input_bin.to_string(),
                    pipeline_parameters: Default::default(),
                    metadata: Default::default(),
                }],
                outputs: vec![proto::ChannelSpecifier {
                    channel_id: "out".to_string(),
                    pipeline_bin_id: output_bin.to_string(),
                    pipeline_parameters: Default::default(),
                    metadata: Default::default(),
                }],
                parameter_overrides: overrides,
            },
        )),
    })
    .await?;

    // Producer: feed the whole clip in 4 KiB frames, full speed, timestamping
    // when each send completes so we can see the throttle.
    let feed_started = Instant::now();
    let feed_done = Arc::new(AtomicU64::new(0)); // millis, 0 = not done
    let data = data.clone();
    let feed_done_writer = Arc::clone(&feed_done);
    let producer = tokio::spawn(async move {
        let mut offset = 0usize;
        while offset < data.len() {
            let end = (offset + 4096).min(data.len());
            if tx
                .send(proto::TransformRequest {
                    channel_id: "in".to_string(),
                    request: Some(proto::transform_request::Request::Payload(
                        proto::InputChannelPayload {
                            chunk_list: vec![data.slice(offset..end)],
                        },
                    )),
                })
                .await
                .is_err()
            {
                return;
            }
            offset = end;
        }
        feed_done_writer.store(
            feed_started.elapsed().as_millis().max(1) as u64,
            Ordering::SeqCst,
        );
        let _ = tx
            .send(proto::TransformRequest {
                channel_id: "in".to_string(),
                request: Some(proto::transform_request::Request::ChannelCompleted(
                    proto::ChannelCompleted {},
                )),
            })
            .await;
    });

    let call_started = Instant::now();
    let mut responses = client
        .transform(ReceiverStream::new(rx))
        .await?
        .into_inner();

    let mut enough_data = 0u64; // ENOUGH_DATA (stop sending)
    let mut need_data = 0u64; // NEED_DATA (resume sending)
    let mut first_enough_at: Option<Duration> = None;
    let mut out_bytes = 0u64;
    let mut termination = None;

    loop {
        match responses.message().await {
            Ok(Some(response)) => match response.response {
                Some(proto::transform_response::Response::FlowControlNotification(n)) => {
                    match n.notification_type() {
                        proto::FlowControlNotificationType::EnoughData => {
                            enough_data += 1;
                            first_enough_at.get_or_insert(call_started.elapsed());
                        }
                        proto::FlowControlNotificationType::NeedData => need_data += 1,
                    }
                }
                Some(proto::transform_response::Response::Payload(p)) => {
                    out_bytes += p.chunk_list.iter().map(|c| c.len() as u64).sum::<u64>();
                }
                Some(proto::transform_response::Response::TransformCompleted(c)) => {
                    termination = Some(format!("{:?}", c.termination_reason()));
                }
                _ => {}
            },
            Ok(None) => break,
            Err(status) => {
                termination = Some(format!("gRPC {}: {}", status.code(), status.message()));
                break;
            }
        }
    }
    let call_wall = call_started.elapsed();
    producer.abort();

    let feed_ms = feed_done.load(Ordering::SeqCst);
    let feed_desc = if feed_ms == 0 {
        "did not finish (call ended first)".to_string()
    } else {
        format!("{:.2} s", feed_ms as f64 / 1000.0)
    };

    println!("  flow control   : {enough_data} × ENOUGH_DATA, {need_data} × NEED_DATA");
    if let Some(t) = first_enough_at {
        println!(
            "  first ENOUGH_DATA at {:.0} ms into the call",
            t.as_secs_f64() * 1000.0
        );
    }
    println!(
        "  feed completed : {feed_desc}   (audio is {:.1} s — if feed ≈ audio, the client \
         was throttled to the drain rate)",
        source_seconds
    );
    println!(
        "  call wall      : {:.2} s   encoded out {:.0} KiB",
        call_wall.as_secs_f64(),
        out_bytes as f64 / 1024.0
    );
    println!(
        "  termination    : {}",
        termination.as_deref().unwrap_or("stream ended")
    );
    Ok(())
}
