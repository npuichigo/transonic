//! In-process concurrency load test.
//!
//! Opens N concurrent `Transform` streams against a running transform
//! service, each streaming a PCM file as fast as possible, and reports
//! aggregate throughput (as a multiple of realtime) plus per-request latency
//! percentiles.  Runs entirely in one process so results are not polluted by
//! per-process startup cost.
//!
//! Usage:
//!   loadtest <endpoint> <pcm-file> <input-bin> <output-bin> \
//!            <source-seconds> <concurrency> [requests]

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::Semaphore;
use tokio_stream::wrappers::ReceiverStream;

use transonic_proto::v1 as proto;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 7 {
        eprintln!(
            "usage: {} <endpoint> <pcm-file> <input-bin> <output-bin> \
             <source-seconds> <concurrency> [requests]",
            args[0]
        );
        std::process::exit(2);
    }
    let endpoint = args[1].clone();
    let data = Bytes::from(std::fs::read(&args[2])?);
    let input_bin = args[3].clone();
    let output_bin = args[4].clone();
    let source_seconds: f64 = args[5].parse()?;
    let concurrency: usize = args[6].parse()?;
    let requests: usize = args
        .get(7)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(concurrency);

    // One shared channel so all streams reuse HTTP/2 multiplexing, like a
    // real client pool would.
    let channel = tonic::transport::Endpoint::from_shared(endpoint)?
        .connect()
        .await?;

    let semaphore = Arc::new(Semaphore::new(concurrency));
    let wall_start = Instant::now();
    let mut handles = Vec::with_capacity(requests);

    for _ in 0..requests {
        let permit = Arc::clone(&semaphore).acquire_owned().await?;
        let mut client =
            proto::transform_service_client::TransformServiceClient::new(channel.clone());
        let data = data.clone();
        let input_bin = input_bin.clone();
        let output_bin = output_bin.clone();
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            run_one(&mut client, data, &input_bin, &output_bin).await
        }));
    }

    let mut latencies = Vec::with_capacity(requests);
    let mut total_out: u64 = 0;
    let mut failures = 0;
    for handle in handles {
        match handle.await.unwrap() {
            Ok((latency, out_bytes)) => {
                latencies.push(latency);
                total_out += out_bytes;
            }
            Err(e) => {
                failures += 1;
                eprintln!("request failed: {e}");
            }
        }
    }
    let wall = wall_start.elapsed();

    latencies.sort();
    let pct = |p: f64| -> f64 {
        if latencies.is_empty() {
            return 0.0;
        }
        let idx = ((latencies.len() as f64 - 1.0) * p).round() as usize;
        latencies[idx].as_secs_f64() * 1000.0
    };
    let ok = latencies.len();
    let total_media = source_seconds * ok as f64;

    println!("concurrency={concurrency} requests={requests} ok={ok} failures={failures}",);
    println!("  wall           : {:.2} s", wall.as_secs_f64());
    println!(
        "  aggregate      : {:.0}x realtime  ({:.1} audio-seconds / wall-second)",
        total_media / wall.as_secs_f64(),
        total_media / wall.as_secs_f64()
    );
    println!(
        "  throughput     : {:.0} req/s   {:.1} MiB/s encoded out",
        ok as f64 / wall.as_secs_f64(),
        total_out as f64 / 1_048_576.0 / wall.as_secs_f64()
    );
    println!(
        "  latency/req    : p50 {:.0} ms  p90 {:.0} ms  p99 {:.0} ms  max {:.0} ms",
        pct(0.50),
        pct(0.90),
        pct(0.99),
        pct(1.0)
    );
    Ok(())
}

async fn run_one(
    client: &mut proto::transform_service_client::TransformServiceClient<tonic::transport::Channel>,
    data: Bytes,
    input_bin: &str,
    output_bin: &str,
) -> Result<(Duration, u64), String> {
    let start = Instant::now();
    let (tx, rx) = tokio::sync::mpsc::channel::<proto::TransformRequest>(16);

    let config = proto::TransformConfig {
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
        parameter_overrides: vec![],
    };
    tx.send(proto::TransformRequest {
        channel_id: String::new(),
        request: Some(proto::transform_request::Request::Config(config)),
    })
    .await
    .map_err(|e| e.to_string())?;

    // Feed the whole file as fast as possible (64 KiB frames).
    tokio::spawn(async move {
        let mut offset = 0usize;
        while offset < data.len() {
            let end = (offset + 65536).min(data.len());
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
        let _ = tx
            .send(proto::TransformRequest {
                channel_id: "in".to_string(),
                request: Some(proto::transform_request::Request::ChannelCompleted(
                    proto::ChannelCompleted {},
                )),
            })
            .await;
    });

    let mut responses = client
        .transform(ReceiverStream::new(rx))
        .await
        .map_err(|e| e.to_string())?
        .into_inner();

    let mut out_bytes = 0u64;
    while let Some(response) = responses.message().await.map_err(|e| e.to_string())? {
        if let Some(proto::transform_response::Response::Payload(payload)) = response.response {
            out_bytes += payload
                .chunk_list
                .iter()
                .map(|c| c.len() as u64)
                .sum::<u64>();
        }
    }
    Ok((start.elapsed(), out_bytes))
}
