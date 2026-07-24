#!/usr/bin/env bash
# Sustained high-concurrency load generator for the Grafana demo.
#
# Unlike a burst loop, this keeps a fixed number of transforms continuously
# in flight so `transonic_active_jobs` stays pinned near the concurrency
# target and the "Active jobs (concurrency)" time series shows a full,
# elevated band instead of a sawtooth that snaps back to 1.
set -u
cd "$(dirname "$0")/.."

ENDPOINT=http://127.0.0.1:15330
IN=testdata/librivox-austen-24kHz-f32le-mono.raw
LT=./target/release/examples/loadtest
BP=./target/release/examples/backpressure

CONC=32          # requests kept continuously in flight
REQS=6000        # per run: ~6000*0.17s/32 ~= 32s of pinned concurrency
SRC=30.0         # seconds of PCM synthesized per request

i=0
while true; do
  i=$((i + 1))
  # Sustained clean load: pins active_jobs at $CONC for the whole run.
  $LT "$ENDPOINT" "$IN" pcm_f32_24k mp3-24khz "$SRC" "$CONC" "$REQS" >/dev/null 2>&1

  # Every 4th run, add a short error burst (bad output bin) + one backpressure
  # observation so the error-ratio and flow-control panels keep moving too.
  if [ $((i % 4)) -eq 0 ]; then
    $LT "$ENDPOINT" "$IN" pcm_f32_24k SANDBAG 5.0 4 16 >/dev/null 2>&1
    $BP "$ENDPOINT" "$IN" pcm_f32_24k mp3-24khz 30.0 >/dev/null 2>&1
  fi
done
