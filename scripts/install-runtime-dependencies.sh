#!/usr/bin/env bash
# Installs the GStreamer runtime plugin packages that provide the codecs
# referenced by the shipped pipeline configurations (Debian/Ubuntu).
set -euo pipefail

sudo apt-get update
sudo apt-get install -y --no-install-recommends \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    gstreamer1.0-tools

echo "Runtime dependencies installed."
