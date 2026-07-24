#!/usr/bin/env bash
# Installs the system packages needed to build Transonic on Debian/Ubuntu.
set -euo pipefail

sudo apt-get update
sudo apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    protobuf-compiler \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    libglib2.0-dev

echo "Build dependencies installed."
echo "For runtime codec coverage also run: scripts/install-runtime-dependencies.sh"
