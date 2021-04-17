#!/bin/bash

# Fetches and install Dolt. To be invoked by the Dockerfile

# echo commands to the terminal output
set -eox pipefail

# Install Dolt

apt-get update -y \
    && apt-get install curl \
    && sudo bash -c 'curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh | sudo bash' \
    && dolt config --global --add user.email bojack@horseman.com \
    && dolt config --global --add user.name "Bojack Horseman" \
    && dolt config --global --add metrics.host eventsapi.awsdev.ld-corp.com \
    && dolt config --global --add metrics.port 443
