#!/bin/bash

set -eox pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# hacky install.sh that doesn't use sudo and installs bins to INSTALL_PATH
"$SCRIPT_DIR"/install.sh

# set configurations
"$INSTALL_PATH"/dolt config --global --add user.name Bojack Horseman
"$INSTALL_PATH"/dolt config --global --add user.email bojack@horseman.com
"$INSTALL_PATH"dolt config --global --add metrics.host eventsapi.awsdev.ld-corp.com
"$INSTALL_PATH"/dolt config --global --add  metrics.port 443
