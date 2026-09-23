#!/bin/bash

set -euo pipefail

echo "[task.sh] [1/2] Starting Execution."
export TZ="HST"
source /workspace/envs/prod.env

echo "[task.sh] [2/2] Running workflow."


echo "[task.sh] All done!"