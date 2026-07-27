#!/usr/bin/env bash
# Submit (or resubmit) the bars-1s Flink job. A fresh submit reprocesses
# from earliest offsets — run `make reset-bars` first or you will get
# duplicate bars (see flink/bars_job.py docstring).
set -euo pipefail

if docker exec flink-jobmanager flink list -r 2>/dev/null | grep -q bars-1s; then
    echo "bars-1s already running; cancel it first:"
    docker exec flink-jobmanager flink list -r | grep bars-1s
    exit 1
fi

docker exec flink-jobmanager flink run -d \
    -py /opt/pipeline/bars_job.py \
    --pyFiles /opt/pipeline/book.py
