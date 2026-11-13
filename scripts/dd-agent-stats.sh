#!/usr/bin/env bash
# Watch the local Datadog agent's DogStatsD ingestion counters, with per-interval
# deltas, to confirm the connector's UDP metrics are actually arriving.
#
# Counters come from `agent status` (cumulative since agent start): UDP packets and
# bytes received on 8125, metric samples parsed out of them, and parse errors.
# Growing +deltas while the connector runs = the pipeline works end-to-end.
#
# Usage: scripts/dd-agent-stats.sh
#   INTERVAL=2 scripts/dd-agent-stats.sh    # poll every 2s (default 5)
#
# Stops on Ctrl-C.

set -euo pipefail

CONTAINER="${CONTAINER:-dd-agent}"
ENGINE="${ENGINE:-podman}"
INTERVAL="${INTERVAL:-5}"

if ! "${ENGINE}" exec "${CONTAINER}" true >/dev/null 2>&1; then
    echo "${CONTAINER} container not running — try scripts/dev-stack.sh up -d dd-agent" >&2
    exit 1
fi

read_stats() {
    "${ENGINE}" exec "${CONTAINER}" agent status 2>/dev/null | awk '
        /Udp Packets:/         { gsub(",", "", $3); udp = $3 }
        /Udp Bytes:/           { gsub(",", "", $3); bytes = $3 }
        /Metric Packets:/      { gsub(",", "", $3); metrics = $3 }
        /Metric Parse Errors:/ { gsub(",", "", $4); errors = $4 }
        END { print udp, bytes, metrics, errors }'
}

echo "watching ${CONTAINER} DogStatsD counters every ${INTERVAL}s (Ctrl-C to stop)" >&2

first=1
prev_udp=0 prev_bytes=0 prev_metrics=0 prev_errors=0
while true; do
    read -r udp bytes metrics errors <<<"$(read_stats)" || true
    if [[ -z "${udp:-}" ]]; then
        echo "could not read DogStatsD stats from agent status (agent still starting?)" >&2
        sleep "${INTERVAL}"
        continue
    fi
    if ((first)); then
        printf '%(%T)T  udp_packets=%s  udp_bytes=%s  metrics=%s  parse_errors=%s\n' \
            -1 "${udp}" "${bytes}" "${metrics}" "${errors}"
        first=0
    else
        printf '%(%T)T  udp_packets=%s (+%d)  udp_bytes=%s (+%d)  metrics=%s (+%d)  parse_errors=%s (+%d)\n' \
            -1 \
            "${udp}" $((udp - prev_udp)) \
            "${bytes}" $((bytes - prev_bytes)) \
            "${metrics}" $((metrics - prev_metrics)) \
            "${errors}" $((errors - prev_errors))
    fi
    prev_udp=${udp} prev_bytes=${bytes} prev_metrics=${metrics} prev_errors=${errors}
    sleep "${INTERVAL}"
done
