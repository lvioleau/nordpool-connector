#!/usr/bin/env bash
# Dev/test stack launcher: fetches the Nord Pool secret bundle via aws-vault, exports the
# profile-derived defaults, and delegates to `podman compose` on compose.yaml (MinIO +
# dd-agent + connector, service-DNS networking).
#
#   scripts/dev-stack.sh                     # up --build (dev endpoint, foreground)
#   scripts/dev-stack.sh up -d --build       # detached
#   scripts/dev-stack.sh logs -f connector
#   scripts/dev-stack.sh down                # stop the stack (MinIO volume kept)
#   ENV_FILE=connector.dev.env PROFILE=nordpool-dev scripts/dev-stack.sh
#
# Secrets: the connector reads every ${VAR} from its environment and never calls Secrets
# Manager itself. This wrapper fetches the bundle on the host (aws-vault) and exports each
# key for compose.yaml's pass-through entries; a key the env file already defines is
# skipped, so the env file wins over the bundle. The secret fetch
# only runs for subcommands that start containers (up / run / create); `down`, `logs`,
# `ps`, `build` skip it.
#
# Override via environment:
#   PROFILE     aws-vault profile for the secret fetch and the derived
#               s3://nordpool-archive-<env> bucket                  (default: nordpool-dev)
#   ENV_FILE    connector ${VAR} config file            (default: connector.dev.env)
#   AWS_REGION  Secrets Manager region                  (default: eu-central-1)
#   CARGO_PROFILE  cargo profile for the connector image build (default: release-debug)

set -euo pipefail

PROFILE="${PROFILE:-nordpool-dev}"
ENV_FILE="${ENV_FILE:-connector.dev.env}"
AWS_REGION="${AWS_REGION:-eu-central-1}"
export ENV_FILE

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

command -v podman >/dev/null || { echo "podman not found on PATH" >&2; exit 1; }

# On macOS podman derives the machine's API socket path from TMPDIR — including in
# `podman machine inspect` — so a shell with TMPDIR unset or altered (devenv shells are)
# computes a socket that doesn't exist. getconf returns the real per-user temp dir
# regardless of the environment; resolve the socket under it and pin DOCKER_HOST, which
# `podman compose` honors over its own computation.
if [[ "$(uname -s)" == "Darwin" && -z "${DOCKER_HOST:-}" ]]; then
    machine_sock="$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null || true)"
    if [[ ! -S "${machine_sock}" ]]; then
        machine_sock="$(TMPDIR="$(getconf DARWIN_USER_TEMP_DIR)" \
            podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null || true)"
    fi
    [[ -S "${machine_sock}" ]] && export DOCKER_HOST="unix://${machine_sock}"
fi

# Bucket derived from the profile: nordpool-dev -> nordpool-archive-dev,
# nordpool-prod-oncall -> nordpool-archive-prod. An explicit ARCHIVE_UPLOAD_URI wins.
if [[ ! -v ARCHIVE_UPLOAD_URI ]]; then
    upload_env="${PROFILE#nordpool-}"
    export ARCHIVE_UPLOAD_URI="s3://nordpool-archive-${upload_env%-oncall}"
fi
export DD_HOSTNAME="${DD_HOSTNAME:-$(hostname)-dev}"

# Keys the env file already defines — a secret must never override one of these.
declare -A env_file_keys=()
if [[ -f "${ENV_FILE}" ]]; then
    while IFS='=' read -r k _; do
        [[ "${k}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] && env_file_keys["${k}"]=1
    done < "${ENV_FILE}"
else
    echo "warning: ${ENV_FILE} not found — the connector will fail on the first unset \${VAR}." >&2
fi

fetch_secrets() {
    command -v aws-vault >/dev/null || { echo "aws-vault not found on PATH" >&2; exit 1; }
    command -v jq        >/dev/null || { echo "jq not found on PATH" >&2; exit 1; }
    local secret_id=""
    if [[ -f "${ENV_FILE}" ]]; then
        secret_id="$(grep -E '^NORDPOOL_SECRET_ID=' "${ENV_FILE}" | tail -n1 | cut -d= -f2- || true)"
    fi
    secret_id="${secret_id:-${NORDPOOL_SECRET_ID:-}}"
    if [[ -z "${secret_id}" ]]; then
        echo "NORDPOOL_SECRET_ID is unset — set it in ${ENV_FILE}." >&2
        exit 1
    fi
    local secret_json
    secret_json="$(aws-vault exec "${PROFILE}" -- \
        aws secretsmanager get-secret-value \
            --secret-id "${secret_id}" \
            --region "${AWS_REGION}" \
            --query SecretString --output text)"
    # Standardize key names on the NORDPOOL_MARKET_DATA_API_* namespace: dev bundles use
    # SSO_* keys (SSO_USER -> ...USERNAME is the one non-prefix rename); prod bundles are
    # already namespaced. Export each key unless the env file or shell already provides it.
    local rename='{ "SSO_USER": "NORDPOOL_MARKET_DATA_API_USERNAME",
                    "SSO_PASSWORD": "NORDPOOL_MARKET_DATA_API_PASSWORD" }'
    local overridden=() key value
    while IFS='=' read -r key value; do
        if [[ -n "${env_file_keys[${key}]:-}" || -v "${key}" ]]; then
            overridden+=("${key}")
            continue
        fi
        export "${key}=${value}"
    done < <(jq -r --argjson rename "${rename}" \
        'to_entries[]
         | ($rename[.key] // (.key | sub("^SSO_"; "NORDPOOL_MARKET_DATA_API_"))) as $name
         | "\($name)=\(.value)"' <<<"${secret_json}")
    if [[ ${#overridden[@]} -gt 0 ]]; then
        echo "env / ${ENV_FILE} takes precedence over the secret for: ${overridden[*]}" >&2
    fi
    # compose.yaml declares the canonical credential pair as pass-through `environment:`
    # entries, and an environment entry shadows the env_file value even when null — so
    # both must be exported here whichever source provides them (env file wins, then the
    # bundle), or the container ends up without them.
    local cred value
    for cred in NORDPOOL_MARKET_DATA_API_USERNAME NORDPOOL_MARKET_DATA_API_PASSWORD; do
        if [[ -n "${env_file_keys[${cred}]:-}" ]]; then
            value="$(grep -E "^${cred}=" "${ENV_FILE}" | tail -n1 | cut -d= -f2-)"
            export "${cred}=${value}"
        elif [[ ! -v "${cred}" ]]; then
            echo "${cred} is set neither in ${ENV_FILE} nor in the secret bundle" >&2
            exit 1
        fi
    done
}

# Default action: build what changed and run the stack in the foreground.
args=("$@")
[[ ${#args[@]} -eq 0 ]] && args=(up --build)

case "${args[0]}" in
    up|run|create) fetch_secrets ;;
esac

exec podman compose "${args[@]}"
