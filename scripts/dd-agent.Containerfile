# Datadog agent for the compose dev stack (compose.yaml): the community grpc_check
# integration (+ its python deps, which `agent integration install` does not ship)
# and the three check configs, baked in at image build time.
#
# The gRPC health probe targets the connector by compose service name; the journald
# and process checks rely on `pid: host` plus the VM journal mounts declared in
# compose.yaml.

FROM docker.io/datadog/agent:7

ENV PIP_ROOT_USER_ACTION=ignore
RUN agent integration install -r -t datadog-grpc-check==1.1.0 \
    && /opt/datadog-agent/embedded/bin/pip install -q grpcio grpcio-health-checking

COPY dd-agent-process.yaml  /etc/datadog-agent/conf.d/process.d/conf.yaml
COPY dd-agent-journald.yaml /etc/datadog-agent/conf.d/journald.d/conf.yaml
COPY dd-agent-grpc.yaml     /etc/datadog-agent/conf.d/grpc_check.d/conf.yaml
