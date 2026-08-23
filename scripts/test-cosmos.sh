#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  printf '%s\n' 'SKIP LOUDLY: Docker is unavailable, so Cosmos vNext emulator conformance did not run.' >&2
  exit 0
fi

IMAGE='mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-latest'
CONTAINER="bugler-cosmos-test-$$"
DOCKER_CONFIG_DIR=$(mktemp -d)
COSMOS_KEY='C2y6yDjf5/R+ob0N8A7Cgv30VRDjE5E+eAdF7pZr4vJlTsSsvb3tOPQSnC29D9xU6QckvYi3vBwAf3vYlA=='

cleanup() {
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
  rmdir "$DOCKER_CONFIG_DIR" 2>/dev/null || true
}
trap cleanup EXIT

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  DOCKER_CONFIG="$DOCKER_CONFIG_DIR" docker pull "$IMAGE"
fi

docker run --rm -d --name "$CONTAINER" \
  -e ENABLE_TELEMETRY=false \
  -p 127.0.0.1::8081 \
  -p 127.0.0.1::8080 \
  "$IMAGE" >/dev/null
API_PORT=$(docker port "$CONTAINER" 8081/tcp | sed -n 's/.*://p' | head -n 1)
HEALTH_PORT=$(docker port "$CONTAINER" 8080/tcp | sed -n 's/.*://p' | head -n 1)
if [[ -z "$API_PORT" || -z "$HEALTH_PORT" ]]; then
  printf '%s\n' 'Cosmos emulator did not publish its test ports.' >&2
  exit 1
fi

ready=0
for _ in $(seq 1 150); do
  if curl --silent --fail "http://127.0.0.1:$HEALTH_PORT/ready" >/dev/null; then
    ready=1
    break
  fi
  sleep 0.1
done
if ((ready == 0)); then
  docker logs "$CONTAINER" >&2
  printf '%s\n' 'Cosmos emulator did not become ready.' >&2
  exit 1
fi

BUGLER_COSMOS_ENDPOINT="http://127.0.0.1:$API_PORT" \
BUGLER_COSMOS_KEY="$COSMOS_KEY" \
  bun test tests/cosmos.test.ts
