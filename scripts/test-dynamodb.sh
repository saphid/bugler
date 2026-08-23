#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  printf '%s\n' 'SKIP LOUDLY: Docker is unavailable, so DynamoDB Local conformance did not run.' >&2
  exit 0
fi

IMAGE='amazon/dynamodb-local:latest'
CONTAINER="bugler-dynamodb-test-$$"
DOCKER_CONFIG_DIR=$(mktemp -d)

cleanup() {
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
  rmdir "$DOCKER_CONFIG_DIR" 2>/dev/null || true
}
trap cleanup EXIT

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  DOCKER_CONFIG="$DOCKER_CONFIG_DIR" docker pull "$IMAGE"
fi

docker run --rm -d --name "$CONTAINER" -p 127.0.0.1::8000 "$IMAGE" \
  -jar DynamoDBLocal.jar -inMemory -sharedDb >/dev/null
PORT=$(docker port "$CONTAINER" 8000/tcp | sed -n 's/.*://p' | head -n 1)
if [[ -z "$PORT" ]]; then
  printf '%s\n' 'DynamoDB Local did not publish its test port.' >&2
  exit 1
fi

ready=0
for _ in $(seq 1 100); do
  if curl --silent --output /dev/null "http://127.0.0.1:$PORT/"; then
    ready=1
    break
  fi
  sleep 0.05
done
if ((ready == 0)); then
  docker logs "$CONTAINER" >&2
  printf '%s\n' 'DynamoDB Local did not become ready.' >&2
  exit 1
fi

BUGLER_DYNAMODB_ENDPOINT="http://127.0.0.1:$PORT" bun test tests/dynamodb.test.ts
