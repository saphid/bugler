#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  printf '%s\n' 'SKIP LOUDLY: Docker is unavailable, so Firestore emulator conformance did not run.' >&2
  exit 0
fi

IMAGE='gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators'
CONTAINER="bugler-firestore-test-$$"
PROJECT_ID='bugler-firestore-test'
DOCKER_CONFIG_DIR=$(mktemp -d)

cleanup() {
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
  rmdir "$DOCKER_CONFIG_DIR" 2>/dev/null || true
}
trap cleanup EXIT

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  DOCKER_CONFIG="$DOCKER_CONFIG_DIR" docker pull "$IMAGE"
fi

docker run --rm -d --name "$CONTAINER" -p 127.0.0.1::8080 "$IMAGE" \
  gcloud emulators firestore start --project="$PROJECT_ID" --host-port=0.0.0.0:8080 --quiet >/dev/null
PORT=$(docker port "$CONTAINER" 8080/tcp | sed -n 's/.*://p' | head -n 1)
if [[ -z "$PORT" ]]; then
  printf '%s\n' 'Firestore emulator did not publish its test port.' >&2
  exit 1
fi

ready=0
for _ in $(seq 1 300); do
  if curl --silent --output /dev/null "http://127.0.0.1:$PORT/"; then
    ready=1
    break
  fi
  sleep 0.1
done
if ((ready == 0)); then
  docker logs "$CONTAINER" >&2
  printf '%s\n' 'Firestore emulator did not become ready.' >&2
  exit 1
fi

FIRESTORE_EMULATOR_HOST="127.0.0.1:$PORT" \
FIRESTORE_PROJECT_ID="$PROJECT_ID" \
  bun test tests/firestore.test.ts
