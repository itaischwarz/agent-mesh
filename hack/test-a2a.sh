#!/usr/bin/env bash
# Smoke-tests the A2A v1.0 round-trip against the containerized echoagent
# image: fetch the agent card, send a message, stream events to completion.
# Builds and runs the same image kind-load pushes into the cluster, then uses
# cmd/a2a-send as the client. Requires docker and curl.
set -euo pipefail

IMG="${ECHOAGENT_IMG:-echoagent:dev}"
URL="http://localhost:50051"
CONTAINER_NAME="echoagent-smoke-test"

echo "Building $IMG from cmd/echoagent/Dockerfile..."
docker build -t "$IMG" -f cmd/echoagent/Dockerfile .

echo "Starting container..."
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER_NAME" -p 50051:50051 "$IMG" >/dev/null
trap 'docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true' EXIT

echo "Waiting for the agent card..."
for _ in $(seq 1 30); do
  curl -fsS "$URL/.well-known/agent-card.json" >/dev/null 2>&1 && break
  sleep 0.5
done
if ! curl -fsS "$URL/.well-known/agent-card.json"; then
  echo "FAIL: echoagent never served its agent card; container logs:"
  docker logs "$CONTAINER_NAME" || true
  exit 1
fi
echo

go run ./cmd/a2a-send -url "$URL" -text "a2a smoke test"
echo "PASS: A2A v1.0 round-trip works against the containerized echoagent"
