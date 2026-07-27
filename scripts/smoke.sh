#!/usr/bin/env bash
# Phase 0 smoke test: every service healthy, and a message survives a
# produce/consume round trip through Redpanda.
set -uo pipefail

PASS=0
FAIL=0

check() {
  local name=$1; shift
  local tries=${TRIES:-30}
  for ((i = 1; i <= tries; i++)); do
    if "$@" > /dev/null 2>&1; then
      echo "ok    $name"
      PASS=$((PASS + 1))
      return 0
    fi
    sleep 2
  done
  echo "FAIL  $name  ($*)"
  FAIL=$((FAIL + 1))
  return 1
}

echo "--- service health ---"
check "redpanda    cluster healthy"  docker exec redpanda sh -c "rpk cluster health | grep -q 'Healthy:.*true'"
check "flink       jobmanager REST"  curl -sf http://localhost:8081/config
check "flink       taskmanager registered" sh -c "curl -sf http://localhost:8081/taskmanagers | grep -q '\"id\"'"
check "minio       live"             curl -sf http://localhost:9000/minio/health/live
check "minio       warehouse bucket" docker run --rm --network market-microstructure_default --entrypoint sh minio/mc:latest -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc ls local/warehouse"
check "postgres    market db"        docker exec postgres pg_isready -U market -d market
check "postgres    iceberg db"       docker exec postgres psql -U market -d iceberg -c "SELECT 1"
check "redis       ping"             docker exec redis redis-cli ping
check "trino       catalogs"         sh -c "docker exec trino trino --execute 'SHOW CATALOGS' | grep -q iceberg"
check "trino       iceberg round trip" sh -c "docker exec trino trino --execute 'CREATE SCHEMA IF NOT EXISTS iceberg.smoke; DROP TABLE IF EXISTS iceberg.smoke.t; CREATE TABLE iceberg.smoke.t (id int); INSERT INTO iceberg.smoke.t VALUES (42); SELECT * FROM iceberg.smoke.t' | grep -q 42 && docker exec trino trino --execute 'DROP TABLE iceberg.smoke.t; DROP SCHEMA iceberg.smoke'"

echo "--- kafka round trip ---"
TOPIC="smoke.test.$$"
MSG="hello-$(date +%s)"
check "topic create"   docker exec redpanda rpk topic create "$TOPIC"
check "produce"        sh -c "echo '$MSG' | docker exec -i redpanda rpk topic produce '$TOPIC'"
check "consume"        sh -c "docker exec redpanda rpk topic consume '$TOPIC' -n 1 -o start | grep -q '$MSG'"
docker exec redpanda rpk topic delete "$TOPIC" > /dev/null 2>&1

echo "---"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
