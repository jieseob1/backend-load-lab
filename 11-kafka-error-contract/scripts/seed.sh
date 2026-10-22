#!/usr/bin/env bash
# 사용: scripts/seed.sh
# jar·이미지 빌드 → Redpanda·PostgreSQL 기동 → 토픽 2개(파티션 4) 생성 → 1만 건씩 적재
set -euo pipefail
cd "$(dirname "$0")/.."

(cd app && ./gradlew -q bootJar)
docker compose build consumer
docker compose up -d redpanda postgres
until docker compose exec -T redpanda rpk cluster info >/dev/null 2>&1; do sleep 1; done

hw() { docker compose exec -T redpanda rpk topic describe "$1" -p 2>/dev/null | awk 'NR>1 {s+=$NF} END {print s+0}'; }

docker compose exec -T redpanda rpk topic create products-mixed products-transient -p 4 >/dev/null 2>&1 || true

if [ "$(hw products-mixed)" -gt 0 ]; then
  echo "토픽에 이미 데이터가 있어 적재를 건너뜁니다. 다시 채우려면: docker compose down -v"
else
  docker compose run --rm -e SPRING_PROFILES_ACTIVE=producer consumer
fi

for t in products-mixed products-transient; do
  echo "$t high watermark 합계: $(hw "$t") (기대값 10000)"
done
