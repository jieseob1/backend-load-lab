#!/usr/bin/env bash
# 사용: scripts/run.sh <방식> <조건> <회차>   예) scripts/run.sh outcome mixed 1
#   방식: silent | propagate | outcome
#   조건: mixed | transient | mixed-crash
# 순서: 초기화 → 측정(lag 0 또는 180초) → 커밋 대기 12초 → 결과 JSON
set -euo pipefail
cd "$(dirname "$0")/.."

MODE=$1 COND=$2 RUN=$3
NAME="${MODE}_${COND}_run${RUN}" RAW=results/raw
mkdir -p "$RAW"
BASE=${COND%-crash}                                         # mixed-crash → mixed
export MODE FAULTS=$BASE TOPIC="products-$BASE" GROUP="g-${NAME}-$(date +%s)"

APP="localhost:${APP_PORT:-18080}"
sql() { docker compose exec -T postgres psql -U lab -d lab -At "$@"; }
m()   { curl -s "$APP/actuator/prometheus" | grep "^$1" | awk '{s+=$NF} END{print s+0}'; }
# 그룹이 Stable일 때만 lag를 냅니다. 재시작 직후처럼 그룹이 비어 있거나 Dead이면 TOTAL-LAG가 0으로 보이기 때문입니다.
lag() { docker compose exec -T redpanda rpk group describe "$GROUP" 2>/dev/null \
          | awk '$1=="STATE"{s=$2} $1=="TOTAL-LAG"{l=$2} END{if (s=="Stable") print l}'; }
up()  { until curl -sf "$APP/actuator/health" >/dev/null; do sleep 1; done; }

echo "▶ $NAME (group=$GROUP)"
docker compose stop consumer >/dev/null 2>&1 || true        # 초기화: 소비자를 멈춘 상태에서
sql -q < app/src/main/resources/schema.sql >/dev/null        # 테이블을 먼저 만들고
sql -c "TRUNCATE product, side_record, delivery_log, ack_log, failed_record;" >/dev/null   # 비운 뒤에 앱을 띄움
docker compose up -d --force-recreate consumer
up

T0=$SECONDS KILLED=0
while [ $((SECONDS - T0)) -lt 180 ]; do                       # 측정: lag 0 또는 180초
  docker stats --no-stream --format "$((SECONDS - T0)),{{.Name}},{{.CPUPerc}},{{.MemUsage}}" >> "$RAW/${NAME}_stats.csv"
  if [[ $COND == *-crash && $KILLED == 0 && $(sql -c "SELECT count(*) FROM ack_log") -ge 5000 ]]; then
    echo "  SIGKILL at ack ≥ 5000"
    docker compose kill -s SIGKILL consumer
    docker compose up -d consumer                             # 같은 그룹으로 재시작
    up
    KILLED=1
  fi
  [ "$(lag)" = "0" ] && break
  sleep 1
done
ELAPSED=$((SECONDS - T0))
sleep 12                                                      # commit interval 5초 × 2 + 여유

docker compose logs consumer > "$RAW/${NAME}_consumer.log" 2>&1
WARN=$(grep -c ' WARN ' "$RAW/${NAME}_consumer.log" || true)
LAG=$(lag); LAG=${LAG:-null}

sql < scripts/check.sql | jq --arg name "$NAME" --argjson elapsed "$ELAPSED" --argjson lag "$LAG" \
  --argjson restarts "$(m lab_restarts_total)" --argjson failedMetric "$(m lab_failed_total)" \
  --argjson warn "${WARN:-0}" \
  '. + {name:$name, elapsedSec:$elapsed, remainingLag:$lag, restarts:$restarts,
        failedMetric:$failedMetric, warnLines:$warn}' > "results/${NAME}.json"

cat "results/${NAME}.json"
