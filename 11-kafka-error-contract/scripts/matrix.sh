#!/usr/bin/env bash
# 사용: scripts/matrix.sh [--with-crash]
# 회차 0(점검, 집계 제외) → 회차 1–3 × (mixed, transient) × 세 방식. --with-crash면 선택 조건도.
set -euo pipefail
cd "$(dirname "$0")/.."

for m in silent propagate outcome; do scripts/run.sh "$m" transient 0; done   # 점검용

for k in 1 2 3; do
  for c in mixed transient; do
    for m in silent propagate outcome; do scripts/run.sh "$m" "$c" "$k"; done
  done
  if [ "${1:-}" = "--with-crash" ]; then
    for m in silent outcome; do scripts/run.sh "$m" mixed-crash "$k"; done
  fi
done

python3 scripts/summarize.py results > results/summary.md
cat results/summary.md
