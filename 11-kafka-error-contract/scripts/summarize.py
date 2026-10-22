#!/usr/bin/env python3
"""results/*_run{k}.json(회차 0 제외)을 (방식, 조건)별 중앙값 표로 만듭니다.
사용: python3 scripts/summarize.py results > results/summary.md
"""
import glob
import json
import os
import statistics
import sys
from collections import defaultdict

FIELDS = [
    ("product", "최종 적재 행"),
    ("side", "부가 저장 행"),
    ("failed", "실패 기록 행"),
    ("acked", "ACK된 id"),
    ("remainingLag", "남은 lag"),
    ("lostLoad", "흔적 없는 누락 최종"),
    ("lostSide", "흔적 없는 누락 부가"),
    ("warnLines", "WARN 줄"),
    ("failedMetric", "실패 메트릭"),
    ("restarts", "재시작"),
    ("redelivered", "2회 이상 전달 id"),
    ("elapsedSec", "소요 초(참고)"),
]
ORDER = [(m, c) for c in ("mixed", "transient") for m in ("silent", "propagate", "outcome")] + [
    ("silent", "mixed-crash"),
    ("outcome", "mixed-crash"),
]


def num(v):
    return v if isinstance(v, (int, float)) else None


def cell(values):
    nums = [v for v in (num(x) for x in values) if v is not None]
    if not nums:
        return "—"
    med = statistics.median(nums)
    med = int(med) if float(med).is_integer() else round(med, 1)
    if len(set(nums)) > 1:  # 회차마다 다르면 원값도 같이
        return f"{med} ({', '.join(str(v) for v in nums)})"
    return str(med)


def main(directory):
    runs = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(directory, "*_run*.json"))):
        name = os.path.basename(path)[:-5]
        mode, cond, run = name.split("_")
        if run == "run0":
            continue
        with open(path) as f:
            runs[(mode, cond)].append(json.load(f))

    header = ["방식", "조건", "회차"] + [label for _, label in FIELDS]
    print("| " + " | ".join(header) + " |")
    print("|" + "---|" * len(header))
    for key in ORDER:
        if key not in runs:
            continue
        rows = runs[key]
        cells = [cell([r.get(field) for r in rows]) for field, _ in FIELDS]
        print("| " + " | ".join([key[0], key[1], str(len(rows))] + cells) + " |")
    print()
    print("칸은 회차 중앙값입니다. 회차마다 값이 다르면 괄호에 원값을 적었습니다.")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "results")
