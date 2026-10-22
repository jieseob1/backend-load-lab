# 11. Kafka 소비의 오류 계약과 ACK

handler가 정상 반환하면 ACK하는 구조에서, 하위 코드가 실패를 다루는 방식(계약)에 따라
"ACK했는데 저장도 실패 기록도 없는" 레코드가 얼마나 생기는지 재는 실험입니다.

| 방식 | 계약 |
|---|---|
| `silent` | 부가 저장 실패를 잡아 넘기고, 최종 적재 실패를 값(`Result`)으로 바꾼 뒤 검사하지 않음. 파싱 실패만 건너뛰고 ACK |
| `propagate` | 아무것도 잡지 않음. 모두 성공했을 때만 ACK |
| `outcome` | 결과 타입(`Outcome`)을 caller가 검사. 일시 오류만 제자리 재시도(최대 3회), 건너뛰거나 포기한 건은 `failed_record`·메트릭에 남긴 뒤에만 ACK |

`silent`는 이전 회사 코드에서 확인된 지점들을 한 경로에 모은 모양이며, 실무 코드를 옮긴 것이 아닙니다.
`outcome`은 "지금 다시 설계한다면"의 제안입니다.

## 환경

- JDK 21, Kotlin 1.9.25, Spring Boot 3.4.5, Reactor Kafka 1.3.21
- Docker Compose: Redpanda v24.2.7(1 CPU), PostgreSQL 16(1 CPU), consumer(2 CPU, 1GB, `-Xmx512m`)
- 입력: 상품 id 1–10,000, key=id, 파티션 4개, 토픽 2개
- 처리: 파티션별로 묶어 순서대로 처리(역순 ACK를 결과에서 배제), commit interval 5초
- 앱 포트: 호스트 `18080`(바꾸려면 `APP_PORT=...`)

## 실패 주입 (id로 고정, 회차마다 같음)

| 실패 | 대상 id | 건수 | 위치 | 성격 |
|---|---|---|---|---|
| 부가 저장 실패 | `id % 100 == 7` | 100 | `FaultInjector`, mixed만 | 매번 실패 |
| 최종 적재 실패 | `id % 100 == 13` | 100 | mixed 토픽 데이터(price -1, CHECK 위반) | 영구 |
| 일시적 DB 오류 | `id % 1000 == 29` | 10 | `FaultInjector`, 두 조건 모두 | 프로세스마다 id당 첫 시도만 |
| 파싱 불가 | `id % 1000 == 500` | 10 | mixed 토픽 데이터(깨진 JSON) | 영구 |

## 실행

```bash
# 0) 재현 테스트 (Kafka 없이 계약 3개 확인)
docker compose up -d --wait postgres
cd app && ./gradlew test && cd ..

# 1) 이미지 빌드, Redpanda·PostgreSQL 기동, 토픽 2개에 1만 건씩 적재
scripts/seed.sh

# 2) 한 회차: scripts/run.sh <방식> <조건> <회차>
scripts/run.sh outcome mixed 1

# 3) 전체: 회차 0(점검, 집계 제외) + 회차 1–3 × 2조건 × 3방식 (+ 재시작 조건)
scripts/matrix.sh --with-crash

# 정리
docker compose down -v
```

JDK가 21이 아니면 `JAVA_HOME`을 21로 맞춰 `./gradlew`를 실행합니다.

## 측정 정의

- 한 회차: 소비자 정지 → 스키마 생성·TRUNCATE → 새 group id로 앱 재생성 → lag 0 또는 180초 → 12초 대기(commit interval 5초 × 2 + 여유) → `scripts/check.sql`
- **흔적 없는 누락**: ACK한 id 가운데 DB에 저장도, 실패 기록도 없는 것. 아직 ACK하지 않고 Kafka에 남은 것은 누락이 아니라 남은 lag로 셉니다.
- `ack_log`, `delivery_log`는 분석용 기록이며 실무 경로에는 없습니다.
- 결과: `results/{방식}_{조건}_run{k}.json`, 원본 로그·`docker stats`는 `results/raw/`, 표는 `results/summary.md`

## 결과 (2026-10-19, 조건마다 3회 중앙값)

| 방식 | 조건 | 최종 적재 | 실패 기록 | ACK | 흔적 없는 누락 | 남은 lag | 재시작 | 2회 이상 전달 |
|---|---|---|---|---|---|---|---|---|
| silent | mixed | 9,880 | 0 | 10,000 | **120** | 0 | 0 | 0 |
| propagate | mixed | 49 | 0 | 49 | 0 | **9,951** | 53 | 2 |
| outcome | mixed | 9,890 | **210** | 10,000 | 0 | 0 | 0 | 0 |
| silent | transient | 9,990 | 0 | 10,000 | **10** | 0 | 0 | 0 |
| propagate | transient | 10,000 | 0 | 10,000 | 0 | 0 | 10 | 12 |
| outcome | transient | 10,000 | 0 | 10,000 | 0 | 0 | 0 | 0 |
| silent | mixed-crash | 9,880 | 0 | 10,000 | 120 | 0 | 0 | 516 (1,527 / 516 / 421) |
| outcome | mixed-crash | 9,890 | 210 | 10,000 | 0 | 0 | 0 | 400 (400 / 296 / 879) |

- 앞의 6개 조건은 3회 모두 같은 값이었습니다. 다만 `propagate·mixed`의 재시작 횟수만 52–53으로 조금 달랐고(표는 중앙값 53), 이 조건은 180초 제한에서 멈춘 값입니다.
- `mixed-crash`는 ACK 5,000건 시점에 SIGKILL 후 같은 그룹으로 재시작했습니다. 다시 전달된 양은 마지막 커밋(5초 주기) 뒤 처리분이라 회차마다 다릅니다.
- 전체 표: `results/summary.md`

## 한계

- 단일 인스턴스, 로컬 Docker. 처리량은 참고값이고 핵심 지표는 건수입니다.
- DLT 대신 같은 DB의 테이블을 씁니다. DB가 죽으면 기록도 실패하고, 그때 `outcome`은 ACK하지 않고 전파합니다.
- 재구독 때 새 `KafkaConsumer`를 만들도록 `Flux.defer`로 감쌌습니다. 오류로 끝날 때 그때까지 ACK한 offset이 커밋되는지는 Reactor Kafka 1.3.21 동작에 따릅니다.
- Reactor Kafka는 2025년 5월 유지보수 중단이 공지됐습니다.
