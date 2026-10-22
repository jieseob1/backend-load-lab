CREATE TABLE IF NOT EXISTS product (
  id BIGINT PRIMARY KEY,
  name TEXT NOT NULL,
  price NUMERIC NOT NULL CHECK (price >= 0),        -- 최종 적재 실패 주입: price -1
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS side_record (              -- 부가 저장소 대용
  product_id BIGINT PRIMARY KEY,
  payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS delivery_log (             -- id별 전달 횟수(재처리 범위)
  message_id BIGINT PRIMARY KEY,
  deliveries INT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS ack_log (                  -- 분석용: ACK한 id. 실무 경로에는 없음
  message_id BIGINT PRIMARY KEY,
  acks INT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS failed_record (            -- DLT 대용. 방식 ③만 씀
  id BIGSERIAL PRIMARY KEY,
  message_id BIGINT NOT NULL,                         -- Kafka key = 상품 id. 값이 깨져도 key는 읽힘
  topic_partition TEXT NOT NULL,
  record_offset BIGINT NOT NULL,
  stage TEXT NOT NULL,                                -- parse, side, load, retry_exhausted
  reason TEXT NOT NULL,
  payload TEXT,
  UNIQUE (topic_partition, record_offset, stage)      -- 재전달돼도 한 번만 기록
);
