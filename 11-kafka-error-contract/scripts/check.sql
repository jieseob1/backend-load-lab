-- 한 회차의 결과를 JSON 한 줄로
SELECT json_build_object(
  'product',     (SELECT count(*) FROM product),
  'side',        (SELECT count(*) FROM side_record),
  'failed',      (SELECT count(*) FROM failed_record),
  'acked',       (SELECT count(*) FROM ack_log),
  'redelivered', (SELECT count(*) FROM delivery_log WHERE deliveries > 1),
  'lostLoad',    (SELECT count(*) FROM ack_log a          -- ACK됐는데 적재도 실패 기록도 없음
                  WHERE NOT EXISTS (SELECT 1 FROM product p WHERE p.id = a.message_id)
                    AND NOT EXISTS (SELECT 1 FROM failed_record f WHERE f.message_id = a.message_id)),
  'lostSide',    (SELECT count(*) FROM ack_log a          -- ACK됐는데 부가 저장도 실패 기록도 없음
                  WHERE NOT EXISTS (SELECT 1 FROM side_record x WHERE x.product_id = a.message_id)
                    AND NOT EXISTS (SELECT 1 FROM failed_record f WHERE f.message_id = a.message_id))
);
