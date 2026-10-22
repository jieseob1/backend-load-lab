package lab.contract

import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.springframework.beans.factory.annotation.Value
import org.springframework.dao.TransientDataAccessResourceException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import reactor.kafka.receiver.ReceiverRecord
import java.math.BigDecimal
import java.util.concurrent.ConcurrentHashMap

typealias Rec = ReceiverRecord<String, String>

data class ProductMessage(val id: Long, val name: String, val price: BigDecimal)

/** 실패를 id로 정해 주입합니다. 회차마다 같은 id가 실패합니다. */
@Component
class FaultInjector(@Value("\${lab.faults:mixed}") private val faults: String) {
    private val seen = ConcurrentHashMap.newKeySet<Long>()

    fun side(id: Long) {
        if (faults == "mixed" && id % 100 == 7L) error("side store down (injected)")
    }

    /** 프로세스 안에서 id당 첫 시도만 실패합니다. 재시작하면 기억이 초기화됩니다. */
    fun transient(id: Long) {
        if (id % 1000 == 29L && seen.add(id)) throw TransientDataAccessResourceException("db blip (injected)")
    }
}

/** 파싱, 부가 저장, 최종 적재, ACK를 한곳에 둡니다. 세 방식이 같은 부품을 씁니다. */
@Component
class Deps(
    private val jdbc: JdbcTemplate,
    val mapper: ObjectMapper,
    private val faults: FaultInjector,
    registry: MeterRegistry,
) {
    private val acked = registry.counter("lab.acked")

    /** 깨진 JSON이면 JsonProcessingException */
    fun parse(r: Rec): ProductMessage = mapper.readValue(r.value(), ProductMessage::class.java)

    suspend fun recordDelivery(r: Rec) = db(
        "INSERT INTO delivery_log(message_id) VALUES (?) " +
            "ON CONFLICT (message_id) DO UPDATE SET deliveries = delivery_log.deliveries + 1",
        r.key().toLong(),
    )

    suspend fun saveSide(m: ProductMessage) {
        faults.side(m.id)
        db(
            "INSERT INTO side_record VALUES (?, ?) ON CONFLICT (product_id) DO UPDATE SET payload = EXCLUDED.payload",
            m.id, m.name,
        )
    }

    suspend fun load(m: ProductMessage) {
        faults.transient(m.id)
        db(
            "INSERT INTO product(id, name, price) VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE " +
                "SET name = EXCLUDED.name, price = EXCLUDED.price, updated_at = now()",
            m.id, m.name, m.price,
        )
    }

    suspend fun ack(r: Rec) {
        db(
            "INSERT INTO ack_log(message_id) VALUES (?) ON CONFLICT (message_id) DO UPDATE SET acks = ack_log.acks + 1",
            r.key().toLong(),
        )
        r.receiverOffset().acknowledge() // 커밋 대상 표시일 뿐, broker commit 완료가 아님
        acked.increment()
    }

    /** 실험 앱은 JDBC라서 블로킹 호출만 IO 디스패처로 옮깁니다(실무는 R2DBC). */
    suspend fun db(sql: String, vararg args: Any): Int = withContext(Dispatchers.IO) { jdbc.update(sql, *args) }
}
