package lab.contract

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.jdbc.core.JdbcTemplate
import reactor.core.publisher.Mono
import reactor.kafka.receiver.ReceiverOffset
import reactor.kafka.receiver.ReceiverRecord

/**
 * Kafka 없이 "정상 반환 뒤 ACK했는데 저장도 기록도 없다"를 보입니다.
 * 실행 전: docker compose up -d postgres
 */
@SpringBootTest(properties = ["lab.consumer.enabled=false"])
class ErrorContractTest @Autowired constructor(
    private val ctx: ApplicationContext,
    private val jdbc: JdbcTemplate,
) {
    class FakeOffset(private val tp: TopicPartition, private val off: Long) : ReceiverOffset {
        var acked = false
        override fun topicPartition() = tp
        override fun offset() = off
        override fun acknowledge() {
            acked = true
        }
        override fun commit(): Mono<Void> = Mono.empty()
    }

    private val badPrice = """{"id":13,"name":"p13","price":-1}""" // CHECK 위반 → 최종 적재 실패

    private fun offset() = FakeOffset(TopicPartition("products-mixed", 0), 13)

    private fun process(mode: String, off: FakeOffset) = ctx.getBean(mode, RecordProcessor::class.java)
        .process(ReceiverRecord(ConsumerRecord("products-mixed", 0, 13L, "13", badPrice), off))
        .block()

    private fun count(sql: String) = jdbc.queryForObject(sql, Long::class.java)

    @BeforeEach
    fun reset() {
        jdbc.execute("TRUNCATE product, side_record, delivery_log, ack_log, failed_record")
    }

    @Test
    fun `silent - 적재가 실패해도 정상 반환하고 ACK한다`() {
        val off = offset()
        process("silent", off)
        assertThat(off.acked).isTrue()
        assertThat(count("SELECT count(*) FROM product")).isZero()
        assertThat(count("SELECT count(*) FROM failed_record")).isZero() // 흔적 없음
    }

    @Test
    fun `propagate - 예외가 밖으로 나가고 ACK하지 않는다`() {
        val off = offset()
        assertThatThrownBy { process("propagate", off) }.isInstanceOf(DataIntegrityViolationException::class.java)
        assertThat(off.acked).isFalse()
    }

    @Test
    fun `outcome - 실패를 기록한 뒤에 ACK한다`() {
        val off = offset()
        process("outcome", off)
        assertThat(off.acked).isTrue()
        assertThat(jdbc.queryForObject("SELECT stage FROM failed_record", String::class.java)).isEqualTo("load")
    }
}
