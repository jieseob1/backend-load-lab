package lab.contract

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

/**
 * producer 프로필에서만 실행됩니다. id 1–10,000을 key=id로 두 토픽에 보냅니다.
 * products-mixed: id % 1000 == 500 → 깨진 JSON, id % 100 == 13 → price -1(CHECK 위반)
 * products-transient: 모두 정상 값
 */
@Component
@Profile("producer")
class ProducerRunner(private val props: LabProps) : CommandLineRunner {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun run(vararg args: String) {
        val config = mapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to props.bootstrap,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        )
        KafkaProducer<String, String>(config).use { producer ->
            for (topic in listOf("products-mixed", "products-transient")) {
                val mixed = topic == "products-mixed"
                for (id in 1L..10_000L) {
                    val value = when {
                        mixed && id % 1000 == 500L -> "{broken"
                        mixed && id % 100 == 13L -> """{"id":$id,"name":"p$id","price":-1}"""
                        else -> """{"id":$id,"name":"p$id","price":${id % 500 + 1}}"""
                    }
                    producer.send(ProducerRecord(topic, id.toString(), value))
                }
                producer.flush()
                log.info("sent 10000 records to {}", topic)
            }
        }
    }
}
