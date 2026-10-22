package lab.contract

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("lab")
data class LabProps(
    val bootstrap: String,
    val group: String,
    val mode: String,
    val topic: String,
    val faults: String,
) {
    fun consumerProps(): Map<String, Any> = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
        ConsumerConfig.GROUP_ID_CONFIG to group,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 500,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
    )
}
