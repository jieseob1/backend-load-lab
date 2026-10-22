package lab.contract

import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Profile
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.util.retry.Retry
import java.time.Duration

interface RecordProcessor {
    fun process(r: Rec): Mono<Void>
}

/**
 * 파티션별로 묶고 묶음 안에서는 하나씩 처리합니다. 그래서 ACK가 offset 순서대로 일어나
 * 역순 ACK가 결과에 섞이지 않습니다(역순 ACK 위험은 7번 실험).
 * 레코드에서 새어 나온 오류는 receive 흐름을 끝내므로 바깥에서 재구독합니다.
 */
@Component
@Profile("!producer")
@ConditionalOnProperty(prefix = "lab.consumer", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class ConsumerPipeline(
    private val props: LabProps,
    ctx: ApplicationContext,
    registry: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val processor = ctx.getBean(props.mode, RecordProcessor::class.java) // silent, propagate, outcome
    private val restarts = registry.counter("lab.restarts")
    private var subscription: Disposable? = null

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        val options = ReceiverOptions.create<String, String>(props.consumerProps())
            .subscription(listOf(props.topic))
            .commitInterval(Duration.ofSeconds(5)) // 실무와 같은 5초
            .commitBatchSize(0)

        subscription = Flux.defer { KafkaReceiver.create(options).receive() } // 재구독마다 새 receiver
            .groupBy { it.receiverOffset().topicPartition() }
            .flatMap({ part -> part.concatMap { r -> processor.process(r).thenReturn(r) } }, 16) // 파티션 수가 16 이하여야 멈추지 않음
            .retryWhen(
                Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                    .maxBackoff(Duration.ofSeconds(5))
                    .transientErrors(true) // 레코드가 처리돼 onNext가 오면 대기 시간 초기화
                    .doBeforeRetry {
                        restarts.increment()
                        log.error("consumer restart #{}: {}", it.totalRetries() + 1, it.failure().toString())
                    },
            )
            .subscribe()

        log.info("consumer started mode={} topic={} group={}", props.mode, props.topic, props.group)
    }

    @PreDestroy
    fun stop() {
        subscription?.dispose()
    }
}
