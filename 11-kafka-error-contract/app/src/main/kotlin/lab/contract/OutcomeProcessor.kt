package lab.contract

import com.fasterxml.jackson.core.JsonProcessingException
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.delay
import kotlinx.coroutines.reactor.mono
import org.springframework.dao.DataAccessException
import org.springframework.dao.TransientDataAccessException
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import kotlin.coroutines.cancellation.CancellationException

sealed interface Outcome {
    data class Saved(val warnings: List<String>) : Outcome
    data class Skipped(val stage: String, val reason: String) : Outcome
    data class Failed(val stage: String, val reason: String, val retryable: Boolean) : Outcome
}

/**
 * ③ 결과 타입. handler가 결과를 돌려주고 caller가 검사합니다. 일시 오류만 제자리에서 최대 3번 시도하고,
 * 건너뛰거나 포기한 건은 failed_record와 메트릭에 남긴 뒤에만 ACK합니다.
 * "지금 다시 설계한다면"의 제안이며 당시 검토한 대안이 아닙니다.
 */
@Component("outcome")
class OutcomeProcessor(private val deps: Deps, private val failures: FailureRecorder) : RecordProcessor {
    override fun process(r: Rec): Mono<Void> = mono {
        deps.recordDelivery(r)
        var outcome = handle(r)
        var attempt = 1
        while (outcome is Outcome.Failed && outcome.retryable && attempt < 3) { // 일시 오류만 재시도
            delay(200L * attempt)
            attempt++
            outcome = handle(r)
        }
        failures.report(r, outcome) // 기록이 실패하면 예외 → ACK 없이 전파
        deps.ack(r)
    }.then()

    private suspend fun handle(r: Rec): Outcome {
        val msg = try {
            deps.parse(r)
        } catch (e: JsonProcessingException) {
            return Outcome.Skipped("parse", e.originalMessage ?: "unparsable")
        }
        val warnings = mutableListOf<String>()
        try {
            deps.saveSide(msg)
        } catch (e: CancellationException) {
            throw e // 취소는 삼키지 않음
        } catch (e: Exception) {
            warnings += "side: ${e.message}"
        }
        return try {
            deps.load(msg)
            Outcome.Saved(warnings)
        } catch (e: TransientDataAccessException) {
            Outcome.Failed("load", e.message ?: "transient", retryable = true)
        } catch (e: DataAccessException) {
            Outcome.Failed("load", e.mostSpecificCause.message ?: "load failed", retryable = false)
        }
    }
}

@Component
class FailureRecorder(private val deps: Deps, private val registry: MeterRegistry) {

    suspend fun report(r: Rec, outcome: Outcome) {
        registry.counter("lab.outcome", "type", outcome::class.simpleName!!.lowercase()).increment()
        when (outcome) {
            is Outcome.Saved -> outcome.warnings.forEach { record(r, "side", it) }
            is Outcome.Skipped -> record(r, outcome.stage, outcome.reason)
            is Outcome.Failed -> record(r, if (outcome.retryable) "retry_exhausted" else outcome.stage, outcome.reason)
        }
    }

    private suspend fun record(r: Rec, stage: String, reason: String) {
        deps.db(
            "INSERT INTO failed_record(message_id, topic_partition, record_offset, stage, reason, payload) " +
                "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (topic_partition, record_offset, stage) DO NOTHING",
            r.key().toLong(), "${r.topic()}-${r.partition()}", r.offset(), stage, reason, r.value(),
        )
        registry.counter("lab.failed", "stage", stage).increment()
    }
}
