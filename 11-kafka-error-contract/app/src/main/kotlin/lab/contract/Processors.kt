package lab.contract

import com.fasterxml.jackson.core.JsonProcessingException
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

/**
 * ① 삼키기. 실무에서 확인된 지점(부가 저장 오류를 잡아 넘김, 적재 실패를 값으로 바꿈, 결과 미검사)을
 * 한 경로에 모은 모양입니다. 실무 코드를 그대로 옮긴 것은 아닙니다.
 */
@Component("silent")
class SilentProcessor(private val deps: Deps) : RecordProcessor {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun process(r: Rec): Mono<Void> = mono {
        deps.recordDelivery(r)
        try {
            handle(r)
        } catch (e: JsonProcessingException) { // 일부 오류만 잡아 건너뜀
            log.warn("skip unparsable key={} offset={}", r.key(), r.offset())
        } // 그 밖의 오류는 전파: ACK 없음
        deps.ack(r) // 정상 반환이면 ACK
    }.then()

    private suspend fun handle(r: Rec) {
        val msg = deps.parse(r)
        try {
            deps.saveSide(msg)
        } catch (e: Exception) {
            log.warn("side failed id={}", msg.id) // 부가 저장 실패를 삼킴
        }
        loadQuietly(msg) // 돌려받은 Result를 검사하지 않음
    }

    private suspend fun loadQuietly(m: ProductMessage): Result<Unit> =
        runCatching { deps.load(m) } // 적재 실패가 값으로 바뀌고 정상 반환
}

/** ② 모두 전파. 아무것도 잡지 않고, 모두 성공했을 때만 ACK합니다. */
@Component("propagate")
class PropagateProcessor(private val deps: Deps) : RecordProcessor {
    override fun process(r: Rec): Mono<Void> = mono {
        deps.recordDelivery(r)
        val msg = deps.parse(r) // 깨진 JSON → 예외
        deps.saveSide(msg) // 부가 저장 실패 → 예외
        deps.load(msg) // 적재 실패 → 예외
        deps.ack(r)
    }.then()
}
