package lab.contract

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class LabApplication

fun main(args: Array<String>) {
    runApplication<LabApplication>(*args)
}
