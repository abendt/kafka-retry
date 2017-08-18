package demo

import com.github.charithe.kafka.EphemeralKafkaBroker
import com.github.charithe.kafka.KafkaJunitRule
import org.assertj.core.api.KotlinAssertions
import org.awaitility.Awaitility
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.junit4.SpringRunner
import java.util.*
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.TimeUnit

@SpringBootTest
@RunWith(SpringRunner::class)
class CanSendToKafkaTest {

    var received = ConcurrentSkipListSet<String>()

    @Autowired
    lateinit var template: KafkaTemplate<String, String>

    @Test
    fun canSend() {
        val messages = ArrayList<String>()

        val size = 1000

        repeat(size) {
            val message = "$it"

            template.send("myTopic", message)
        }

        println("Sent $size Messages")

        Awaitility.await().atMost(1, TimeUnit.MINUTES).untilAsserted {
            KotlinAssertions.assertThat(received).hasSize(size)
        }

        println("OutOfOrder " + outOfOrder)
        println("Errors " + errors)
        println("Dups " + dups)
        println("Threads seen " + threads)
    }

    @KafkaListener(topics = arrayOf("myTopic"))
    fun listen(payload: String) {

        threads.add(Thread.currentThread().name)

        val nextInt = Math.abs(random.nextInt(100))

        if (nextInt < 20) {
            errors++
            throw RuntimeException("for test")
        }

        val index = payload.toInt() - 1

        if (index > 0) {
            if (!received.contains("$index")) {

                println("expected " + received + " to contain " + index)

                outOfOrder++
            }
        }

        if (received.contains(payload)) {
            dups++
        }

        received.add(payload)
    }

    companion object {
        @ClassRule
        @JvmField
        var kafkaRule = KafkaJunitRule(EphemeralKafkaBroker.create(9988))

        val random = Random(System.currentTimeMillis())

        var errors = 0
        var outOfOrder = 0
        var dups = 0

        val threads = HashSet<String>()
    }
}