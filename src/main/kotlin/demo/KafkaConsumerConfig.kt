package demo

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.retry.support.RetryTemplate
import org.springframework.retry.backoff.FixedBackOffPolicy
import org.springframework.retry.policy.SimpleRetryPolicy
import org.springframework.retry.RetryPolicy


@Configuration
@EnableKafka
class KafkaConsumerConfig {

    @Bean
    fun getRetryPolicy(): RetryPolicy {
        val simpleRetryPolicy = SimpleRetryPolicy()
        simpleRetryPolicy.maxAttempts = 100
        return simpleRetryPolicy
    }

    @Bean
    fun getBackOffPolicy(): FixedBackOffPolicy {
        val backOffPolicy = FixedBackOffPolicy()
        backOffPolicy.backOffPeriod = 10
        return backOffPolicy
    }

    @Bean
    fun getRetryTemplate(): RetryTemplate {
        val retryTemplate = RetryTemplate()
        retryTemplate.setRetryPolicy(getRetryPolicy())
        retryTemplate.setBackOffPolicy(getBackOffPolicy())
        return retryTemplate
    }

    @Bean
    fun kafkaListenerContainerFactory(consumerFactory: ConsumerFactory<String, String>): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()

        factory.setConsumerFactory(consumerFactory)
        factory.setRetryTemplate(getRetryTemplate())
        factory.setConcurrency(10)

        return factory
    }
}