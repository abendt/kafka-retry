configure Spring Boot/Spring Kafka to use a Retry template in case of
exceptions during message processing

* we don't want to ack any message before all previous messages are ack'ed
* duplicate deliveries are ok