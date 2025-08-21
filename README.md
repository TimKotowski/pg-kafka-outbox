Postgres transactional outbox pattern.

`pg-kafka-outbox` ability to read messages actomically with message ordering guarantees at a per kafka key level. 
Built for simplicity and reliability in mind. `pg-kafka-outbox` is a cheap spooling
that will scrape messages and send them out to to consumers to handle the messages to be sent too kafka.

Allows higher level control on the process of handling messages to kafka, leaving only the reliability and atomoicity to
the outbox pattern. Supports arbitrary binary data. Can be configured to consume messages by Kafka key, allowing either exactly once 
per key ackowledgment or multiple messages with the same key.

This Readme is currently under active development. 
