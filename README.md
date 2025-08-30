`pg-kafka-outbox` is a high-performance transactional outbox processing system for Go and Postgres.

Built for simplicity and reliability in mind. `pg-kafka-outbox` is a cheap spooling
that will scrape messages and send them out to consumers to handle message sending to kafka.
Allows higher level control on the process of handling messages to kafka, leaving only the reliability and atomoicity to
the outbox pattern. Supports arbitrary binary data. Can be configured to 

1. Supports FIFO (first-in-first-out) processing of messages from a per kafka key level, where ordering is critical.
2. Supports dedupliction of messages for exactly-once processing of messages
3. Supports high throughput of messages, by leveraging the ability to spawn as many consumers as needed.
Each consumer is isolated, supporting less message congestion through one channel. Paired with NON FIFO processing of messages,
where ordering isn't critical.
   
