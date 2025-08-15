Postgres transactional outbox pattern.

`pg-kafka-outbox` ability to read messages actomically with message ordering guarantees at a per kafka key level.
Built for simplicity and reliability in mind. `pg-kafka-outbox` is a cheap spooling
that will scrape messages and send them out to to consumers to handle the messages progamaticely too kafka.

Idea is to give user control on the process of handling messages to kafka, leaving only the reliability and atomoicity to
the outbox pattern. Supports arbitrary binary data.


This Readme is currently under active development. 
