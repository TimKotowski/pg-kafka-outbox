Postgres transactional outbox pattern. 

`pg-kafka-outbox` ability to read messages actomically with message ordering guarantees at a per kafka key level. 
Built for simplicity and reliability in mind. `pg-kafka-outbox` is a cheap spooling 
that will scrape messages and send them out to to consumers to handle the messages progamaticely tolo kafka. 
