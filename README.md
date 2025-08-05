Postgres transactional outbox pattern. Allowing ability to read messages actomically with message ordering guarantees.
Make sure messages getting read happen at a per key guarantee. Built for simplicity and reliability in mind. `pg-kafka-outbox` is a cheap spooling 
that will scrape messages and send them out to to consumers to handle the messages progamaticely tolo kafka. 
