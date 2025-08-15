CREATE TYPE status AS ENUM ('SCHEDULED', 'RUNNING' 'FAILED');

CREATE TABLE IF NOT EXISTS outbox_jobs (
  id bigint,
  topic text not null,
  key bytea,
  payload bytea,
  partition int,
  status status NOT NULL DEFAULT 'SCHEDULED',
  retries int,
  max_retries int,
  priority timestampz not null,
  created_at timestampz not null,
  started_by varchar NOT NULL,
  started_at timestampz not null,

  constraint pk_outbox_jobs primary key(id)
);

CREATE TABLE IF NOT EXISTS outbox_dead_letter_jobs (
  id  bigint, 
  topic text not null,
  key bytea,
  payload bytea,
  partition int,
  retries int,
  status status NOT NULL,
  priority timestampz not null,
  created_at timestampz not null,
  started_by varchar NOT NULL,
  started_at timestampz not null,

  constraint pk_outbox_dead_letter_jobs primary key(id)
);

CREATE INDEX job_look_up_idx on outbox_jobs(priority, status)
