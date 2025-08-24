CREATE TYPE status AS ENUM (
'PENDING',
'RUNNING',
'SUCCEEDED',
'FAILED',
);

CREATE TABLE IF NOT EXISTS outbox (
  job_id varchar(26),
  topic text not null,
  key bytea,
  payload bytea,
  partition int,
  headers bytea,
  status status NOT NULL,
  retries int,
  max_retries int,
  created_at timestamptz not null,
  started_at timestamptz not null,

  constraint pk_outbox_jobs primary key(job_id)
);

CREATE TABLE IF NOT EXISTS dead_letter_outbox_messages (
  job_id varchar(26),
  topic text not null,
  key bytea,
  payload bytea,
  partition int,
  headers bytea,
  retries int not null,
  max_retries int not null,
  status status NOT NULL,
  created_at timestamptz not null,
  started_at timestamptz not null,
  started_by varchar NOT NULL,

  constraint pk_dead_letter_jobs primary key(job_id)
);

CREATE INDEX job_status_composite_idx on jobs(status);
