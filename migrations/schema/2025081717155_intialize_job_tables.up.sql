CREATE TYPE status AS ENUM ('SCHEDULED', 'RUNNING', 'FAILED');

CREATE TABLE IF NOT EXISTS jobs (
  job_id varchar(26),
  topic text not null,
  key bytea,
  payload bytea,
  partition int,
  headers bytea,
  status status NOT NULL DEFAULT 'SCHEDULED',
  retries int,
  max_retries int,
  priority timestamptz not null,
  created_at timestamptz not null,
  started_by varchar NOT NULL,
  started_at timestamptz not null,

  constraint pk_outbox_jobs primary key(job_id)
);

CREATE TABLE IF NOT EXISTS dead_letter_jobs (
  job_id varchar(26),
  topic text not null,
  key bytea,
  payload bytea,
  partition int,
  headers bytea,
  retries int,
  status status NOT NULL,
  priority timestamptz not null,
  created_at timestamptz not null,
  started_by varchar NOT NULL,
  started_at timestamptz not null,

  constraint pk_dead_letter_jobs primary key(job_id)
);

CREATE INDEX job_look_up_idx on jobs(priority, status)
