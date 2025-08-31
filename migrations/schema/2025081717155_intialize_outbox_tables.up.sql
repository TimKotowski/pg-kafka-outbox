CREATE TYPE status AS ENUM (
'PENDING',
'RUNNING',
'SUCCESSFUL',
'FAILED',
'PENDING_RETRY'
);

CREATE TABLE IF NOT EXISTS outbox (
  job_id text,
  topic text not null,
  key bytea,
  payload bytea,
  partition int,
  headers bytea,
  group_id text not null,
  status status not null,
  retries int not null,
  max_retries int not null,
  fingerprint bytea not null,
  created_at timestamptz not null,
  started_at timestamptz,

  constraint pk_outbox_jobs primary key(job_id)
);

CREATE TABLE IF NOT EXISTS dead_letter_outbox (
  job_id text,
  topic text not null,
  key bytea,
  payload bytea,
  partition int,
  headers bytea,
  group_id text not null,
  status status not null,
  retries int not null,
  max_retries int not null,
  fingerprint bytea not null,
  created_at timestamptz not null,
  started_at timestamptz not null,

  constraint pk_dead_letter_outbox primary key(job_id)
);

CREATE INDEX outbox_status_idx on outbox(status);
CREATE INDEX outbox_fingerprint_idx on outbox(fingerprint);
CREATE INDEX outbox_group_id_idx on outbox(group_id);
CREATE INDEX outbox_partial_idx on outbox(created_at) where status IN ('PENDING', 'PENDING_RETRY');
