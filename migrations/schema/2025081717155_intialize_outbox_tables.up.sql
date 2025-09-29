CREATE TYPE status AS ENUM (
'PENDING',
'RUNNING',
'COMPLETED',
'FAILED',
'PENDING_RETRY'
);

CREATE TABLE IF NOT EXISTS outbox (
  id text,
  topic text not null,
  key bytea,
  payload bytea,
  partition int,
  headers bytea,
  status status not null,
  retries int not null,
  max_retries int not null,
  group_id text not null,
  fingerprint text not null,
  created_at timestamptz not null,
  updated_at timestamptz not null,
  completed_at timestamptz,
  started_at timestamptz,

  constraint pk_outbox_jobs primary key(id)
);

CREATE TABLE IF NOT EXISTS dead_letter_outbox (
  id text,
  topic text not null,
  key bytea,
  payload bytea,
  partition int,
  headers bytea,
  status status not null,
  retries int not null,
  max_retries int not null,
  group_id text not null,
  fingerprint text not null,
  created_at timestamptz not null,
  updated_at timestamptz not null,
  completed_at timestamptz,
  started_at timestamptz not null,

  constraint pk_dead_letter_outbox primary key(id)
);

CREATE INDEX outbox_fingerprint_idx on outbox(fingerprint);
CREATE INDEX outbox_partial_idx on outbox(created_at) where status IN ('PENDING', 'PENDING_RETRY');
create INDEX outbox_status_group_id_created_at_idx ON outbox (status, group_id, created_at);
