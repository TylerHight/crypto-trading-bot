CREATE TABLE IF NOT EXISTS paper_pilots (
    pilot_id char(64) PRIMARY KEY CHECK (pilot_id ~ '^[a-f0-9]{64}$'),
    session_id char(64) NOT NULL UNIQUE REFERENCES paper_sessions(session_id),
    state text NOT NULL CHECK (
        state IN ('registered', 'running', 'completed', 'failed', 'inconclusive', 'cancelled')
    ),
    plan jsonb NOT NULL,
    raw_plan_sha256 char(64) NOT NULL CHECK (raw_plan_sha256 ~ '^[a-f0-9]{64}$'),
    canonical_plan_sha256 char(64) NOT NULL CHECK (canonical_plan_sha256 ~ '^[a-f0-9]{64}$'),
    approved_by varchar(128) NOT NULL,
    approval_note varchar(1000) NOT NULL,
    local_development boolean NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    finalized_at timestamptz,
    assessment jsonb
);

CREATE TABLE IF NOT EXISTS paper_pilot_commands (
    command_id varchar(128) PRIMARY KEY,
    pilot_id char(64) NOT NULL REFERENCES paper_pilots(pilot_id),
    action text NOT NULL,
    payload_digest char(64) NOT NULL CHECK (payload_digest ~ '^[a-f0-9]{64}$'),
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_pilot_cycles (
    pilot_id char(64) NOT NULL REFERENCES paper_pilots(pilot_id),
    command_id varchar(128) NOT NULL UNIQUE,
    payload_digest char(64) NOT NULL CHECK (payload_digest ~ '^[a-f0-9]{64}$'),
    manifest_uri text NOT NULL,
    manifest_sha256 char(64) NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    success boolean NOT NULL,
    result jsonb NOT NULL,
    PRIMARY KEY (pilot_id, command_id)
);

CREATE TABLE IF NOT EXISTS paper_pilot_snapshots (
    pilot_id char(64) NOT NULL REFERENCES paper_pilots(pilot_id),
    event_date date NOT NULL,
    as_of timestamptz NOT NULL,
    artifact_uri text NOT NULL,
    artifact_sha256 char(64) NOT NULL CHECK (artifact_sha256 ~ '^[a-f0-9]{64}$'),
    manifest_uri text NOT NULL,
    manifest_sha256 char(64) NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
    document jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (pilot_id, event_date)
);

CREATE INDEX IF NOT EXISTS paper_pilot_cycles_pilot_time_idx
    ON paper_pilot_cycles (pilot_id, completed_at);

INSERT INTO paper_schema_migrations (version)
VALUES ('002_paper_pilots')
ON CONFLICT (version) DO NOTHING;
