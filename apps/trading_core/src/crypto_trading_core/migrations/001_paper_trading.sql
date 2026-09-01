CREATE TABLE IF NOT EXISTS paper_schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS paper_sessions (
    session_id char(64) PRIMARY KEY CHECK (session_id ~ '^[a-f0-9]{64}$'),
    state text NOT NULL CHECK (state IN ('active', 'paused', 'stopped')),
    spec jsonb NOT NULL,
    session_document jsonb NOT NULL,
    cash numeric(38,18) NOT NULL CHECK (cash >= 0),
    base_quantity numeric(38,18) NOT NULL CHECK (base_quantity >= 0),
    current_equity numeric(38,18) CHECK (current_equity >= 0),
    peak_equity numeric(38,18) NOT NULL CHECK (peak_equity > 0),
    total_fees numeric(38,18) NOT NULL CHECK (total_fees >= 0),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_candle_inputs (
    session_id char(64) NOT NULL REFERENCES paper_sessions(session_id),
    window_start timestamptz NOT NULL,
    manifest_uri text NOT NULL,
    manifest_sha256 char(64) NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
    snapshot_key char(64) NOT NULL CHECK (snapshot_key ~ '^[a-f0-9]{64}$'),
    exchange text NOT NULL,
    symbol text NOT NULL,
    open numeric(38,18) NOT NULL CHECK (open > 0),
    high numeric(38,18) NOT NULL CHECK (high > 0),
    low numeric(38,18) NOT NULL CHECK (low > 0),
    close numeric(38,18) NOT NULL CHECK (close > 0),
    PRIMARY KEY (session_id, window_start),
    CHECK (low <= open AND open <= high AND low <= close AND close <= high)
);

CREATE TABLE IF NOT EXISTS paper_decisions (
    session_id char(64) NOT NULL REFERENCES paper_sessions(session_id),
    decision_time timestamptz NOT NULL,
    observed_candle_window_start timestamptz NOT NULL,
    fast_sma numeric(38,18) NOT NULL CHECK (fast_sma > 0),
    slow_sma numeric(38,18) NOT NULL CHECK (slow_sma > 0),
    previous_target text NOT NULL CHECK (previous_target IN ('FLAT', 'LONG')),
    new_target text NOT NULL CHECK (new_target IN ('FLAT', 'LONG')),
    strategy_version text NOT NULL,
    PRIMARY KEY (session_id, decision_time),
    CHECK (previous_target <> new_target)
);

CREATE TABLE IF NOT EXISTS paper_fills (
    session_id char(64) NOT NULL REFERENCES paper_sessions(session_id),
    decision_time timestamptz NOT NULL,
    fill_time timestamptz NOT NULL,
    side text NOT NULL CHECK (side IN ('BUY', 'SELL')),
    base_quantity numeric(38,18) NOT NULL CHECK (base_quantity > 0),
    reference_open_price numeric(38,18) NOT NULL CHECK (reference_open_price > 0),
    execution_price numeric(38,18) NOT NULL CHECK (execution_price > 0),
    gross_notional numeric(38,18) NOT NULL CHECK (gross_notional > 0),
    fee numeric(38,18) NOT NULL CHECK (fee >= 0),
    cash_after numeric(38,18) NOT NULL CHECK (cash_after >= 0),
    base_after numeric(38,18) NOT NULL CHECK (base_after >= 0),
    PRIMARY KEY (session_id, decision_time),
    UNIQUE (session_id, fill_time)
);

CREATE TABLE IF NOT EXISTS paper_equity (
    session_id char(64) NOT NULL REFERENCES paper_sessions(session_id),
    window_start timestamptz NOT NULL,
    close numeric(38,18) NOT NULL CHECK (close > 0),
    cash numeric(38,18) NOT NULL CHECK (cash >= 0),
    base_quantity numeric(38,18) NOT NULL CHECK (base_quantity >= 0),
    position text NOT NULL CHECK (position IN ('FLAT', 'LONG')),
    equity numeric(38,18) NOT NULL CHECK (equity >= 0),
    drawdown numeric(38,18) NOT NULL CHECK (drawdown >= 0 AND drawdown <= 1),
    PRIMARY KEY (session_id, window_start)
);

CREATE TABLE IF NOT EXISTS paper_session_events (
    event_id bigserial PRIMARY KEY,
    session_id char(64) NOT NULL REFERENCES paper_sessions(session_id),
    command_id varchar(128) NOT NULL,
    payload_digest char(64) NOT NULL CHECK (payload_digest ~ '^[a-f0-9]{64}$'),
    actor varchar(128) NOT NULL,
    action text NOT NULL,
    reason varchar(1000) NOT NULL,
    resulting_state text NOT NULL CHECK (resulting_state IN ('active', 'paused', 'stopped')),
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    UNIQUE (session_id, command_id)
);

CREATE INDEX IF NOT EXISTS paper_candle_inputs_manifest_idx
    ON paper_candle_inputs (manifest_sha256);
CREATE INDEX IF NOT EXISTS paper_session_events_session_time_idx
    ON paper_session_events (session_id, created_at);

INSERT INTO paper_schema_migrations (version)
VALUES ('001_paper_trading')
ON CONFLICT (version) DO NOTHING;
