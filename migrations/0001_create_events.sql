CREATE TABLE events (
    id              VARCHAR(128) PRIMARY KEY,
    event_type      VARCHAR(128) NOT NULL,
    schema_version  INTEGER NOT NULL CHECK (schema_version = 1),
    occurred_at     TIMESTAMPTZ NOT NULL,
    payload         JSONB NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT events_id_not_blank CHECK (length(id) > 0),
    CONSTRAINT events_type_not_blank CHECK (length(trim(event_type)) > 0)
);

CREATE INDEX idx_events_event_type_received_at
    ON events (event_type, received_at DESC);
