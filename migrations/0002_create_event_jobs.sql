CREATE TABLE event_jobs (
    event_id       VARCHAR(128) PRIMARY KEY
                   REFERENCES events(id) ON DELETE CASCADE,
    status         VARCHAR(20) NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'processing', 'succeeded', 'dead_letter')),
    attempts       INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts   INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    available_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_at      TIMESTAMPTZ,
    worker_id      VARCHAR(128),
    last_error     VARCHAR(2000),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at   TIMESTAMPTZ
);

CREATE INDEX idx_event_jobs_pending
    ON event_jobs (available_at, created_at)
    WHERE status = 'pending';

CREATE INDEX idx_event_jobs_expired_leases
    ON event_jobs (locked_at)
    WHERE status = 'processing';

CREATE INDEX idx_event_jobs_dead_letter
    ON event_jobs (updated_at DESC)
    WHERE status = 'dead_letter';
