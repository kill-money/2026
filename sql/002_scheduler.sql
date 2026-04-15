CREATE TABLE IF NOT EXISTS sessions (
    id SERIAL PRIMARY KEY,
    session_name TEXT UNIQUE NOT NULL,
    api_id BIGINT NOT NULL,
    api_hash TEXT NOT NULL,
    phone TEXT NOT NULL,
    status TEXT DEFAULT 'idle',
    last_heartbeat TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS channel_assignments (
    chat_id BIGINT NOT NULL,
    session_id INTEGER REFERENCES sessions(id),
    assigned_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (chat_id)
);

CREATE INDEX IF NOT EXISTS idx_session_status ON sessions(status);
