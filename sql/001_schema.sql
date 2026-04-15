CREATE TABLE IF NOT EXISTS messages (
    ts BIGINT,
    chat_id BIGINT,
    msg_id BIGINT,
    phone TEXT,
    tag TEXT,
    confidence FLOAT,
    sender TEXT,
    text TEXT,
    live SMALLINT,
    PRIMARY KEY (chat_id, msg_id)
);

CREATE INDEX IF NOT EXISTS idx_ts ON messages (ts DESC);
CREATE INDEX IF NOT EXISTS idx_phone ON messages (phone);
