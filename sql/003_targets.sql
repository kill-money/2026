CREATE TABLE IF NOT EXISTS channel_targets (
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT,
    username TEXT,
    invite_link TEXT,
    title TEXT,
    entity_type TEXT NOT NULL,
    source_keyword TEXT,
    status TEXT NOT NULL,
    discovered_at TIMESTAMP NOT NULL DEFAULT now(),
    discovered_by_session TEXT,
    join_attempts INT NOT NULL DEFAULT 0,
    last_join_attempt_at TIMESTAMP,
    joined_at TIMESTAMP,
    cooldown_until TIMESTAMP,
    last_error TEXT,
    UNIQUE (chat_id),
    UNIQUE (username),
    UNIQUE (invite_link)
);

CREATE INDEX IF NOT EXISTS idx_channel_targets_status ON channel_targets(status);
CREATE INDEX IF NOT EXISTS idx_channel_targets_cooldown ON channel_targets(cooldown_until);
