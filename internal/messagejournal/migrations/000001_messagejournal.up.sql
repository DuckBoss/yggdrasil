DROP TABLE IF EXISTS runtime;
CREATE TABLE IF NOT EXISTS runtime (
    id INTEGER NOT NULL PRIMARY KEY,
    message_id VARCHAR(36) NOT NULL,
    sent DATETIME NOT NULL,
    worker_name VARCHAR(128) NOT NULL,
    response_to VARCHAR(36),
    worker_event INTEGER,
    worker_message TEXT
);
CREATE TABLE IF NOT EXISTS persistent (
    id INTEGER NOT NULL PRIMARY KEY,
    message_id VARCHAR(36) NOT NULL,
    sent DATETIME NOT NULL,
    worker_name VARCHAR(128) NOT NULL,
    response_to VARCHAR(36),
    worker_event INTEGER,
    worker_message TEXT
);