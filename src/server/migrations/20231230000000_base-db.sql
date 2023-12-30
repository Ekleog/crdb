CREATE TABLE sessions (
    session_id UUID  PRIMARY KEY NOT NULL,
    user_id UUID NOT NULL,
    name VARCHAR NOT NULL,
    login_time TIMESTAMP NOT NULL,
    last_active TIMESTAMP NOT NULL
);

CREATE TABLE binaries (
    id UUID PRIMARY KEY NOT NULL,
    data BYTEA NOT NULL
);

CREATE TABLE snapshots (
    snapshot_id UUID PRIMARY KEY NOT NULL, -- the ULID, cast to an UUID
    object_id UUID NOT NULL,
    is_creation BOOLEAN NOT NULL,
    is_latest BOOLEAN NOT NULL,
    snapshot_version INTEGER NOT NULL,
    snapshot JSONB NOT NULL,
    users_who_can_read UUID ARRAY NOT NULL,
    is_heavy BOOLEAN NOT NULL,
    required_binaries UUID ARRAY NOT NULL
);

CREATE TABLE events (
    event_id UUID PRIMARY KEY NOT NULL, -- the ULID, cast to an UUID
    object_id UUID NOT NULL,
    data JSONB NOT NULL,
    required_binaries UUID ARRAY NOT NULL
);
