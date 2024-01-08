CREATE TABLE sessions (
    session_token UUID PRIMARY KEY NOT NULL, -- The actual session token, used for auth
    session_ref UUID UNIQUE NOT NULL, -- An UUID used to refer to the session by other sessions
    user_id UUID NOT NULL,
    name VARCHAR NOT NULL,
    login_time BIGINT NOT NULL, -- Milliseconds from unix epoch
    last_active BIGINT NOT NULL, -- Milliseconds from unix epoch. Note that for performance reasons this is precise ~only to the minute
    expiration_time BIGINT -- Milliseconds from unix epoch, NULL for non-expiring tokens
);

CREATE TABLE binaries (
    binary_id UUID PRIMARY KEY NOT NULL,
    data BYTEA NOT NULL
);

CREATE TABLE snapshots (
    snapshot_id UUID PRIMARY KEY NOT NULL, -- the timestamp ULID, cast to an UUID
    type_id UUID NOT NULL,
    object_id UUID NOT NULL,
    is_creation BOOLEAN NOT NULL,
    is_latest BOOLEAN NOT NULL,
    snapshot_version INTEGER NOT NULL,
    snapshot JSONB NOT NULL,
    -- The three below are nullable, and set to a correct value only for is_latest snapshots
    users_who_can_read UUID ARRAY,
    users_who_can_read_depends_on UUID ARRAY, -- List of all the other objects on which `users_who_can_read` depends
    reverse_dependents_to_update UUID ARRAY, -- List of reverse dependents that have not had their permissions updated yet
    is_heavy BOOLEAN NOT NULL,
    required_binaries UUID ARRAY NOT NULL
);

CREATE TABLE events (
    event_id UUID PRIMARY KEY NOT NULL, -- the timestamp ULID, cast to an UUID
    object_id UUID NOT NULL,
    data JSONB NOT NULL,
    required_binaries UUID ARRAY NOT NULL
);

CREATE UNIQUE INDEX snapshot_creations ON snapshots (object_id) WHERE is_creation;
CREATE UNIQUE INDEX snapshot_latests ON snapshots (object_id) WHERE is_latest;
