CREATE TABLE sessions (
    session_token UUID PRIMARY KEY NOT NULL, -- The actual session token, used for auth
    session_ref UUID UNIQUE NOT NULL, -- An UUID used to refer to the session by other sessions
    user_id UUID NOT NULL,
    name VARCHAR NOT NULL,
    login_time BIGINT NOT NULL, -- Milliseconds from unix epoch
    last_active BIGINT NOT NULL, -- Milliseconds from unix epoch. Note that for performance reasons this is not necessarily fully precise
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
    normalizer_version INTEGER NOT NULL,
    snapshot_version INTEGER NOT NULL,
    snapshot JSONB NOT NULL,
    -- The three below are nullable, and set to a correct value only for is_latest snapshots
    users_who_can_read UUID ARRAY,
    users_who_can_read_depends_on UUID ARRAY, -- List of all the other objects on which `users_who_can_read` depends
    reverse_dependents_to_update UUID ARRAY, -- List of reverse dependents that have not had their permissions updated yet
    required_binaries UUID ARRAY NOT NULL,
    last_modified UUID NOT NULL -- `Updatedness` at which this snapshot was written on the server (so, its last update), including vacuum's no-op recreations
);

CREATE TABLE events (
    event_id UUID PRIMARY KEY NOT NULL, -- the timestamp ULID, cast to an UUID
    object_id UUID NOT NULL,
    data JSONB NOT NULL,
    required_binaries UUID ARRAY NOT NULL,
    last_modified UUID NOT NULL -- `Updatedness` at which this event was written on the server
);

CREATE UNIQUE INDEX snapshot_creations ON snapshots (object_id) WHERE is_creation;
CREATE UNIQUE INDEX snapshot_latests ON snapshots (object_id) WHERE is_latest;
