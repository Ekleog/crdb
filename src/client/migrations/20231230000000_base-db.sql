
-- BLOBs are ULIDs if not specified otherwise

CREATE TABLE binaries (
    binary_id BLOB PRIMARY KEY NOT NULL,
    data BLOB NOT NULL
);

CREATE TABLE snapshots (
    snapshot_id BLOB PRIMARY KEY NOT NULL,
    type_id BLOB NOT NULL,
    object_id BLOB NOT NULL,
    is_creation BOOLEAN NOT NULL,
    is_latest BOOLEAN NOT NULL,
    snapshot_version INTEGER NOT NULL,
    snapshot BLOB NOT NULL, -- JSONB
    -- Whether we need to keep the object in the database, either because the
    -- user explicitly required it, or because it has not been successfully
    -- uploaded yet. Both are meaningfully set only on the is_creation
    -- snapshot, values on other snapshots are ignored.
    is_locked BOOLEAN NOT NULL,
    upload_succeeded BOOLEAN NOT NULL
);

CREATE TABLE snapshots_binaries (
    snapshot_id BLOB NOT NULL REFERENCES snapshots (snapshot_id),
    binary_id BLOB NOT NULL REFERENCES binaries (binary_id),
    PRIMARY KEY (binary_id, snapshot_id)
);

CREATE TABLE events (
    event_id BLOB PRIMARY KEY NOT NULL,
    object_id BLOB NOT NULL,
    data BLOB NOT NULL, -- JSONB
    upload_succeeded BOOLEAN NOT NULL
);

CREATE TABLE events_binaries (
    event_id BLOB NOT NULL REFERENCES events (event_id),
    binary_id BLOB NOT NULL REFERENCES binaries (binary_id),
    PRIMARY KEY (binary_id, event_id)
);

CREATE UNIQUE INDEX snapshot_creations ON snapshots (object_id) WHERE is_creation;
CREATE UNIQUE INDEX snapshot_latests ON snapshots (object_id) WHERE is_latest;
