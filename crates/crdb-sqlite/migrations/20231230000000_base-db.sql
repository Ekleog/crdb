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
    normalizer_version INTEGER NOT NULL,
    snapshot_version INTEGER NOT NULL,
    snapshot BLOB NOT NULL, -- JSONB
    -- `Updatedness` of the event that caused this snapshot's creation,
    -- or of the last `recreate` call.
    -- Null if no event was ever received from the server for this object.
    have_all_until BLOB,
    -- The importance specific to this object. See [`Importance`].
    importance INTEGER NOT NULL,
    -- The importance of this object that comes from queries. See [`Importance`].
    importance_from_queries INTEGER NOT NULL
);

CREATE TABLE snapshots_binaries (
    snapshot_id BLOB NOT NULL REFERENCES snapshots (snapshot_id),
    binary_id BLOB NOT NULL REFERENCES binaries (binary_id),
    PRIMARY KEY (binary_id, snapshot_id)
);

CREATE TABLE events (
    event_id BLOB PRIMARY KEY NOT NULL,
    object_id BLOB NOT NULL,
    data BLOB NOT NULL -- JSONB
);

CREATE TABLE events_binaries (
    event_id BLOB NOT NULL REFERENCES events (event_id),
    binary_id BLOB NOT NULL REFERENCES binaries (binary_id),
    PRIMARY KEY (binary_id, event_id)
);

CREATE TABLE upload_queue (
    upload_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    data BLOB NOT NULL -- JSONB
);

CREATE TABLE upload_queue_binaries (
    upload_id INTEGER NOT NULL REFERENCES upload_queue (upload_id),
    binary_id BLOB NOT NULL REFERENCES binaries (binary_id),
    PRIMARY KEY (binary_id, upload_id)
);

CREATE UNIQUE INDEX snapshot_creations ON snapshots (object_id)
WHERE
    is_creation;

CREATE UNIQUE INDEX snapshot_latests ON snapshots (object_id)
WHERE
    is_latest;
