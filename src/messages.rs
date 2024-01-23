#![allow(dead_code)] // TODO(api): remove

use crate::{EventId, ObjectId, Query, Timestamp, TypeId};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};
use ulid::Ulid;

#[derive(serde::Deserialize, serde::Serialize)]
pub struct ClientMessage {
    request_id: RequestId,
    request: Request,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct RequestId(Ulid);

#[derive(serde::Deserialize, serde::Serialize)]
pub enum Request {
    GetTime,
    Get {
        object_ids: HashSet<ObjectId>,
        if_updated_since: Option<Timestamp>,
        subscribe: bool,
    },
    Query {
        query: Query,
        subscribe: bool,
    },
    Unsubscribe(HashSet<ObjectId>),
    UnsubscribeQuery(Query),
    Upload(Vec<UploadOrBinary>),
}

#[derive(serde::Deserialize, serde::Serialize)]
pub enum UploadOrBinary {
    Upload(Upload),
    Binary(Arc<Vec<u8>>),
}

#[derive(serde::Deserialize, serde::Serialize)]
pub enum Upload {
    Object {
        object_id: ObjectId,
        type_id: TypeId,
        created_at: EventId,
        data: serde_json::Value,
    },
    Event {
        event_id: EventId,
        type_id: TypeId,
        object_id: ObjectId,
        data: serde_json::Value,
    },
}

/// One ServerMessage is supposed to hold as much data as possible
/// without delaying updates, but still avoiding going too far above
/// than 1M / message, to allow for better resumability.
///
/// If the `last_response` field is set to `true`, then it means that
/// all the previous `ServerMessage`s that answered this `request`,
/// taken together, hold the answer to the request.
///
/// Any subsequent updates, obtained by subscribing to the object or
/// query, will be pushed as `Update`s.
#[derive(serde::Deserialize, serde::Serialize)]
pub enum ServerMessage {
    Response {
        request: RequestId,
        response: ResponsePart,
        last_response: bool,
    },
    Update {
        updates: Vec<Update>,
        now_have_all_until: Timestamp,
    },
}

#[derive(serde::Deserialize, serde::Serialize)]
pub enum ResponsePart {
    Success,
    Error(crate::SerializableError),
    CurrentTime(Timestamp),
    Objects(Vec<ApiObject>),
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct ApiObject {
    id: ObjectId,
    created_at: EventId,
    type_id: TypeId,
    creation_snapshot: serde_json::Value,
    events: BTreeMap<EventId, serde_json::Value>,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub enum Update {
    Object {
        object_id: ObjectId,
        type_id: TypeId,
        created_at: EventId,
        data: serde_json::Value,
    },
    Event {
        event_id: EventId,
        type_id: TypeId,
        object_id: ObjectId,
        data: serde_json::Value,
    },
    Recreation {
        type_id: TypeId,
        object_id: ObjectId,
        time: Timestamp,
    },
    Binary(Arc<Vec<u8>>),
}
