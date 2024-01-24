use crate::{DynSized, EventId, ObjectId, Timestamp, TypeId};
use std::sync::Arc;

#[derive(Clone)]
pub struct DynNewObject {
    pub type_id: TypeId,
    pub id: ObjectId,
    pub created_at: EventId,
    pub object: Arc<dyn DynSized>,
}

#[derive(Clone)]
pub struct DynNewEvent {
    pub type_id: TypeId,
    pub object_id: ObjectId,
    pub id: EventId,
    pub event: Arc<dyn DynSized>,
}

#[derive(Clone, Debug)]
pub struct DynNewRecreation {
    pub type_id: TypeId,
    pub object_id: ObjectId,
    pub time: Timestamp,
}
