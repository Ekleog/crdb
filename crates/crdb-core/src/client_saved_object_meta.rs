use crate::{Importance, ObjectId, TypeId, Updatedness};

pub struct ClientSavedObjectMeta {
    pub type_id: TypeId,
    pub object_id: ObjectId,
    pub have_all_until: Option<Updatedness>,
    pub importance: Importance,
}
