use crate::{backend_api, JsonSnapshot, ObjectId, User};

use super::private;

pub trait ObjectPermissions: private::Sealed {
    fn users_who_can_read<'a, C: backend_api::ObjectGet>(
        // type_id: part of JsonSnapshot
        object_id: ObjectId,
        object: JsonSnapshot,
        cb: &'a C,
    ) -> impl 'a + waaaa::Future<Output = crate::Result<crate::UsersWhoCanRead<'a>>>;

    fn can_create<'a, C: backend_api::ObjectGet>(
        user: User,
        // type_id: part of JsonSnapshot
        object_id: ObjectId,
        object: JsonSnapshot,
    ) -> impl 'a + waaaa::Future<Output = crate::Result<bool>>;

    fn can_submit<'a, C: backend_api::ObjectGet>(
        user: User,
        // type_id: part of JsonSnapshot
        object_id: ObjectId,
        object: JsonSnapshot,
        event: serde_json::Value,
    ) -> impl 'a + waaaa::Future<Output = crate::Result<bool>>;
}
