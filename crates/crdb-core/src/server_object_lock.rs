use crate::ObjectId;

pub struct ServerObjectLock<'a> {
    pub reord: reord::Lock,
    pub lockable: <lockable::LockPool<ObjectId> as lockable::Lockable<ObjectId, ()>>::Guard<'a>,
}
