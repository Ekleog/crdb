use crdb_core::{EventId, ObjectId, TypeId, Updatedness, User};
use ulid::Ulid;

pub const fn ulid(s: &str) -> Ulid {
    match Ulid::from_string(s) {
        Ok(r) => r,
        Err(_) => panic!("const fn failed"),
    }
}

pub const OBJECT_ID_NULL: ObjectId = ObjectId(ulid("00000000000000000000000000"));
pub const OBJECT_ID_1: ObjectId = ObjectId(ulid("10000000000000000000000010"));
pub const OBJECT_ID_2: ObjectId = ObjectId(ulid("10000000000000000000000020"));
pub const OBJECT_ID_3: ObjectId = ObjectId(ulid("10000000000000000000000030"));
pub const OBJECT_ID_4: ObjectId = ObjectId(ulid("10000000000000000000000040"));
pub const OBJECT_ID_5: ObjectId = ObjectId(ulid("10000000000000000000000050"));
pub const OBJECT_ID_6: ObjectId = ObjectId(ulid("10000000000000000000000060"));

pub const EVENT_ID_NULL: EventId = EventId(ulid("00000000000000000000000000"));
pub const EVENT_ID_1: EventId = EventId(ulid("20000000000000000000000010"));
pub const EVENT_ID_2: EventId = EventId(ulid("20000000000000000000000020"));
pub const EVENT_ID_3: EventId = EventId(ulid("20000000000000000000000030"));
pub const EVENT_ID_4: EventId = EventId(ulid("20000000000000000000000040"));
pub const EVENT_ID_5: EventId = EventId(ulid("20000000000000000000000050"));
pub const EVENT_ID_MAX: EventId = EventId(Ulid::from_parts(u64::MAX, u128::MAX));

pub const TYPE_ID_NULL: TypeId = TypeId(ulid("00000000000000000000000000"));
pub const TYPE_ID_1: TypeId = TypeId(ulid("30000000000000000000000010"));
pub const TYPE_ID_2: TypeId = TypeId(ulid("30000000000000000000000020"));
pub const TYPE_ID_3: TypeId = TypeId(ulid("30000000000000000000000030"));
pub const TYPE_ID_4: TypeId = TypeId(ulid("30000000000000000000000040"));
pub const TYPE_ID_5: TypeId = TypeId(ulid("30000000000000000000000050"));

pub const USER_ID_NULL: User = User(ulid("00000000000000000000000000"));
pub const USER_ID_1: User = User(ulid("20000000000000000000000010"));
pub const USER_ID_2: User = User(ulid("20000000000000000000000020"));
pub const USER_ID_3: User = User(ulid("20000000000000000000000030"));
pub const USER_ID_4: User = User(ulid("20000000000000000000000040"));
pub const USER_ID_5: User = User(ulid("20000000000000000000000050"));

pub const UPDATEDNESS_NULL: Updatedness = Updatedness(ulid("00000000000000000000000000"));
pub const UPDATEDNESS_1: Updatedness = Updatedness(ulid("50000000000000000000000010"));
pub const UPDATEDNESS_2: Updatedness = Updatedness(ulid("50000000000000000000000020"));
pub const UPDATEDNESS_3: Updatedness = Updatedness(ulid("50000000000000000000000030"));
pub const UPDATEDNESS_4: Updatedness = Updatedness(ulid("50000000000000000000000040"));
pub const UPDATEDNESS_5: Updatedness = Updatedness(ulid("50000000000000000000000050"));
