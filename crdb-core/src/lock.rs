bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct Lock: u8 {
        const NONE = 0;
        const OBJECT = 0b01;
        const FOR_QUERIES = 0b10;
    }
}

impl Lock {
    pub fn from_query_lock(b: bool) -> Lock {
        match b {
            true => Lock::FOR_QUERIES,
            false => Lock::NONE,
        }
    }
}
