bitflags::bitflags! {
    #[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
    #[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
    pub struct Importance: u8 {
        const NONE = 0;

        /// Request that the server always send updates to this object/query
        ///
        /// If `false`, this request will only be answered once.
        const SUBSCRIBE = 0b01;

        /// Keep the object/query locally across vacuums
        ///
        /// If `false`, the object/query will be dropped after the next vacuum.
        const LOCK = 0b10;
    }
}

impl Importance {
    pub fn lock(&self) -> bool {
        self.contains(Importance::LOCK)
    }

    pub fn subscribe(&self) -> bool {
        self.contains(Importance::SUBSCRIBE)
    }
}
