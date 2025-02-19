use super::private;

pub trait UlidChecker: private::Sealed {
    /// Panics if there are two types with the same ULID configured
    fn check_ulids();
}
