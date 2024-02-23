use crate::BinPtr;
use ulid::Ulid;

pub fn hash_binary(data: &[u8]) -> BinPtr {
    use sha3::Digest;
    let mut hasher = sha3::Sha3_224::new();
    hasher.update(data);
    BinPtr(Ulid::from_bytes(
        hasher.finalize()[..16].try_into().unwrap(),
    ))
}
