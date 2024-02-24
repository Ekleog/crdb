use crate::{stubs::USER_ID_NULL, ulid};
use anyhow::Context;
use crdb_core::{BinPtr, CanDoCallbacks, DbPtr, Object, ObjectId, SearchableString, TypeId, User};
use std::collections::{BTreeSet, HashSet, VecDeque};

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    arbitrary::Arbitrary,
    deepsize::DeepSizeOf,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct TestObjectFull {
    pub name: SearchableString,
    pub deps: Vec<DbPtr<TestObjectFull>>,
    pub bins: Vec<BinPtr>,
    pub users: BTreeSet<User>,
}

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    arbitrary::Arbitrary,
    deepsize::DeepSizeOf,
    serde::Deserialize,
    serde::Serialize,
)]
pub enum TestEventFull {
    Rename(String),
    AddDep(DbPtr<TestObjectFull>),
    RmDep(DbPtr<TestObjectFull>),
    AddBin(BinPtr),
    RmBin(BinPtr),
    AddUser(User),
    RmUser(User),
}

impl TestObjectFull {
    pub fn standardize(&mut self, self_id: ObjectId) {
        self.deps.sort_unstable();
        // Yes this > doesn't make sense it should be < but all the regression tests were written with this so :shrug: it's for fuzzers anyway
        self.deps.retain(|d| d.to_object_id() > self_id);
    }
}

impl Object for TestObjectFull {
    type Event = TestEventFull;

    fn type_ulid() -> &'static TypeId {
        static TYPE: TypeId = TypeId(ulid("01HKKAJ5Q0WPKJ4T4VEVG358GJ"));
        &TYPE
    }

    async fn can_create<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        self_id: ObjectId,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        Ok(self
            .deps
            .last()
            .map(|d| d.to_object_id() > self_id)
            .unwrap_or(true))
    }

    async fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _self_id: ObjectId,
        _event: &'a Self::Event,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    // This cannot be an async fn for now, see https://github.com/rust-lang/rust/issues/119727
    fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        db: &'a C,
    ) -> impl 'a + waaaa::Future<Output = anyhow::Result<HashSet<User>>> {
        async move {
            let mut res = self.users.iter().copied().collect::<HashSet<_>>();
            res.insert(USER_ID_NULL);
            let mut deps_to_observe = self.deps.iter().copied().collect::<VecDeque<_>>();
            while let Some(remote) = deps_to_observe.pop_front() {
                match db.get(remote).await {
                    Err(crate::Error::ObjectDoesNotExist(o)) if o == remote.to_object_id() => (),
                    Err(e) => return Err(e).context(format!("fetching {remote:?}")),
                    Ok(r) => {
                        res.extend(r.users.iter().copied());
                        deps_to_observe.extend(r.deps.iter().copied());
                        deps_to_observe.make_contiguous().sort_unstable();
                    }
                }
            }
            Ok(res)
        }
    }

    fn apply(&mut self, self_id: DbPtr<TestObjectFull>, event: &Self::Event) {
        match event {
            TestEventFull::Rename(s) => {
                self.name = SearchableString::from(s.clone());
            }
            TestEventFull::AddDep(d) => {
                // Try to keep the vecs small while fuzzing. Also, make sure to stay a DAG.
                // Yes this > doesn't make sense it should be < but all the regression tests were written with this so :shrug: it's for fuzzers anyway
                if self.deps.len() < 4 && *d > self_id {
                    self.deps.push(*d);
                    self.deps.sort_unstable();
                }
            }
            TestEventFull::RmDep(d) => {
                self.deps.retain(|v| v != d);
            }
            TestEventFull::AddBin(b) => {
                if self.bins.len() < 4 {
                    self.bins.push(*b);
                }
            }
            TestEventFull::RmBin(b) => {
                self.bins.retain(|v| v != b);
            }
            TestEventFull::AddUser(u) => {
                if self.users.len() < 4 {
                    self.users.insert(*u);
                }
            }
            TestEventFull::RmUser(u) => {
                self.users.retain(|v| v != u);
            }
        }
    }

    fn required_binaries(&self) -> Vec<BinPtr> {
        self.bins.clone()
    }
}

impl crdb_core::Event for TestEventFull {
    fn required_binaries(&self) -> Vec<BinPtr> {
        match self {
            TestEventFull::AddBin(b) => vec![*b],
            _ => Vec::new(),
        }
    }
}
