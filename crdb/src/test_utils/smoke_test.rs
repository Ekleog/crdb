#[cfg(feature = "_tests")]
#[macro_export]
macro_rules! smoke_test {
    (
        db: $db:ident,
        vacuum: $vacuum:expr,
        query_all: $query_all:expr,
        test_remove: $test_remove:expr,
    ) => {
        use std::sync::Arc;
        use $crate::{
            crdb_internal::{test_utils::*, Db, Lock},
            Query, Updatedness,
        };

        let mut updatedness = Updatedness::from_u128(1);
        let mut next_updatedness = move || {
            updatedness = Updatedness(updatedness.0.increment().unwrap());
            Some(updatedness)
        };

        $db.create(
            OBJECT_ID_1,
            EVENT_ID_1,
            Arc::new(TestObjectSimple::stub_1()),
            next_updatedness(),
            Lock::OBJECT,
        )
        .await
        .expect("creating test object 1 failed");
        $db.assert_invariants_generic().await;
        $db.assert_invariants_for::<TestObjectSimple>().await;
        $db.create(
            OBJECT_ID_1,
            EVENT_ID_2,
            Arc::new(TestObjectSimple::stub_2()),
            next_updatedness(),
            Lock::OBJECT,
        )
        .await
        .expect_err("creating duplicate test object 1 spuriously worked");
        $db.assert_invariants_generic().await;
        $db.assert_invariants_for::<TestObjectSimple>().await;
        $db.create(
            OBJECT_ID_1,
            EVENT_ID_1,
            Arc::new(TestObjectSimple::stub_1()),
            next_updatedness(),
            Lock::OBJECT,
        )
        .await
        .expect("creating exact copy test object 1 failed");
        $db.assert_invariants_generic().await;
        $db.assert_invariants_for::<TestObjectSimple>().await;
        $db.submit::<TestObjectSimple>(
            OBJECT_ID_1,
            EVENT_ID_3,
            Arc::new(TestEventSimple::Clear),
            next_updatedness(),
            Lock::NONE,
        )
        .await
        .expect("clearing object 1 failed");
        $db.assert_invariants_generic().await;
        $db.assert_invariants_for::<TestObjectSimple>().await;
        $db.submit::<TestObjectSimple>(
            OBJECT_ID_1,
            EVENT_ID_3,
            Arc::new(TestEventSimple::Clear),
            next_updatedness(),
            Lock::OBJECT,
        )
        .await
        .expect("submitting duplicate event failed");
        $db.assert_invariants_generic().await;
        $db.assert_invariants_for::<TestObjectSimple>().await;
        $db.submit::<TestObjectSimple>(
            OBJECT_ID_1,
            EVENT_ID_3,
            Arc::new(TestEventSimple::Set(b"foo".to_vec())),
            next_updatedness(),
            Lock::OBJECT,
        )
        .await
        .expect_err("submitting duplicate event with different contents worked");
        $db.assert_invariants_generic().await;
        $db.assert_invariants_for::<TestObjectSimple>().await;
        assert_eq!(
            Vec::<u8>::new(),
            $db.get_latest::<TestObjectSimple>(Lock::NONE, OBJECT_ID_1)
                .await
                .expect("getting object 1")
                .0
        );
        $db.submit::<TestObjectSimple>(
            OBJECT_ID_1,
            EVENT_ID_2,
            Arc::new(TestEventSimple::Set(b"bar".to_vec())),
            next_updatedness(),
            Lock::NONE,
        )
        .await
        .expect("submitting event failed");
        $db.assert_invariants_generic().await;
        $db.assert_invariants_for::<TestObjectSimple>().await;
        assert_eq!(
            Vec::<u8>::new(),
            $db.get_latest::<TestObjectSimple>(Lock::OBJECT, OBJECT_ID_1)
                .await
                .expect("getting object 1")
                .0
        );
        $db.submit::<TestObjectSimple>(
            OBJECT_ID_1,
            EVENT_ID_4,
            Arc::new(TestEventSimple::Set(b"baz".to_vec())),
            next_updatedness(),
            Lock::OBJECT,
        )
        .await
        .expect("submitting event failed");
        $db.assert_invariants_generic().await;
        $db.assert_invariants_for::<TestObjectSimple>().await;
        assert_eq!(
            b"baz".to_vec(),
            $db.get_latest::<TestObjectSimple>(Lock::NONE, OBJECT_ID_1)
                .await
                .expect("getting object 1")
                .0
        );
        $vacuum.await.unwrap();
        $db.assert_invariants_generic().await;
        $db.assert_invariants_for::<TestObjectSimple>().await;
        let all_objects = $query_all;
        assert_eq!(all_objects.len(), 1);
        if $test_remove {
            $db.remove(OBJECT_ID_1).await.unwrap();
            $db.assert_invariants_generic().await;
            $db.assert_invariants_for::<TestObjectSimple>().await;
            let all_objects = $query_all;
            assert_eq!(all_objects.len(), 0);
        }
        let data = Arc::new([1u8, 2, 3]) as Arc<[u8]>;
        let ptr = $crate::hash_binary(&data);
        assert!($db.get_binary(ptr).await.unwrap().is_none());
        $db.create_binary(ptr, data.clone()).await.unwrap();
        $db.assert_invariants_generic().await;
        $db.assert_invariants_for::<TestObjectSimple>().await;
        assert!($db.get_binary(ptr).await.unwrap().unwrap() == data);
    };
}
