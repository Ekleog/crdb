use crate::{Server, ServerVacuumSchedule};
use axum::{
    extract::{State, WebSocketUpgrade},
    routing::get,
};
use axum_test::{TestServer, TestWebSocket};
use crdb_core::{
    ClientMessage, Object, Request, RequestId, ResponsePart, ServerMessage, SessionRef,
    SessionToken, UpdateData, Updates, Upload, User,
};
use crdb_test_utils::{
    TestEventPerms, TestObjectPerms, EVENT_ID_1, EVENT_ID_2, EVENT_ID_3, OBJECT_ID_1, USER_ID_1,
    USER_ID_2,
};
use std::{str::FromStr, sync::Arc};

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(db): State<Arc<Server<crdb_test_utils::Config>>>,
) -> Result<axum::response::Response, String> {
    Ok(ws.on_upgrade(move |sock| async move { db.answer(sock).await }))
}

async fn build_test_server(db: sqlx::PgPool) -> (Arc<Server<crdb_test_utils::Config>>, TestServer) {
    let (crdb_server, upgrade_finished) = Server::new(
        crdb_test_utils::Config,
        db,
        0,
        ServerVacuumSchedule::new(
            cron::Schedule::from_str("0 0 0 1 * * *").unwrap(),
            chrono::Utc,
        ),
    )
    .await
    .unwrap();
    let errors = upgrade_finished.await.unwrap();
    assert_eq!(errors, 0);
    let crdb_server = Arc::new(crdb_server);

    let app = axum::Router::new()
        .route("/ws", get(websocket_handler))
        .with_state(crdb_server.clone());
    let axum_server = TestServer::builder().http_transport().build(app).unwrap();

    (crdb_server, axum_server)
}

async fn send_and_receive_one(
    client: &mut TestWebSocket,
    request_id: u64,
    request: Request,
) -> ResponsePart {
    client
        .send_json(&ClientMessage {
            request_id: RequestId(request_id),
            request: Arc::new(request),
        })
        .await;
    let response = client.receive_json::<ServerMessage>().await;
    match response {
        ServerMessage::Response {
            request_id: RequestId(response_id),
            response,
            last_response: true,
        } if request_id == response_id => response,
        _ => panic!("unexpected response: {response:?}"),
    }
}

async fn upload(client: &mut TestWebSocket, request_id: u64, upload: Upload) {
    let resp = send_and_receive_one(client, request_id, Request::Upload(upload)).await;
    assert!(matches!(resp, ResponsePart::Success));
}

async fn get_updates(client: &mut TestWebSocket) -> Updates {
    let response = client.receive_json::<ServerMessage>().await;
    match response {
        ServerMessage::Updates(u) => u,
        _ => panic!("expected updates, got server message: {response:?}"),
    }
}

async fn connect_user(
    crdb: &Server<crdb_test_utils::Config>,
    axum: &TestServer,
    user: User,
) -> (SessionToken, SessionRef, TestWebSocket) {
    let (token, ref_) = crdb
        .login_session(user, String::from("test session"), None)
        .await
        .unwrap();
    let mut client = axum.get_websocket("/ws").await.into_websocket().await;
    client
        .send_json(&ClientMessage {
            request_id: RequestId(0),
            request: Arc::new(Request::SetToken(token.clone())),
        })
        .await;
    assert!(matches!(
        client.receive_json::<ServerMessage>().await,
        ServerMessage::Response {
            request_id: RequestId(0),
            response: ResponsePart::Success,
            last_response: true,
        }
    ));
    (token, ref_, client)
}

#[sqlx::test]
async fn regression_losing_and_regaining_read_rights_on_subscribed_caused_deletion_instead_of_update(
    db: sqlx::PgPool,
) {
    let (crdb, axum) = build_test_server(db).await;

    let (_, _, mut client1) = connect_user(&crdb, &axum, USER_ID_1).await;
    let (_, _, mut client2) = connect_user(&crdb, &axum, USER_ID_2).await;

    upload(
        &mut client1,
        1,
        Upload::Object {
            object_id: OBJECT_ID_1,
            type_id: *TestObjectPerms::type_ulid(),
            created_at: EVENT_ID_1,
            snapshot_version: TestObjectPerms::snapshot_version(),
            object: Arc::new(serde_json::to_value(TestObjectPerms(USER_ID_1)).unwrap()),
            subscribe: true,
        },
    )
    .await;

    // skip creation update
    assert!(matches!(
        get_updates(&mut client1).await.data[0].data,
        UpdateData::Creation { .. }
    ));

    upload(
        &mut client1,
        2,
        Upload::Event {
            object_id: OBJECT_ID_1,
            type_id: *TestObjectPerms::type_ulid(),
            event_id: EVENT_ID_2,
            event: Arc::new(serde_json::to_value(TestEventPerms::Set(USER_ID_2)).unwrap()),
            subscribe: false,
        },
    )
    .await;

    assert!(matches!(
        get_updates(&mut client1).await.data[0].data,
        UpdateData::LostReadRights
    ));

    // TODO(perf-low): pushing one update to all clients on each change to an object
    // they can read is not optimal, even if the update is empty and just consists of
    // their new updatedness
    assert!(get_updates(&mut client2).await.data.is_empty());

    upload(
        &mut client2,
        1,
        Upload::Event {
            object_id: OBJECT_ID_1,
            type_id: *TestObjectPerms::type_ulid(),
            event_id: EVENT_ID_3,
            event: Arc::new(serde_json::to_value(TestEventPerms::Set(USER_ID_1)).unwrap()),
            subscribe: false,
        },
    )
    .await;

    let recover_update = get_updates(&mut client1).await.data;
    println!("got updates after LostReadRights: {recover_update:?}");
    // should have gotten a full re-creation to rebuild the object in the client's db")
    assert_eq!(recover_update[0].object_id, OBJECT_ID_1);
    assert!(matches!(
        recover_update[0].data,
        UpdateData::Creation { .. }
    ));
}
