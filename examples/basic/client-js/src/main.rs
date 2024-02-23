use basic_api::{AuthInfo, Item, Tag};
use crdb::{
    fts::SearchableString, ConnectionEvent, Importance, JsonPathItem, Query, QueryId, SessionToken,
    User,
};
use futures::stream::StreamExt;
use std::{collections::BTreeSet, rc::Rc, str::FromStr, sync::Arc, time::Duration};
use ulid::Ulid;
use web_time::web::SystemTimeExt;
use yew::{prelude::*, suspense::use_future_with};

const CACHE_WATERMARK: usize = 8 * 1024 * 1024;
const VACUUM_FREQUENCY: Duration = Duration::from_secs(3600);
const WEBSOCKET_URL: &str = "ws://127.0.0.1:8080/api/ws";

fn main() {
    tracing_wasm::set_as_global_default();
    yew::set_custom_panic_hook(Box::new(|info| {
        let message = match info.location() {
            None => format!("Panic occurred at unknown place:\n"),
            Some(l) => format!(
                "Panic occurred at file '{}' line '{}'. See console for details.\n{:?}",
                l.file(),
                l.line(),
                info,
            ),
        };
        let document = web_sys::window()
            .expect("no web_sys window")
            .document()
            .expect("no web_sys document");
        document
            .get_element_by_id("body")
            .expect("no #body element")
            .set_inner_html(include_str!("../panic-page.html"));
        document
            .get_element_by_id("panic-message")
            .expect("no #panic-message element")
            .set_inner_html(&message);
        console_error_panic_hook::hook(info);
    }));
    yew::Renderer::<App>::new().render();
}

#[function_component(App)]
fn app() -> Html {
    let require_relogin = use_state(|| false);
    let logging_in = use_state(|| false);
    let connection_status = use_state(|| ConnectionEvent::LoggedOut);
    let db = use_future_with((), {
        let require_relogin = require_relogin.clone();
        let connection_status = connection_status.clone();
        move |_| async move {
            let (db, upgrade_handle) = crdb::ClientDb::connect(
                basic_api::db::ApiConfig,
                "basic-crdb",
                CACHE_WATERMARK,
                move || {
                    tracing::info!("db requested a relogin");
                    require_relogin.set(true);
                },
                |upload, err| async move {
                    panic!("failed submitting {upload:?}: {err:?}");
                },
                crdb::ClientVacuumSchedule::new(VACUUM_FREQUENCY),
            )
            .await
            .map_err(|err| format!("{err:?}"))?;
            let upgrade_errs = upgrade_handle.await;
            if upgrade_errs != 0 {
                return Err(format!("got {upgrade_errs} errors while upgrading"));
            }
            tracing::debug!("setting on_connection_event");
            db.on_connection_event(move |evt| {
                tracing::info!(?evt, "connection event");
                connection_status.set(evt);
            });
            Ok(db)
        }
    });
    if *logging_in {
        return html! {
            <h1>{ "Loading…" }</h1>
        };
    }
    let db = match db {
        Err(_) => {
            return html! { <h1>{ "Loading…" }</h1> };
        }
        Ok(db) => db.as_ref().expect("failed loading database").clone(),
    };
    if *require_relogin {
        let on_login = Callback::from({
            let require_relogin = require_relogin.clone();
            let logging_in = logging_in.clone();
            move |(user, token)| {
                require_relogin.set(false);
                logging_in.set(true);
                let db = db.clone();
                let logging_in = logging_in.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    tracing::trace!("sending login to db");
                    db.login(Arc::new(String::from(WEBSOCKET_URL)), user, token)
                        .await
                        .expect("failed logging in");
                    tracing::trace!("db acknowledged login");
                    logging_in.set(false);
                });
            }
        });
        html! {
            <Login {on_login} />
        }
    } else {
        html! {
            <Refresher {db} connection_status={Rc::new(format!("{:?}", &*connection_status))} />
        }
    }
}

#[derive(Properties)]
struct RefresherProps {
    db: Arc<crdb::ClientDb>,
    connection_status: Rc<String>,
}

impl PartialEq for RefresherProps {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.db, &other.db) && self.connection_status == other.connection_status
    }
}

#[derive(Clone)]
struct ArcEq<T>(Arc<T>);

impl<T> PartialEq for ArcEq<T> {
    fn eq(&self, other: &ArcEq<T>) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[function_component(Refresher)]
fn refresher(
    RefresherProps {
        db,
        connection_status,
    }: &RefresherProps,
) -> Html {
    // TODO(misc-high): write a crdb-yew to hide that and only refresh each individual query when required
    let counter = use_mut_ref(|| 0); // Counter used only to force a refresh of each component that uses DbContext
    let force_update = use_force_update();
    *counter.borrow_mut() += 1;
    use_effect_with(ArcEq(db.clone()), {
        let force_update = force_update.clone();
        move |ArcEq(db)| {
            wasm_bindgen_futures::spawn_local({
                let force_update = force_update.clone();
                let db = db.clone();
                async move {
                    let mut updates = db.listen_for_all_updates();
                    loop {
                        match updates.recv().await {
                            Err(crdb::broadcast::error::RecvError::Closed) => break,
                            _ => (), // ignore the contents, just refresh
                        }
                        tracing::debug!("refreshing the whole app");
                        force_update.force_update();
                    }
                }
            });
            wasm_bindgen_futures::spawn_local({
                let force_update = force_update.clone();
                let db = db.clone();
                async move {
                    // TODO(api-high): normalize watcher names: watch upload queue, listen updates, ?? connection events?
                    let mut updates = db.watch_upload_queue();
                    while updates.changed().await.is_ok() {
                        tracing::debug!("refreshing the whole app");
                        force_update.force_update();
                    }
                }
            });
        }
    });
    html! {
        <ContextProvider<DbContext> context={DbContext(db.clone(), force_update.clone(), *counter.borrow())}>
            <MainView {connection_status} />
        </ContextProvider<DbContext>>
    }
}

#[derive(Properties, PartialEq)]
struct LoginProps {
    on_login: Callback<(User, SessionToken)>,
}

fn user_to_ulid(mut user: String) -> String {
    user.make_ascii_uppercase();
    user.chars()
        .map(|c| match c {
            'I' => '1',
            'L' => '7',
            'O' => '0',
            'U' => 'V',
            c => c,
        })
        .filter(|&c| (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z'))
        .collect()
}

#[function_component(Login)]
fn login(LoginProps { on_login }: &LoginProps) -> Html {
    let username = use_state(|| String::from(""));
    let password = use_state(|| String::from(""));
    let on_user_change = {
        let username = username.clone();
        move |e: Event| {
            let input: web_sys::HtmlInputElement = e.target_unchecked_into();
            username.set(user_to_ulid(input.value()));
        }
    };
    let on_pass_change = {
        let password = password.clone();
        move |e: Event| {
            let input: web_sys::HtmlInputElement = e.target_unchecked_into();
            password.set(input.value());
        }
    };
    let onclick = {
        let on_login = on_login.clone();
        let username = username.clone();
        let password = password.clone();
        move |_| {
            let on_login = on_login.clone();
            let username = username.clone();
            let password = password.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let user = format!("{:0>26}", *username);
                let user = User(Ulid::from_str(&user).expect("username is invalid"));
                let auth_info = AuthInfo {
                    user,
                    pass: (*password).clone(),
                };
                let token = gloo_net::http::Request::post("/api/login")
                    .json(&auth_info)
                    .expect("failed serializing auth info")
                    .send()
                    .await
                    .expect("failed sending login request")
                    .json::<SessionToken>()
                    .await
                    .expect("failed deserializing login response, probably wrong password");
                on_login.emit((user, token))
            });
        }
    };
    html! {
        <>
            <h1>{ "Login" }</h1>
            <form>
                { "User: " }
                <input
                    type="text"
                    placeholder="username"
                    maxlength="26"
                    value={(*username).clone()}
                    onchange={on_user_change}
                    />
                { " Password: " }
                <input
                    type="password"
                    placeholder="password"
                    value={(*password).clone()}
                    onchange={on_pass_change}
                    />
                { " " }
                <input
                    type="button"
                    value="Login"
                    {onclick}
                    />
            </form>
        </>
    }
}

#[derive(Clone)]
struct DbContext(Arc<crdb::ClientDb>, UseForceUpdateHandle, usize);

impl PartialEq for DbContext {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) && self.2 == other.2
    }
}

fn show_user(user: User) -> String {
    let s = format!("{}", user.0);
    let i = s.find(|c| c != '0').unwrap_or_else(|| s.len() - 3);
    s[i..].to_owned()
}

#[derive(PartialEq, Properties)]
struct MainViewProps {
    connection_status: Rc<String>,
}

#[function_component(MainView)]
fn main_view(MainViewProps { connection_status }: &MainViewProps) -> Html {
    let db = use_context::<DbContext>().unwrap().0;
    let logout = {
        let db = db.clone();
        Callback::from(move |_| {
            let db = db.clone();
            wasm_bindgen_futures::spawn_local(async move {
                db.logout().await.expect("failed logging out");
            });
        })
    };
    html! {<>
        <h1>
            { format!("Logged in as {}: {connection_status} ", show_user(db.user().unwrap())) }
            <input
                type="button"
                value="Logout"
                onclick={logout}
                />
        </h1>
        <div style="position: relative">
            <div style="height: 100%; width: 55%; position: absolute; top: 0; left: 0">
                <CreateTag /><br />
                <QueryRemoteTags /><br />
                <hr />
                <CreateItem /><br />
                <QueryRemoteItems /><br />
            </div>
            <div style="height: 100%; width: 45%; position: absolute; top: 0; right: 0">
                <ShowSessionList /><hr />
                <ShowUploadQueue /><hr />
                <ShowSubscribedQueries /><hr />
                <ShowLocalDb /><hr />
            </div>
        </div>
    </>}
}

fn create<T: crdb::Object>(
    db: Arc<crdb::ClientDb>,
    text: UseStateHandle<String>,
    name: &'static str,
    creator: impl 'static + Fn(User, &str) -> T,
) -> Html {
    let onchange = {
        let text = text.clone();
        move |e: Event| {
            let input: web_sys::HtmlInputElement = e.target_unchecked_into();
            text.set(input.value());
        }
    };
    let create_it = Callback::from({
        let text = text.clone();
        move |importance| {
            let object = creator(db.user().unwrap(), &*text);
            let db = db.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let _ = db
                    .create(importance, Arc::new(object))
                    .await
                    .expect("failed creating item");
            })
        }
    });
    html! {<>
        { format!("Create {name}: ") }
        <input
            type="text"
            placeholder="text"
            value={(*text).clone()}
            {onchange} />
        <input
            type="button"
            value={ format!("Create {name}") }
            onclick={create_it.reform(|_| Importance::Latest)} />
        <input
            type="button"
            value={ format!("Create {name} & Subscribe") }
            onclick={create_it.reform(|_| Importance::Subscribe)} />
        <input
            type="button"
            value={ format!("Create {name} & Lock") }
            onclick={create_it.reform(|_| Importance::Lock)} />
    </>}
}

#[function_component(CreateItem)]
fn create_item() -> Html {
    let db = use_context::<DbContext>().unwrap().0;
    let text = use_state(|| String::new());
    create(db, text, "Item", |owner, text| Item {
        owner,
        text: SearchableString::from(text),
        tags: BTreeSet::new(),
        file: None,
    })
}

#[function_component(CreateTag)]
fn create_tag() -> Html {
    let db = use_context::<DbContext>().unwrap().0;
    let text = use_state(|| String::new());
    create(db, text, "Tag", |owner, text| {
        let mut set = BTreeSet::new();
        set.insert(owner);
        Tag {
            name: String::from(text),
            users_who_can_read: set.clone(),
            users_who_can_edit: set,
        }
    })
}

#[function_component(QueryRemoteItems)]
fn query_remote_items() -> Html {
    let db = use_context::<DbContext>().unwrap().0;
    let query = use_state(|| String::new());
    let query_res = use_state(|| Vec::new());
    let onchange = {
        let query = query.clone();
        move |e: Event| {
            let input: web_sys::HtmlInputElement = e.target_unchecked_into();
            query.set(input.value());
        }
    };
    let run_query_remote = Callback::from({
        let query = query.clone();
        let query_res = query_res.clone();
        move |importance| {
            let query = Query::ContainsStr(
                vec![JsonPathItem::Key(String::from("text"))],
                (*query).clone(),
            );
            let db = db.clone();
            let query_res = query_res.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let res = db
                    .query_remote::<basic_api::Item>(importance, QueryId::now(), Arc::new(query))
                    .await
                    .expect("failed querying item")
                    .collect::<Vec<_>>()
                    .await;
                query_res.set(res);
            })
        }
    });
    let query_results = query_res
        .iter()
        .map(|i| html! {<li><RenderItem data={Rc::new(i.as_ref().unwrap().clone())} /></li>})
        .collect::<Html>();
    html! {<>
        { "Query Remote Items: "}
        <input
            type="text"
            placeholder="text"
            value={(*query).clone()}
            {onchange} />
        <input
            type="button"
            value="Query Items"
            onclick={run_query_remote.reform(|_| Importance::Latest)} />
        <input
            type="button"
            value="Query Items & Subscribe"
            onclick={run_query_remote.reform(|_| Importance::Subscribe)} />
        <input
            type="button"
            value="Query Items & Lock"
            onclick={run_query_remote.reform(|_| Importance::Lock)} />
        <ul>
            { query_results }
        </ul>
    </>}
}

#[function_component(QueryRemoteTags)]
fn query_remote_tags() -> Html {
    let db = use_context::<DbContext>().unwrap().0;
    let query = use_state(|| String::new());
    let query_res = use_state(|| Vec::new());
    let onchange = {
        let query = query.clone();
        move |e: Event| {
            let input: web_sys::HtmlInputElement = e.target_unchecked_into();
            query.set(input.value());
        }
    };
    let run_query_remote = Callback::from({
        let query = query.clone();
        let query_res = query_res.clone();
        move |importance| {
            let query = Query::Eq(
                vec![JsonPathItem::Key(String::from("name"))],
                serde_json::Value::String((*query).clone()),
            );
            let db = db.clone();
            let query_res = query_res.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let res = db
                    .query_remote::<basic_api::Tag>(importance, QueryId::now(), Arc::new(query))
                    .await
                    .expect("failed querying tags")
                    .collect::<Vec<_>>()
                    .await;
                query_res.set(res);
            })
        }
    });
    let query_results = query_res
        .iter()
        .map(|i| html! {<li><RenderTag data={Rc::new(i.as_ref().unwrap().clone())} /></li>})
        .collect::<Html>();
    html! {<>
        { "Query Remote Tags: "}
        <input
            type="text"
            placeholder="text"
            value={(*query).clone()}
            {onchange} />
        <input
            type="button"
            value="Query Tags"
            onclick={run_query_remote.reform(|_| Importance::Latest)} />
        <input
            type="button"
            value="Query Tags & Subscribe"
            onclick={run_query_remote.reform(|_| Importance::Subscribe)} />
        <input
            type="button"
            value="Query Tags & Lock"
            onclick={run_query_remote.reform(|_| Importance::Lock)} />
        <ul>
            { query_results }
        </ul>
    </>}
}

#[function_component(ShowSessionList)]
fn show_session_list() -> Html {
    let db = use_context::<DbContext>().unwrap();
    let do_refresh = use_state(|| 0);
    // TODO(api-high): expose way to watch for session list, get rid of do_refresh
    let sessions = use_future_with((db.clone(), *do_refresh), |arg| {
        let db = arg.0 .0.clone();
        async move { db.list_sessions().await }
    });
    let current_session = use_future_with((db.clone(), *do_refresh), |arg| {
        let db = arg.0 .0.clone();
        async move { db.current_session().await }
    });
    let new_name = use_state(|| String::new());
    let on_new_name_change = {
        let new_name = new_name.clone();
        move |e: Event| {
            let input: web_sys::HtmlInputElement = e.target_unchecked_into();
            new_name.set(input.value());
        }
    };
    let rename_current_session = {
        let db = db.0.clone();
        let new_name = new_name.clone();
        let do_refresh = do_refresh.clone();
        move |_| {
            let db = db.clone();
            let new_name = new_name.clone();
            let do_refresh = do_refresh.clone();
            wasm_bindgen_futures::spawn_local(async move {
                db.rename_session((*new_name).clone())
                    .await
                    .unwrap()
                    .expect("failed renaming session");
                do_refresh.set(*do_refresh + 1);
            })
        }
    };
    let (sessions, current_session) = match (sessions, current_session) {
        (Ok(a), Ok(b)) => (a, b),
        _ => {
            return html! {
                { "Loading…" }
            }
        }
    };
    let sessions = sessions.as_ref().expect("failed listing sessions");
    let current_session = current_session
        .as_ref()
        .expect("failed getting current session");
    let sessions = sessions
        .into_iter()
        .map(|s| {
            let session_ref = s.session_ref;
            let db = db.0.clone();
            let do_refresh = do_refresh.clone();
            let onclick = move |_| {
                let db = db.clone();
                let do_refresh = do_refresh.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    db.disconnect_session(session_ref).await.unwrap().expect("failed disconnecting session");
                    do_refresh.set(*do_refresh + 1);
                })
            };
            html! {
                <li style={ (s.session_ref == current_session.session_ref).then(|| "font-weight: bold").unwrap_or("") }>
                    { format!(
                        "'{}' (logged in {}, last active {}) ",
                        s.session_name,
                        chrono::DateTime::<chrono::Utc>::from(s.login_time.to_std()).naive_utc(),
                        chrono::DateTime::<chrono::Utc>::from(s.last_active.to_std()).naive_utc(),
                    ) }
                    <input
                        type="button"
                        value="Logout"
                        { onclick } />
                </li>
            }
        })
        .collect::<Html>();
    html! {<>
        <h3>
            { "Session list " }
            <input
                type="button"
                value="Refresh"
                onclick={ move |_| do_refresh.set(*do_refresh + 1) } />
            <input
                type="text"
                placeholder={ current_session.session_name.clone() }
                value={ (*new_name).clone() }
                onchange={ on_new_name_change } />
            <input
                type="button"
                value="Rename current session"
                onclick={ rename_current_session } />
        </h3>
        <ul>
            { sessions }
        </ul>
    </>}
}

#[function_component(ShowUploadQueue)]
fn show_upload_queue() -> Html {
    let db = use_context::<DbContext>().unwrap();
    let upload_queue = use_future_with(db, |db| {
        let db = db.clone();
        async move {
            let ids =
                db.0.list_uploads()
                    .await
                    .expect("failed listing upload queue");
            let mut res = Vec::with_capacity(ids.len());
            for id in ids {
                res.push(db.0.get_upload(id).await);
            }
            Ok::<_, crdb::Error>(res)
        }
    });
    let upload_queue = match &upload_queue {
        Err(_) => return html! { <h6>{ "Loading upload queue" }</h6> },
        Ok(r) => r.as_ref().expect("failed loading upload queue"),
    };
    let upload_queue = upload_queue
        .iter()
        .map(|i| html! {<> <br />{ format!("{i:?}") } </>})
        .collect::<Html>();
    html! {<>
        <h3>{ "Upload queue" }</h3>
        { upload_queue }
    </>}
}

#[function_component(ShowSubscribedQueries)]
fn show_subscribed_queries() -> Html {
    let db = use_context::<DbContext>().unwrap();
    let do_refresh = use_force_update();
    let subscribed_queries = db.0.list_subscribed_queries();
    let subscribed_queries = subscribed_queries
        .into_iter()
        .map(|q| {
            let query_id = q.0;
            let db = db.0.clone();
            let do_refresh = do_refresh.clone();
            let onclick = move |_| {
                let db = db.clone();
                let do_refresh = do_refresh.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    db.unsubscribe_query(query_id)
                        .await
                        .expect("failed unsubscribing from query");
                    do_refresh.force_update();
                })
            };
            html! {
                <li>
                    { format!("{:?} {:?}", q.1 .0, q.1 .3) }
                    <input
                        type="button"
                        value="Unsubscribe"
                        { onclick } />
                </li>
            }
        })
        .collect::<Html>();
    html! {<>
        <h3>
            { "Subscribed Queries " }
            <input
                type="button"
                value="Refresh"
                onclick={ move |_| do_refresh.force_update() } />
        </h3>
        <ul>
            { subscribed_queries }
        </ul>
    </>}
}

#[function_component(ShowLocalDb)]
fn show_local_db() -> Html {
    let db = use_context::<DbContext>().unwrap();
    let local_items = use_future_with(db.clone(), |db| {
        let db = db.clone();
        async move {
            Ok::<_, crdb::Error>(
                db.0.query_local::<basic_api::Item>(Arc::new(Query::All(Vec::new())))
                    .await?
                    .collect::<Vec<_>>()
                    .await,
            )
        }
    });
    let local_tags = use_future_with(db, |db| {
        let db = db.clone();
        async move {
            Ok::<_, crdb::Error>(
                db.0.query_local::<basic_api::Tag>(Arc::new(Query::All(Vec::new())))
                    .await?
                    .collect::<Vec<_>>()
                    .await,
            )
        }
    });
    let local_items = match &local_items {
        Err(_) => return html! { <h6>{ "Loading local items…" }</h6> },
        Ok(r) => r.as_ref().expect("failed loading local items"),
    };
    let local_items = local_items
        .iter()
        .map(|i| html! {<li><RenderItem data={Rc::new(i.as_ref().unwrap().clone())} /></li>})
        .collect::<Html>();
    let local_tags = match &local_tags {
        Err(_) => return html! { <h6>{ "Loading local tags…" }</h6> },
        Ok(r) => r.as_ref().expect("failed loading local tags"),
    };
    let local_tags = local_tags
        .iter()
        .map(|i| html! {<li><RenderTag data={Rc::new(i.as_ref().unwrap().clone())} /></li>})
        .collect::<Html>();
    html! {<>
        <h3>{ "Local DB" }</h3>
        <ul>
            { local_tags }
            <hr />
            { local_items }
        </ul>
    </>}
}

#[derive(Properties)]
struct RenderItemProps {
    data: Rc<crdb::Obj<Item>>,
}

impl PartialEq for RenderItemProps {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.data, &other.data)
    }
}

#[function_component(RenderItem)]
fn render_item(RenderItemProps { data }: &RenderItemProps) -> Html {
    let db = use_context::<DbContext>().unwrap();
    let ptr = data.ptr();
    // TODO(api-high): whether the object is locked or not should be exposed to the user
    let unlock = {
        let db = db.clone();
        move |_| {
            let db = db.0.clone();
            wasm_bindgen_futures::spawn_local(async move {
                db.unlock(ptr).await.expect("failed unlocking object")
            })
        }
    };
    // TODO(api-high): whether the object is subscribed or not should be exposed to the user
    let unsubscribe = {
        let db = db.clone();
        let force_update = db.1.clone();
        move |_| {
            let db = db.0.clone();
            let force_update = force_update.clone();
            wasm_bindgen_futures::spawn_local(async move {
                db.unsubscribe(ptr)
                    .await
                    .expect("failed unsubscribing object");
                force_update.force_update(); // TODO(blocked): remove once the todos in unsubscribe are solved
            })
        }
    };
    html! {<>
        <small>{ format!("{}: ", data.ptr().to_object_id().0) }</small>
        <i>{ show_user(data.owner) }</i>
        { ": " }
        <b>{ &*data.text }</b>
        { format!(" {:?} {:?} ", data.tags, data.file) }
        <input
            type="button"
            value="Unlock"
            onclick={unlock} />
        <input
            type="button"
            value="Unsubscribe"
            onclick={unsubscribe} />
    </>}
}

#[derive(Properties)]
struct RenderTagProps {
    data: Rc<crdb::Obj<Tag>>,
}

impl PartialEq for RenderTagProps {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.data, &other.data)
    }
}

#[function_component(RenderTag)]
fn render_tag(RenderTagProps { data }: &RenderTagProps) -> Html {
    let db = use_context::<DbContext>().unwrap();
    let show_edit = use_state(|| false);
    let ptr = data.ptr();
    // TODO(api-high): whether the object is locked or not should be exposed to the user
    let unlock = {
        let db = db.clone();
        move |_| {
            let db = db.0.clone();
            wasm_bindgen_futures::spawn_local(async move {
                db.unlock(ptr).await.expect("failed unlocking object")
            })
        }
    };
    // TODO(api-high): whether the object is subscribed or not should be exposed to the user
    let unsubscribe = {
        let db = db.clone();
        let force_update = db.1.clone();
        move |_| {
            let db = db.0.clone();
            let force_update = force_update.clone();
            wasm_bindgen_futures::spawn_local(async move {
                db.unsubscribe(ptr)
                    .await
                    .expect("failed unsubscribing object");
                force_update.force_update(); // TODO(blocked): remove once the todos in unsubscribe are solved
            })
        }
    };
    let click_edit = {
        let show_edit = show_edit.clone();
        move |_| show_edit.set(!*show_edit)
    };
    html! {<>
        <small>{ format!("{}: ", data.ptr().to_object_id().0) }</small>
        <b>{ &*data.name }</b>
        { " read=" }
        { for data.users_who_can_read.iter().map(|u| format!("{},", show_user(*u))) }
        { " write=" }
        { for data.users_who_can_edit.iter().map(|u| format!("{},", show_user(*u))) }
        <input
            type="button"
            value="Unlock"
            onclick={unlock} />
        <input
            type="button"
            value="Unsubscribe"
            onclick={unsubscribe} />
        <input
            type="button"
            value="Edit"
            onclick={click_edit} />
        { (*show_edit).then(|| html! { <div><EditTag {data} /></div> }) }
    </>}
}

#[function_component(EditTag)]
fn edit_tag(RenderTagProps { data }: &RenderTagProps) -> Html {
    html! {
        { "edit" }
    }
}
