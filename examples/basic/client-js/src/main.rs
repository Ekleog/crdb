use basic_api::{AuthInfo, Item};
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
            let (db, upgrade_handle) = basic_api::db::Db::connect(
                String::from("basic-crdb"),
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
            Ok(Rc::new(db))
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
    db: Rc<basic_api::db::Db>,
    connection_status: Rc<String>,
}

impl PartialEq for RefresherProps {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.db, &other.db) && self.connection_status == other.connection_status
    }
}

#[derive(Clone)]
struct RcEq<T>(Rc<T>);

impl<T> PartialEq for RcEq<T> {
    fn eq(&self, other: &RcEq<T>) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
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
    use_effect_with(RcEq(db.clone()), {
        let force_update = force_update.clone();
        move |RcEq(db)| {
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
        <ContextProvider<DbContext> context={DbContext(db.clone(), *counter.borrow())}>
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
struct DbContext(Rc<basic_api::db::Db>, usize);

impl PartialEq for DbContext {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0) && self.1 == other.1
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
                <CreateItem /><br />
                <QueryRemoteItems /><br />
            </div>
            <div style="height: 100%; width: 45%; position: absolute; top: 0; right: 0">
                <ShowSessionList /><hr />
                <ShowUploadQueue /><hr />
                <ShowLocalDb /><hr />
            </div>
        </div>
    </>}
}

#[function_component(CreateItem)]
fn create_item() -> Html {
    let db = use_context::<DbContext>().unwrap().0;
    let text = use_state(|| String::new());
    let onchange = {
        let text = text.clone();
        move |e: Event| {
            let input: web_sys::HtmlInputElement = e.target_unchecked_into();
            text.set(input.value());
        }
    };
    let create_item = Callback::from({
        let text = text.clone();
        move |importance| {
            let item = Item {
                owner: db.user().unwrap(),
                text: SearchableString::from(&*text),
                tags: BTreeSet::new(),
                file: None,
            };
            let db = db.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let _ = db
                    .create_item(importance, Arc::new(item))
                    .await
                    .expect("failed creating item");
            })
        }
    });
    html! {<>
        { "Create Item: "}
        <input
            type="text"
            placeholder="text"
            value={(*text).clone()}
            {onchange} />
        <input
            type="button"
            value="Create Item"
            onclick={create_item.reform(|_| Importance::Latest)} />
        <input
            type="button"
            value="Create Item & Subscribe"
            onclick={create_item.reform(|_| Importance::Subscribe)} />
        <input
            type="button"
            value="Create Item & Lock"
            onclick={create_item.reform(|_| Importance::Lock)} />
    </>}
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
                    .query_item_remote(importance, QueryId::now(), Arc::new(query))
                    .await
                    .expect("failed creating item")
                    .collect::<Vec<_>>()
                    .await;
                query_res.set(res);
            })
        }
    });
    let query_results = query_res
        .iter()
        .map(|r| html! {<> <br /> { format!("{r:?}") } </>})
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
        { query_results }
    </>}
}

#[function_component(ShowSessionList)]
fn show_session_list() -> Html {
    let db = use_context::<DbContext>().unwrap();
    let do_refresh = use_state(|| 0);
    // TODO(api-high): expose way to watch for session list
    let sessions = use_future_with((db, *do_refresh), |arg| {
        let db = arg.0.clone();
        tracing::info!("re-listing sessions");
        async move { db.0.list_sessions().await }
    });
    let sessions = match sessions {
        Ok(s) => s,
        Err(_) => {
            return html! {
                { "Loading…" }
            }
        }
    };
    let sessions = sessions.as_ref().expect("failed listing sessions");
    let sessions = sessions
        .into_iter()
        .map(|s| {
            html! {
                <li key={ format!("{:?}", s.session_ref) }>
                    { format!(
                        "{} (logged in {}, last active {})",
                        s.session_name,
                        chrono::DateTime::<chrono::Local>::from(s.login_time.to_std()),
                        chrono::DateTime::<chrono::Local>::from(s.last_active.to_std()),
                    ) }
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
                onclick={move |_| do_refresh.set(*do_refresh + 1) } />
        </h3>
        { sessions }
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

#[function_component(ShowLocalDb)]
fn show_local_db() -> Html {
    let db = use_context::<DbContext>().unwrap();
    let local_items = use_future_with(db, |db| {
        let db = db.clone();
        async move {
            Ok::<_, crdb::Error>(
                db.0.query_item_local(Arc::new(Query::All(Vec::new())))
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
        .map(|i| html! {<> <br />{ format!("{i:?}") } </>})
        .collect::<Html>();
    html! {<>
        <h3>{ "Local DB" }</h3>
        { local_items }
    </>}
}
