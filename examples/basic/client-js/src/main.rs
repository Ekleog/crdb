use basic_api::{AuthInfo, Item};
use crdb::{fts::SearchableString, Importance, JsonPathItem, Query, QueryId, SessionToken, User};
use futures::stream::StreamExt;
use std::{collections::BTreeSet, rc::Rc, str::FromStr, sync::Arc, time::Duration};
use ulid::Ulid;
use yew::prelude::*;
use yew_hooks::prelude::*;

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
    // TODO(misc-high): write a crdb-yew to hide that and only refresh when required
    let force_update = use_force_update();
    let db = use_async_with_options(
        {
            let require_relogin = require_relogin.clone();
            async move {
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
                db.on_connection_event(|evt| {
                    tracing::info!(?evt, "connection event");
                });
                let mut updates = db.listen_for_all_updates();
                let force_update = force_update.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    loop {
                        match updates.recv().await {
                            Err(crdb::broadcast::error::RecvError::Closed) => break,
                            _ => (), // ignore the contents, just refresh
                        }
                        tracing::info!("refreshing");
                        force_update.force_update();
                    }
                });
                Ok(Rc::new(db))
            }
        },
        UseAsyncOptions::enable_auto(),
    );
    if db.loading || *logging_in {
        html! {
            <h1>{ "Loading…" }</h1>
        }
    } else if let Some(err) = &db.error {
        panic!("Unexpected error loading database:\n{err}");
    } else if *require_relogin {
        let on_login = Callback::from({
            let db = db.data.clone().unwrap();
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
    } else if let Some(db) = &db.data {
        html! {
            <ContextProvider<DbContext> context={DbContext(db.clone())}>
                <MainView />
            </ContextProvider<DbContext>>
        }
    } else {
        // See https://github.com/jetli/yew-hooks/issues/40 for why this branch is required
        html! {
            <h1>{ "This should not be visible more than a fraction of a second" }</h1>
        }
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
struct DbContext(Rc<basic_api::db::Db>);

impl PartialEq for DbContext {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

fn show_user(user: User) -> String {
    let s = format!("{}", user.0);
    let i = s.find(|c| c != '0').unwrap_or_else(|| s.len() - 3);
    s[i..].to_owned()
}

#[function_component(MainView)]
fn main_view() -> Html {
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
            { format!("Logged in as {} ", show_user(db.user().unwrap())) }
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
                <ShowLocalDb /><br />
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

#[function_component(ShowLocalDb)]
fn show_local_db() -> Html {
    let db = use_context::<DbContext>().unwrap().0;
    let local_items = use_async_with_options(
        async move {
            Ok::<_, String>(
                db.query_item_local(Arc::new(Query::All(Vec::new())))
                    .await
                    .map_err(|err| format!("{err:?}"))?
                    .map(|v| v.map_err(|err| format!("{err:?}")))
                    .collect::<Vec<_>>()
                    .await,
            )
        },
        UseAsyncOptions::enable_auto(),
    );
    let local_items = if local_items.loading {
        html! { <h6>{ "Loading local items" }</h6> }
    } else if let Some(err) = &local_items.error {
        panic!("failed loading local items: {err:?}");
    } else if let Some(local_items) = &local_items.data {
        local_items
            .iter()
            .map(|i| html! {<> <br />{ format!("{i:?}") } </>})
            .collect::<Html>()
    } else {
        html! { <h6>{ "Transient message, should go away soon" }</h6> }
    };
    html! {<>
        <h3>{ "Local DB" }</h3>
        { local_items }
    </>}
}
