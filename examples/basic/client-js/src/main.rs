use basic_api::AuthInfo;
use crdb::{SessionToken, User};
use std::{rc::Rc, str::FromStr, sync::Arc, time::Duration};
use ulid::Ulid;
use yew::prelude::*;
use yew_hooks::prelude::*;

const CACHE_WATERMARK: usize = 8 * 1024 * 1024;
const VACUUM_FREQUENCY: Duration = Duration::from_secs(3600);

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
                    db.login(Arc::new(String::from("/api/ws")), user, token)
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
        let _: &Rc<basic_api::db::Db> = db;
        unimplemented!() // TODO(example-high)
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
    let state = use_state(|| (String::from(""), String::from("")));
    let on_user_change = {
        let state = state.clone();
        move |e: Event| {
            let input: web_sys::HtmlInputElement = e.target_unchecked_into();
            state.set((user_to_ulid(input.value()), state.1.clone()));
        }
    };
    let on_pass_change = {
        let state = state.clone();
        move |e: Event| {
            let input: web_sys::HtmlInputElement = e.target_unchecked_into();
            state.set((state.0.clone(), input.value()));
        }
    };
    let onclick = {
        let on_login = on_login.clone();
        let state = state.clone();
        move |_| {
            let on_login = on_login.clone();
            let state = state.clone();
            wasm_bindgen_futures::spawn_local(async move {
                let user = format!("{:0>26}", state.0);
                let user = User(Ulid::from_str(&user).expect("username is invalid"));
                let auth_info = AuthInfo {
                    user,
                    pass: state.1.clone(),
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
                    value={state.0.clone()}
                    onchange={on_user_change}
                    />
                { " Password: " }
                <input
                    type="password"
                    placeholder="password"
                    value={state.1.clone()}
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
