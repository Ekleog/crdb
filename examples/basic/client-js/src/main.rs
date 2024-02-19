use yew::prelude::*;

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
    let db = use_state(|| None);
    if let Some(db) = &*db {
        let _: &basic_api::db::Db = db;
        unimplemented!() // TODO(example-high)
    } else {
        let on_login = Callback::from(move |_| {
            unimplemented!() // TODO(example-high)
        });
        html! {
            <Login {on_login} />
        }
    }
}

#[derive(Properties, PartialEq)]
struct LoginProps {
    on_login: Callback<()>,
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
        .filter(|&c| {
            (c >= '0' && c <= '9')
                || (c >= 'A' && c <= 'Z')
        })
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
        move |_| {
            let on_login = on_login.clone();
            wasm_bindgen_futures::spawn_local(async move {
                on_login.emit(()) // TODO(example-high)
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
                    type="submit"
                    value="Login"
                    {onclick}
                    />
            </form>
        </>
    }
}
