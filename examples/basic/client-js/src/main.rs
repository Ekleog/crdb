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
    html! {
        <h1>{ "Hello World" }</h1>
    }
}
