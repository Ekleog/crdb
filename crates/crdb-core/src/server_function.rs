pub struct ServerFunction<Args> {
    calls: VecDeque<Args>,
}

impl ServerFunction<Args> {
    pub fn new() -> Self {
        Self {
            calls: VecDeque::new(),
        }
    }

    pub fn call(&mut self, args: Args) {
        self.calls.push_back(args);
    }

    pub async fn run_executor(&mut self, f: impl FnMut(Args) -> waaa::BoxFuture<'static, ()>) {
        // TODO(api-med): actually finish the ServerFunction work
        // We want to:
        // - expose it publicly
        // - have run_executor run in a separate process
        // - have the user call call whenever they want the client to call the server function
        // - have the call function trigger run_executor to execute f and push a new event to the object
        // - this will probably require passing the object to call so that f can be called with it
        // - think about access patterns, eg. how to limit the new events to only the run_executor user?
    }
}
