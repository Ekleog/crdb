use crate::{
    crdb_internal::Lock,
    future::CrdbFn,
    ids::QueryId,
    messages::{
        ClientMessage, MaybeObject, Request, RequestId, ResponsePart, ServerMessage, Update,
        UpdateData, Updates,
    },
    timestamp::SystemTimeExt,
    ObjectId, Query, SessionToken, TypeId, Updatedness,
};
use anyhow::anyhow;
use futures::{channel::mpsc, future::OptionFuture, SinkExt, StreamExt};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use web_time::Instant;
use web_time::SystemTime;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(target_arch = "wasm32")]
mod wasm;

#[cfg(not(target_arch = "wasm32"))]
use native as implem;
#[cfg(target_arch = "wasm32")]
use wasm as implem;

const RECONNECT_INTERVAL: Duration = Duration::from_secs(10);
const PING_INTERVAL: Duration = Duration::from_secs(10);
const PONG_DEADLINE: Duration = Duration::from_secs(10);

pub struct RequestWithSidecar {
    pub request: Arc<Request>,
    pub sidecar: Vec<Arc<[u8]>>,
}

pub struct ResponsePartWithSidecar {
    pub response: ResponsePart,
    pub sidecar: Option<Arc<[u8]>>,
}

pub enum Command {
    Login {
        url: Arc<String>,
        token: SessionToken,
    },
    Logout,
}

pub enum ConnectionEvent {
    LoggingIn,
    FailedConnecting(anyhow::Error),
    FailedSendingToken(anyhow::Error),
    LostConnection(anyhow::Error),
    InvalidToken(SessionToken),
    Connected,
    TimeOffset(i64), // Time offset with the server, in milliseconds
    LoggedOut,
}

enum IncomingMessage<T> {
    Text(T),
    Binary(Arc<[u8]>),
}

pub enum State {
    NoValidInfo,
    Disconnected {
        url: Arc<String>,
        token: SessionToken,
    },
    TokenSent {
        url: Arc<String>,
        token: SessionToken,
        socket: implem::WebSocket,
        request_id: RequestId,
    },
    Connected {
        url: Arc<String>,
        token: SessionToken,
        socket: implem::WebSocket,
        // Currently expecting `usize` more binaries for the request `RequestId`
        expected_binaries: Option<(RequestId, usize)>,
    },
}

impl State {
    fn disconnect(self) -> Self {
        match self {
            State::NoValidInfo => State::NoValidInfo,
            State::Disconnected { url, token }
            | State::TokenSent { url, token, .. }
            | State::Connected { url, token, .. } => State::Disconnected { url, token },
        }
    }

    fn no_longer_expecting_binaries(&mut self) {
        match self {
            State::NoValidInfo | State::Disconnected { .. } | State::TokenSent { .. } => (),
            State::Connected {
                expected_binaries, ..
            } => *expected_binaries = None,
        }
    }

    async fn next_msg(&mut self) -> Option<anyhow::Result<IncomingMessage<ServerMessage>>> {
        match self {
            State::NoValidInfo | State::Disconnected { .. } => None,
            State::TokenSent { socket, .. } | State::Connected { socket, .. } => {
                match implem::next(socket).await {
                    Err(err) => Some(Err(err)),
                    Ok(IncomingMessage::Binary(b)) => Some(Ok(IncomingMessage::Binary(b))),
                    Ok(IncomingMessage::Text(msg)) => match serde_json::from_str(&msg) {
                        Ok(msg) => Some(Ok(IncomingMessage::Text(msg))),
                        Err(err) => Some(Err(err.into())),
                    },
                }
            }
        }
    }
}

pub type ResponseSender = mpsc::UnboundedSender<ResponsePartWithSidecar>;

pub struct Connection<GetSubscribedObjects, GetSubscribedQueries> {
    state: State,
    last_request_id: RequestId,
    commands: mpsc::UnboundedReceiver<Command>,
    requests: mpsc::UnboundedReceiver<(ResponseSender, Arc<RequestWithSidecar>)>,
    not_sent_requests: VecDeque<(RequestId, Arc<RequestWithSidecar>, ResponseSender)>,
    // The last `bool` shows whether we already started sending an answer to the Sender. If yes, we need to
    // kill it with an Error rather than restart it from 0, to avoid duplicate answers.
    pending_requests: HashMap<RequestId, (Arc<RequestWithSidecar>, ResponseSender, bool)>,
    event_cb: Box<dyn CrdbFn<ConnectionEvent>>,
    update_sender: mpsc::UnboundedSender<Updates>,
    last_ping: i64, // Milliseconds since unix epoch
    next_ping: Option<Instant>,
    next_pong_deadline: Option<(RequestId, Instant)>,
    get_subscribed_objects: GetSubscribedObjects,
    get_subscribed_queries: GetSubscribedQueries,
}

impl<GSO, GSQ> Connection<GSO, GSQ>
where
    GSO: 'static + FnMut() -> HashMap<ObjectId, Option<Updatedness>>,
    GSQ: 'static + FnMut() -> HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, Lock)>,
{
    pub fn new(
        commands: mpsc::UnboundedReceiver<Command>,
        requests: mpsc::UnboundedReceiver<(ResponseSender, Arc<RequestWithSidecar>)>,
        event_cb: Box<dyn CrdbFn<ConnectionEvent>>,
        update_sender: mpsc::UnboundedSender<Updates>,
        get_subscribed_objects: GSO,
        get_subscribed_queries: GSQ,
    ) -> Connection<GSO, GSQ> {
        Connection {
            state: State::NoValidInfo,
            last_request_id: RequestId(0),
            commands,
            requests,
            not_sent_requests: VecDeque::new(),
            pending_requests: HashMap::new(),
            event_cb,
            update_sender,
            last_ping: SystemTime::now().ms_since_posix().unwrap(),
            next_ping: None,
            next_pong_deadline: None,
            get_subscribed_objects,
            get_subscribed_queries,
        }
    }

    pub async fn run(mut self) {
        loop {
            // TODO(perf-low): ping/pong should probably be eg. 1 minute when user is inactive, and 10s when active
            tokio::select! {
                // Retry connecting if we're looping there
                // TODO(perf-low): this should probably listen on network status, with eg. window.ononline, to not retry
                // when network is down?
                _reconnect_attempt_interval = crate::sleep(RECONNECT_INTERVAL),
                    if self.is_trying_to_connect() => (),

                // Send the next ping, if it's time to do it
                Some(_) = OptionFuture::from(self.next_ping.map(crate::sleep_until)), if self.is_connected() => {
                    let request_id = self.next_request_id();
                    let _ = self.send_connected(&ClientMessage {
                        request_id,
                        request: Arc::new(Request::GetTime),
                    }).await;
                    self.last_ping = SystemTime::now().ms_since_posix().unwrap();
                    self.next_ping = None;
                    self.next_pong_deadline = Some((request_id, Instant::now() + PONG_DEADLINE));
                }

                // Next pong did not come in time, disconnect
                Some(_) = OptionFuture::from(self.next_pong_deadline.map(|(_, t)| crate::sleep_until(t))), if self.is_connecting() => {
                    self.state = self.state.disconnect();
                    self.next_pong_deadline = None;
                }

                // Listen for any incoming commands (including end-of-run)
                // Note: StreamExt::next is cancellation-safe on any Stream
                command = self.commands.next() => {
                    let Some(command) = command else {
                        break; // ApiDb was dropped, let's close ourselves
                    };
                    self.handle_command(command);
                }

                // Listen for incoming requests from the client
                request = self.requests.next() => {
                    let Some((sender, request)) = request else {
                        break; // ApiDb was dropped, let's close ourselves
                    };
                    let request_id = self.next_request_id();
                    match self.state {
                        State::Connected { .. } => self.handle_request(request_id, request, sender).await,
                        _ => self.not_sent_requests.push_back((request_id, request, sender)),
                    }
                }

                // Listen for incoming server messages
                Some(message) = self.state.next_msg() => match message {

                    // There was an error in the stream. Likely disconnection.
                    Err(err) => {
                        self.state = self.state.disconnect();
                        (self.event_cb)(ConnectionEvent::LostConnection(err));
                    }

                    // We got a new text message.
                    Ok(IncomingMessage::Text(message)) => match self.state {
                        State::NoValidInfo | State::Disconnected { .. } => unreachable!(),

                        // We were waiting for an answer to SetToken. Handle it.
                        State::TokenSent { url, token, socket, request_id: req } => match message {
                            ServerMessage::Response {
                                request_id,
                                response: ResponsePart::Success,
                                last_response: true
                            } if req == request_id => {
                                self.state = State::Connected { url, token, socket, expected_binaries: None };
                                self.next_ping = Some(Instant::now() + PING_INTERVAL);
                                self.next_pong_deadline = None;
                                (self.event_cb)(ConnectionEvent::Connected);

                                // Re-subscribe to the previously subscribed queries and objects
                                // Start with subscribed objects, so that we easily tell the server what we already know about them.
                                // Only then re-subscribe to queries, this way the server can answer AlreadySubscribed whenever relevant.
                                let subscribed_objects = (self.get_subscribed_objects)();
                                let subscribed_queries = (self.get_subscribed_queries)();
                                if !subscribed_objects.is_empty() {
                                    let (responses_sender, responses_receiver) = mpsc::unbounded();
                                    let request_id = self.next_request_id();
                                    self.handle_request(
                                        request_id,
                                        Arc::new(RequestWithSidecar {
                                            request: Arc::new(Request::GetSubscribe(subscribed_objects)),
                                            sidecar: Vec::new(),
                                        }),
                                        responses_sender,
                                    ).await;
                                    crate::spawn(Self::send_responses_as_updates(self.update_sender.clone(), responses_receiver));
                                }
                                for (query_id, (query, type_id, have_all_until, _)) in subscribed_queries {
                                    let (responses_sender, responses_receiver) = mpsc::unbounded();
                                    let request_id = self.next_request_id();
                                    self.handle_request(
                                        request_id,
                                        Arc::new(RequestWithSidecar {
                                            request: Arc::new(Request::QuerySubscribe {
                                                query_id,
                                                type_id,
                                                query,
                                                only_updated_since: have_all_until,
                                            }),
                                            sidecar: Vec::new(),
                                        }),
                                        responses_sender,
                                    ).await;
                                    crate::spawn(Self::send_responses_as_updates(self.update_sender.clone(), responses_receiver));
                                }
                            }
                            ServerMessage::Response {
                                request_id,
                                response: ResponsePart::Error(crate::SerializableError::InvalidToken(tok)),
                                last_response: true
                            } if req == request_id && tok == token => {
                                self.state = State::NoValidInfo;
                                (self.event_cb)(ConnectionEvent::InvalidToken(token));
                            }
                            resp => {
                                self.state = State::Disconnected { url, token };
                                (self.event_cb)(ConnectionEvent::LostConnection(
                                    anyhow!("Unexpected server answer to login request: {resp:?}")
                                ));
                            }
                        }

                        // Main function, must now deal with requests and updates.
                        State::Connected { expected_binaries: None, .. } => {
                            self.handle_connected_message(message).await;
                        }

                        // We got a new text message while still expecting a binary message. Protocol violation.
                        State::Connected { expected_binaries: Some(_), .. } => {
                            self.state = self.state.disconnect();
                            (self.event_cb)(ConnectionEvent::LostConnection(
                                anyhow!("Unexpected server message while waiting for binaries: {message:?}")
                            ));
                        }
                    }

                    // We got a new binary message.
                    Ok(IncomingMessage::Binary(message)) => {
                        if let State::Connected { expected_binaries: Some((request_id, num_bins)), .. } = &mut self.state {
                            if let Some((_, sender, already_sent)) = self.pending_requests.get_mut(&request_id) {
                                *already_sent = true;
                                let _ = sender.unbounded_send(ResponsePartWithSidecar {
                                    response: ResponsePart::Binaries(1),
                                    sidecar: Some(message),
                                });
                                *num_bins -= 1;
                                if *num_bins == 0 {
                                    self.state.no_longer_expecting_binaries();
                                }
                            } else {
                                tracing::error!(?request_id, "Connection::State.expected_binaries is pointing to a non-existent request");
                            }
                        } else {
                            self.state = self.state.disconnect();
                            (self.event_cb)(ConnectionEvent::LostConnection(
                                anyhow!("Unexpected server binary frame while not waiting for it")
                            ));
                        }
                    }
                }
            }

            if let State::Connected { .. } = self.state {
                if !self.not_sent_requests.is_empty() {
                    let not_sent_requests =
                        std::mem::replace(&mut self.not_sent_requests, VecDeque::new());
                    for (request_id, request, sender) in not_sent_requests {
                        self.handle_request(request_id, request, sender).await;
                    }
                }
            }

            // Attempt connecting if we're not connected but have connection info
            if let State::Disconnected { url, token } = &self.state {
                let url = url.clone();
                let token = *token;
                let mut socket = match implem::connect(&*url).await {
                    Ok(socket) => socket,
                    Err(err) => {
                        (self.event_cb)(ConnectionEvent::FailedConnecting(err));
                        self.state = State::Disconnected { url, token }; // try again next loop
                        continue;
                    }
                };
                let request_id = self.next_request_id();
                let message = ClientMessage {
                    request_id,
                    request: Arc::new(Request::SetToken(token)),
                };
                if let Err(err) = Self::send(&mut socket, &message).await {
                    (self.event_cb)(ConnectionEvent::FailedSendingToken(err));
                    self.state = State::Disconnected { url, token }; // try again next loop
                    continue;
                }
                self.state = State::TokenSent {
                    url,
                    token,
                    socket,
                    request_id,
                };
                self.next_pong_deadline = Some((request_id, Instant::now() + PONG_DEADLINE));
                // We're waiting for a reconnection, re-enqueue all pending requests
                if !self.pending_requests.is_empty() {
                    for (request_id, (request, sender, already_sent)) in
                        self.pending_requests.drain()
                    {
                        if already_sent {
                            let _ = sender.unbounded_send(ResponsePartWithSidecar {
                                response: ResponsePart::Error(
                                    crate::SerializableError::ConnectionLoss,
                                ),
                                sidecar: None,
                            });
                        } else {
                            self.not_sent_requests
                                .push_front((request_id, request, sender));
                        }
                    }
                    self.not_sent_requests
                        .make_contiguous()
                        .sort_unstable_by_key(|v| v.0);
                }
            }
        }
    }

    fn is_trying_to_connect(&self) -> bool {
        matches!(self.state, State::Disconnected { .. })
    }

    fn is_connected(&self) -> bool {
        matches!(self.state, State::Connected { .. })
    }

    fn is_connecting(&self) -> bool {
        matches!(
            self.state,
            State::Connected { .. } | State::TokenSent { .. }
        )
    }

    fn next_request_id(&mut self) -> RequestId {
        self.last_request_id = RequestId(self.last_request_id.0 + 1);
        self.last_request_id
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::Login { url, token } => {
                self.state = State::Disconnected { url, token };
                (self.event_cb)(ConnectionEvent::LoggingIn);
            }
            Command::Logout => {
                self.state = State::NoValidInfo;
                (self.event_cb)(ConnectionEvent::LoggedOut);
            }
        }
    }

    async fn handle_request(
        &mut self,
        request_id: RequestId,
        request: Arc<RequestWithSidecar>,
        sender: ResponseSender,
    ) {
        let message = ClientMessage {
            request_id,
            request: request.request.clone(),
        };
        self.send_connected(&message).await;
        self.send_connected_sidecar(&request.sidecar).await;
        self.pending_requests
            .insert(request_id, (request, sender, false));
    }

    async fn handle_connected_message(&mut self, message: ServerMessage) {
        match message {
            ServerMessage::Updates(updates) => {
                if let Err(err) = self.update_sender.send(updates).await {
                    tracing::error!(?err, "failed sending updates");
                }
            }
            ServerMessage::Response {
                request_id,
                response,
                last_response,
            } => {
                if let Some((_, sender, already_sent)) = self.pending_requests.get_mut(&request_id)
                {
                    if let ResponsePart::Binaries(num_bins) = &response {
                        // The request was binary retrieval. We should remember that and send the binary frames as they come.
                        let State::Connected {
                            expected_binaries, ..
                        } = &mut self.state
                        else {
                            panic!("Called send_connected while not connected");
                        };
                        if *num_bins > 0 {
                            *expected_binaries = Some((request_id, *num_bins));
                        }
                        // Do not send a response part yet! We'll send them one by one as the binaries come in.
                    } else {
                        // Regular response, just send it.
                        // Ignore errors when sending, in case the requester did not await on the response future
                        *already_sent = true;
                        let _ = sender.unbounded_send(ResponsePartWithSidecar {
                            response,
                            sidecar: None,
                        });
                        if last_response {
                            self.pending_requests.remove(&request_id);
                        }
                    }
                } else if self.next_pong_deadline.map(|(r, _)| r) == Some(request_id) {
                    let ResponsePart::CurrentTime(server_time) = response else {
                        tracing::error!("Server answered GetTime with unexpected {response:?}");
                        return;
                    };
                    let Ok(server_time) = server_time.ms_since_posix() else {
                        tracing::error!("Server answered GetTime with obviously-wrong timestamp {server_time:?}");
                        return;
                    };
                    self.next_ping = Some(Instant::now() + PING_INTERVAL);
                    self.next_pong_deadline = None;
                    // Figure out the time offset with the server, only counting certainly-off times
                    let now = SystemTime::now().ms_since_posix().unwrap();
                    if server_time.saturating_sub(now) > 0 {
                        (self.event_cb)(ConnectionEvent::TimeOffset(
                            server_time.saturating_sub(now),
                        ));
                    } else if server_time.saturating_sub(self.last_ping) < 0 {
                        (self.event_cb)(ConnectionEvent::TimeOffset(
                            server_time.saturating_sub(self.last_ping),
                        ));
                    } else {
                        (self.event_cb)(ConnectionEvent::TimeOffset(0));
                    }
                } else {
                    tracing::warn!(
                        "Server gave us a response to {request_id:?} that we do not know of"
                    );
                }
            }
        }
    }

    async fn send_responses_as_updates(
        update_sender: mpsc::UnboundedSender<Updates>,
        mut responses_receiver: mpsc::UnboundedReceiver<ResponsePartWithSidecar>,
    ) {
        // No need to keep track of self.subscribed_*, this will be done before even reaching this point
        while let Some(response) = responses_receiver.next().await {
            // Ignore the sidecar here. We're not requesting any binaries so there can't be anything anyway
            match response.response {
                ResponsePart::Error(crate::SerializableError::ConnectionLoss) => (), // too bad, let's empty the feed and try again next reconnection
                ResponsePart::Error(crate::SerializableError::ObjectDoesNotExist(object_id)) => {
                    // Server claimed this object doesn't exist, but we actually knew about it already
                    // The only possible conclusion is that we lost the rights to read the object.
                    let _ = update_sender.unbounded_send(Updates {
                        data: vec![Arc::new(Update {
                            object_id,
                            data: UpdateData::LostReadRights,
                        })],
                        now_have_all_until: Updatedness::from_u128(0), // Placeholder: the object will be deleted locally anyway
                    });
                }
                ResponsePart::Error(err) => {
                    tracing::error!(?err, "got unexpected server error upon re-subscribing");
                }
                ResponsePart::Objects { data, .. } => {
                    for maybe_object in data.into_iter() {
                        match maybe_object {
                            MaybeObject::AlreadySubscribed(_) => continue,
                            MaybeObject::NotYetSubscribed(object) => {
                                let now_have_all_until = object.now_have_all_until;
                                let _ = update_sender.unbounded_send(Updates {
                                    data: object.into_updates(),
                                    now_have_all_until,
                                });
                            }
                        }
                    }
                    // Note: We do not care about negative updates. Indeed, if an object were to stop matching
                    // and we reconnect, we would still resubscribe to the object anyway, because we automatically
                    // subscribe to (and never automatically unsubscribe from) any objects returned by subscribed
                    // queries. As such, the updates to that object will just keep coming through anyway, and the
                    // fact that they no longer match the queries should be made obvious at that point.
                    // TODO(misc-med): if we introduce a ManuallyUpdated subscription level, this would stop being true:
                    // we could have subscription be handled just like locking, and objects that stop matching
                    // queries would then automatically fall back to being ManuallyUpdated
                }
                response => {
                    tracing::error!(
                        ?response,
                        "got unexpected server response upon re-subscribing"
                    );
                }
            }
        }
    }

    async fn send_connected_sidecar(&mut self, sidecar: &Vec<Arc<[u8]>>) {
        let State::Connected {
            socket, url, token, ..
        } = &mut self.state
        else {
            panic!("Called send_connected while not connected");
        };
        if let Err(err) = implem::send_sidecar(socket, sidecar).await {
            (self.event_cb)(ConnectionEvent::LostConnection(err));
            self.state = State::Disconnected {
                url: url.clone(),
                token: *token,
            };
        }
    }

    async fn send_connected(&mut self, message: &ClientMessage) {
        let State::Connected {
            socket, url, token, ..
        } = &mut self.state
        else {
            panic!("Called send_connected while not connected");
        };
        if let Err(err) = Self::send(socket, &message).await {
            (self.event_cb)(ConnectionEvent::LostConnection(err));
            self.state = State::Disconnected {
                url: url.clone(),
                token: *token,
            };
        }
    }

    async fn send(sock: &mut implem::WebSocket, msg: &ClientMessage) -> anyhow::Result<()> {
        let msg = serde_json::to_string(msg)?;
        implem::send_text(sock, msg).await
    }
}
