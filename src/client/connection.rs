use crate::{
    ids::QueryId,
    messages::{
        ClientMessage, MaybeObject, Request, RequestId, ResponsePart, ServerMessage, Update,
    },
    ObjectId, SessionToken, Timestamp,
};
use anyhow::anyhow;
use futures::{channel::mpsc, future::OptionFuture, stream, SinkExt, StreamExt};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::time::Instant;

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

    async fn next_msg(&mut self) -> Option<anyhow::Result<ServerMessage>> {
        match self {
            State::NoValidInfo | State::Disconnected { .. } => None,
            State::TokenSent { socket, .. } | State::Connected { socket, .. } => {
                let msg = match implem::next_text(socket).await {
                    Ok(msg) => msg,
                    Err(err) => return Some(Err(err)),
                };
                let msg = match serde_json::from_str(&msg) {
                    Ok(msg) => msg,
                    Err(err) => return Some(Err(err.into())),
                };
                Some(Ok(msg))
            }
        }
    }
}

pub struct Connection {
    state: State,
    last_request_id: RequestId,
    commands: mpsc::UnboundedReceiver<Command>,
    requests: mpsc::UnboundedReceiver<(mpsc::UnboundedSender<ResponsePart>, Request)>,
    // TODO(api): upon reconnecting we should also re-subscribe to all the things we were previously subscribed on
    not_sent_requests: VecDeque<(RequestId, Arc<Request>, mpsc::UnboundedSender<ResponsePart>)>,
    // The last `bool` shows whether we already started sending an answer to the Sender. If yes, we need to
    // kill it with an Error rather than restart it from 0, to avoid duplicate answers.
    pending_requests: HashMap<RequestId, (Arc<Request>, mpsc::UnboundedSender<ResponsePart>, bool)>,
    event_cb: Arc<RwLock<Box<dyn Send + Sync + Fn(ConnectionEvent)>>>,
    update_sender: mpsc::UnboundedSender<Update>,
    last_ping: i64, // Milliseconds since unix epoch
    next_ping: Option<Instant>,
    next_pong_deadline: Option<(RequestId, Instant)>,
    subscribed_objects: HashMap<ObjectId, Option<Timestamp>>, // TODO(api): actually update that timestamp on each received Update
    subscribed_queries: HashMap<QueryId, Option<Timestamp>>,
}

impl Connection {
    pub fn new(
        commands: mpsc::UnboundedReceiver<Command>,
        requests: mpsc::UnboundedReceiver<(mpsc::UnboundedSender<ResponsePart>, Request)>,
        event_cb: Arc<RwLock<Box<dyn Fn(ConnectionEvent) + Sync + Send>>>,
        update_sender: mpsc::UnboundedSender<Update>,
    ) -> Connection {
        Connection {
            state: State::NoValidInfo,
            last_request_id: RequestId(0),
            commands,
            requests,
            not_sent_requests: VecDeque::new(),
            pending_requests: HashMap::new(),
            event_cb,
            update_sender,
            last_ping: Timestamp::now()
                .time_ms_i()
                .expect("Time is obviously ill-set"),
            next_ping: None,
            next_pong_deadline: None,
            subscribed_objects: HashMap::new(),
            subscribed_queries: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        loop {
            // TODO(low): ping/pong should probably be eg. 1 minute when user is inactive, and 10s when active
            tokio::select! {
                // Retry connecting if we're looping there
                // TODO(low): this should probably listen on network status, with eg. window.ononline, to not retry
                // when network is down?
                _reconnect_attempt_interval = tokio::time::sleep(RECONNECT_INTERVAL),
                    if self.is_trying_to_connect() => (),

                // Send the next ping, if it's time to do it
                Some(_) = OptionFuture::from(self.next_ping.map(tokio::time::sleep_until)), if self.is_connected() => {
                    let request_id = self.next_request_id();
                    let _ = self.send_connected(&ClientMessage {
                        request_id,
                        request: Arc::new(Request::GetTime),
                    }).await;
                    self.last_ping = Timestamp::now().time_ms_i().expect("Time is obviously ill-set");
                    self.next_ping = None;
                    self.next_pong_deadline = Some((request_id, Instant::now() + PONG_DEADLINE));
                }

                // Next pong did not come in time, disconnect
                Some(_) = OptionFuture::from(self.next_pong_deadline.map(|(_, t)| tokio::time::sleep_until(t))), if self.is_connecting() => {
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
                        State::Connected { .. } => self.handle_request(request_id, Arc::new(request), sender).await,
                        _ => self.not_sent_requests.push_back((request_id, Arc::new(request), sender)),
                    }
                }

                // Listen for incoming server messages
                Some(message) = self.state.next_msg() => match message {

                    // There was an error in the stream. Likely disconnection.
                    Err(err) => {
                        self.state = self.state.disconnect();
                        self.event_cb.read().unwrap()(ConnectionEvent::LostConnection(err));
                    }

                    Ok(message) => match self.state {
                        State::NoValidInfo | State::Disconnected { .. } => unreachable!(),

                        // We were waiting for an answer to SetToken. Handle it.
                        State::TokenSent { url, token, socket, request_id: req } => match message {
                            ServerMessage::Response {
                                request_id,
                                response: ResponsePart::Success,
                                last_response: true
                            } if req == request_id => {
                                self.state = State::Connected { url, token, socket };
                                self.next_ping = Some(Instant::now() + PING_INTERVAL);
                                self.next_pong_deadline = None;
                                self.event_cb.read().unwrap()(ConnectionEvent::Connected);
                            }
                            ServerMessage::Response {
                                request_id,
                                response: ResponsePart::Error(crate::SerializableError::InvalidToken(tok)),
                                last_response: true
                            } if req == request_id && tok == token => {
                                self.state = State::NoValidInfo;
                                self.event_cb.read().unwrap()(ConnectionEvent::InvalidToken(token));
                            }
                            resp => {
                                self.state = State::NoValidInfo;
                                self.event_cb.read().unwrap()(ConnectionEvent::LostConnection(
                                    anyhow!("Unexpected server answer to login request: {resp:?}")
                                ));
                            }
                        }

                        // Main function, must now deal with requests and updates.
                        State::Connected { .. } => {
                            self.handle_connected_message(message).await;
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
                        self.event_cb.read().unwrap()(ConnectionEvent::FailedConnecting(err));
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
                    self.event_cb.read().unwrap()(ConnectionEvent::FailedSendingToken(err));
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
                            let _ = sender.unbounded_send(ResponsePart::ConnectionLoss);
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
                self.event_cb.read().unwrap()(ConnectionEvent::LoggingIn);
            }
            Command::Logout => {
                self.state = State::NoValidInfo;
                self.event_cb.read().unwrap()(ConnectionEvent::LoggedOut);
            }
        }
    }

    async fn handle_request(
        &mut self,
        request_id: RequestId,
        request: Arc<Request>,
        sender: mpsc::UnboundedSender<ResponsePart>,
    ) {
        match &*request {
            Request::Get {
                object_ids,
                subscribe: true,
            } => {
                self.subscribed_objects
                    .extend(object_ids.iter().map(|(id, t)| (*id, *t)));
            }
            Request::Query {
                query_id,
                query: _,
                only_updated_since,
                subscribe: true,
            } => {
                self.subscribed_queries
                    .insert(*query_id, *only_updated_since);
            }
            Request::Unsubscribe(object_ids) => {
                for object_id in object_ids {
                    self.subscribed_objects.remove(object_id);
                }
            }
            Request::UnsubscribeQuery(query_id) => {
                self.subscribed_queries.remove(query_id);
            }
            _ => (),
        }
        let message = ClientMessage {
            request_id,
            request: request.clone(),
        };
        self.send_connected(&message).await;
        self.pending_requests
            .insert(request_id, (request, sender, false));
    }

    async fn handle_connected_message(&mut self, message: ServerMessage) {
        match message {
            ServerMessage::Updates(updates) => {
                // Update our local subscription information
                for update in updates.iter() {
                    if let Some(updated) = self.subscribed_objects.get_mut(&update.object_id) {
                        *updated = Some(update.now_have_all_until_for_object);
                    }
                    for (query_id, now_updated) in update.now_have_all_until_for_queries.iter() {
                        if let Some(updated) = self.subscribed_queries.get_mut(&query_id) {
                            *updated = Some(*now_updated);
                        }
                    }
                }

                // And send the update
                if let Err(err) = self
                    .update_sender
                    .send_all(&mut stream::iter(updates).map(Ok))
                    .await
                {
                    tracing::error!(?err, "failed sending updates");
                }
            }
            ServerMessage::Response {
                request_id,
                response,
                last_response,
            } => {
                if let Some((request, sender, already_sent)) =
                    self.pending_requests.get_mut(&request_id)
                {
                    // Update our local subscription information
                    match &**request {
                        Request::Get {
                            subscribe: true, ..
                        } => {
                            if let ResponsePart::Objects { data, .. } = &response {
                                for maybe_object in data {
                                    match maybe_object {
                                        MaybeObject::AlreadySubscribed(_) => (),
                                        MaybeObject::NotYetSubscribed(o) => {
                                            self.subscribed_objects
                                                .insert(o.object_id, Some(o.now_have_all_until));
                                        }
                                    }
                                }
                            }
                        }
                        Request::Query {
                            query_id,
                            subscribe: true,
                            ..
                        } => {
                            if let ResponsePart::Objects {
                                now_have_all_until, ..
                            } = &response
                            {
                                self.subscribed_queries
                                    .insert(*query_id, *now_have_all_until);
                            }
                        }
                        _ => (),
                    }

                    // And send the response
                    // Ignore errors when sending, in case the requester did not await on the response future
                    *already_sent = true;
                    let _ = sender.unbounded_send(response);
                    if last_response {
                        self.pending_requests.remove(&request_id);
                    }
                } else if self.next_pong_deadline.map(|(r, _)| r) == Some(request_id) {
                    let ResponsePart::CurrentTime(server_time) = response else {
                        tracing::error!("Server answered GetTime with unexpected {response:?}");
                        return;
                    };
                    let Ok(server_time) = server_time.time_ms_i() else {
                        tracing::error!("Server answered GetTime with obviously-wrong timestamp {server_time:?}");
                        return;
                    };
                    self.next_ping = Some(Instant::now() + PING_INTERVAL);
                    self.next_pong_deadline = None;
                    // Figure out the time offset with the server, only counting certainly-off times
                    let now = Timestamp::now()
                        .time_ms_i()
                        .expect("Time was obviously wrong");
                    if server_time.saturating_sub(now) > 0 {
                        self.event_cb.read().unwrap()(ConnectionEvent::TimeOffset(
                            server_time.saturating_sub(now),
                        ));
                    } else if server_time.saturating_sub(self.last_ping) < 0 {
                        self.event_cb.read().unwrap()(ConnectionEvent::TimeOffset(
                            server_time.saturating_sub(self.last_ping),
                        ));
                    } else {
                        self.event_cb.read().unwrap()(ConnectionEvent::TimeOffset(0));
                    }
                } else {
                    tracing::warn!(
                        "Server gave us a response to {request_id:?} that we do not know of"
                    );
                }
            }
        }
    }

    /// Returns Ok(()) if sending succeeded, and Err(()) if sending failed and triggered a disconnection.
    async fn send_connected(&mut self, message: &ClientMessage) {
        let State::Connected { socket, url, token } = &mut self.state else {
            panic!("Called send_connected while not connected");
        };
        if let Err(err) = Self::send(socket, &message).await {
            self.event_cb.read().unwrap()(ConnectionEvent::LostConnection(err));
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
