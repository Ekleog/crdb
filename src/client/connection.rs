use crate::{
    ids::RequestId,
    messages::{ClientMessage, Request, ResponsePart, ServerMessage, Update},
    SessionToken,
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
    commands: mpsc::UnboundedReceiver<Command>,
    requests: mpsc::UnboundedReceiver<(mpsc::UnboundedSender<ResponsePart>, Request)>,
    // TODO(api): replace not_sent_request with a proper request for ApiDb to give us all pending requests upon connecting
    not_sent_requests: VecDeque<(mpsc::UnboundedSender<ResponsePart>, Request)>,
    pending_requests: HashMap<RequestId, mpsc::UnboundedSender<ResponsePart>>,
    event_cb: Arc<RwLock<Box<dyn Send + Sync + Fn(ConnectionEvent)>>>,
    update_sender: mpsc::UnboundedSender<Update>,
    next_ping: Option<Instant>,
    next_pong_deadline: Option<(RequestId, Instant)>,
}

impl Connection {
    pub fn new(
        commands: mpsc::UnboundedReceiver<Command>,
        requests: mpsc::UnboundedReceiver<(mpsc::UnboundedSender<ResponsePart>, Request)>,
        event_cb: Arc<RwLock<Box<dyn Fn(ConnectionEvent) + Sync + Send>>>,
        update_sender: mpsc::UnboundedSender<Update>,
    ) -> Connection {
        Connection {
            commands,
            requests,
            not_sent_requests: VecDeque::new(),
            pending_requests: HashMap::new(),
            state: State::NoValidInfo,
            event_cb,
            update_sender,
            next_ping: None,
            next_pong_deadline: None,
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

                // TODO(api): timeout TokenSent state
                // Send the next ping, if it's time to do it
                Some(_) = OptionFuture::from(self.next_ping.map(tokio::time::sleep_until)), if self.is_connected() => {
                    let request_id = RequestId::now();
                    let _ = self.send_connected(&ClientMessage {
                        request_id,
                        request: Request::GetTime,
                    }).await;
                    self.next_ping = None;
                    self.next_pong_deadline = Some((request_id, Instant::now() + PONG_DEADLINE));
                }

                // Next pong did not come in time, disconnect
                Some(_) = OptionFuture::from(self.next_pong_deadline.map(|(_, t)| tokio::time::sleep_until(t))), if self.is_connected() => {
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
                    match self.state {
                        State::Connected { .. } => self.handle_request(sender, request).await,
                        _ => self.not_sent_requests.push_back((sender, request)),
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
                    for (sender, request) in not_sent_requests {
                        self.handle_request(sender, request).await;
                    }
                }
            }

            // Attempt connecting if we're not connected but have connection info
            if let State::Disconnected { url, token } = self.state {
                let mut socket = match implem::connect(&*url).await {
                    Ok(socket) => socket,
                    Err(err) => {
                        self.event_cb.read().unwrap()(ConnectionEvent::FailedConnecting(err));
                        self.state = State::Disconnected { url, token }; // try again next loop
                        continue;
                    }
                };
                let request_id = RequestId::now();
                let message = ClientMessage {
                    request_id,
                    request: Request::SetToken(token),
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
            }
        }
    }

    fn is_trying_to_connect(&self) -> bool {
        matches!(self.state, State::Disconnected { .. })
    }

    fn is_connected(&self) -> bool {
        matches!(self.state, State::Connected { .. })
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
        sender: mpsc::UnboundedSender<ResponsePart>,
        request: Request,
    ) {
        let request_id = RequestId::now();
        let message = ClientMessage {
            request_id,
            request,
        };
        match self.send_connected(&message).await {
            Ok(()) => {
                self.pending_requests.insert(request_id, sender);
            }
            Err(()) => {
                self.not_sent_requests.push_front((sender, message.request));
            }
        }
    }

    async fn handle_connected_message(&mut self, message: ServerMessage) {
        match message {
            ServerMessage::Updates(updates) => {
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
                if let Some(sender) = self.pending_requests.get_mut(&request_id) {
                    // Ignore errors when sending, in case the requester did not await on the response future
                    let _ = sender.unbounded_send(response);
                    if last_response {
                        self.pending_requests.remove(&request_id);
                    }
                } else if self.next_pong_deadline.map(|(r, _)| r) == Some(request_id) {
                    self.next_ping = Some(Instant::now() + PING_INTERVAL);
                    self.next_pong_deadline = None;
                    // TODO(api): tell the user if the time is too off
                } else {
                    tracing::warn!(
                        "Sender gave us a response to {request_id:?} that we do not know of"
                    );
                }
            }
        }
    }

    /// Returns Ok(()) if sending succeeded, and Err(()) if sending failed and triggered a disconnection.
    async fn send_connected(&mut self, message: &ClientMessage) -> Result<(), ()> {
        let State::Connected { socket, url, token } = &mut self.state else {
            panic!("Called handle_request while not connected");
        };
        match Self::send(socket, &message).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.event_cb.read().unwrap()(ConnectionEvent::LostConnection(err));
                self.state = State::Disconnected {
                    url: url.clone(),
                    token: *token,
                };
                Err(())
            }
        }
    }

    async fn send(sock: &mut implem::WebSocket, msg: &ClientMessage) -> anyhow::Result<()> {
        let msg = serde_json::to_string(msg)?;
        implem::send_text(sock, msg).await
    }
}
