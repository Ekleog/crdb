use crate::{
    ids::RequestId,
    messages::{ClientMessage, Request, ResponsePart, ServerMessage, Update},
    SessionToken,
};
use anyhow::anyhow;
use futures::{channel::mpsc, stream, SinkExt, StreamExt};
use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
    time::Duration,
};

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(target_arch = "wasm32")]
mod wasm;

#[cfg(not(target_arch = "wasm32"))]
use native as implem;
#[cfg(target_arch = "wasm32")]
use wasm as implem;

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
    not_sent_requests: VecDeque<(mpsc::UnboundedSender<ResponsePart>, Request)>,
    event_cb: Arc<RwLock<Box<dyn Send + Sync + Fn(ConnectionEvent)>>>,
    update_sender: mpsc::UnboundedSender<Update>,
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
            state: State::NoValidInfo,
            event_cb,
            update_sender,
        }
    }

    pub async fn run(mut self) {
        loop {
            // TODO(api): regularly send GetTime requests for ping/pong checking
            // TODO(low): ping/pong should probably be eg. 1 minute when user is inactive, and 10s when active
            tokio::select! {
                // Retry connecting if we're looping there
                // TODO(low): this should probably listen on network status, with eg. window.ononline, to not retry
                // when network is down?
                _reconnect_attempt_interval = tokio::time::sleep(Duration::from_secs(10)),
                    if self.is_trying_to_connect() => (),

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
                        State::TokenSent { url, token, socket, request_id } => match message {
                            ServerMessage::Response {
                                request,
                                response: ResponsePart::Success,
                                last_response: true
                            } if request == request_id => {
                                self.state = State::Connected { url, token, socket };
                                self.event_cb.read().unwrap()(ConnectionEvent::Connected);
                            }
                            ServerMessage::Response {
                                request,
                                response: ResponsePart::Error(crate::SerializableError::InvalidToken(tok)),
                                last_response: true
                            } if request == request_id && tok == token => {
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
        let _ = (sender, request);
        unimplemented!() // TODO(api)
    }

    async fn handle_connected_message(&mut self, message: ServerMessage) {
        let State::Connected { socket, .. } = &mut self.state else {
            panic!("Called handle_connected_message while not actually connected");
        };
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
                request,
                response,
                last_response,
            } => {
                let _ = (socket, request, response, last_response);
                unimplemented!() // TODO(api): implement
            }
        }
    }

    async fn send(sock: &mut implem::WebSocket, msg: &ClientMessage) -> anyhow::Result<()> {
        let msg = serde_json::to_string(msg)?;
        implem::send_text(sock, msg).await
    }
}
