use crate::{
    db_trait::{DynNewEvent, DynNewObject, DynNewRecreation},
    ids::RequestId,
    messages::{ClientMessage, Request, ServerMessage},
    SessionToken,
};
use futures::{channel::mpsc, StreamExt};
use std::{
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
    event_cb: Arc<RwLock<Box<dyn Send + Sync + Fn(ConnectionEvent)>>>,
    new_objects_sender: async_broadcast::Sender<DynNewObject>,
    new_events_sender: async_broadcast::Sender<DynNewEvent>,
    new_recreations_sender: async_broadcast::Sender<DynNewRecreation>,
}

impl Connection {
    pub fn new(
        commands: mpsc::UnboundedReceiver<Command>,
        event_cb: Arc<RwLock<Box<dyn Fn(ConnectionEvent) + Sync + Send>>>,
        new_events_sender: async_broadcast::Sender<DynNewEvent>,
        new_objects_sender: async_broadcast::Sender<DynNewObject>,
        new_recreations_sender: async_broadcast::Sender<DynNewRecreation>,
    ) -> Connection {
        Connection {
            commands,
            state: State::NoValidInfo,
            event_cb,
            new_events_sender,
            new_objects_sender,
            new_recreations_sender,
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

                // Listen for incoming server messages
                Some(message) = self.state.next_msg() => {
                    unimplemented!() // TODO(api)
                }
            }

            // Attempt connecting if we're in a connection loop
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
                let message =
                    serde_json::to_string(&message).expect("failed serializing client message");
                if let Err(err) = implem::send_text(&mut socket, message).await {
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
}
