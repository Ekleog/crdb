use crate::{
    db_trait::{DynNewEvent, DynNewObject, DynNewRecreation},
    SessionToken,
};
use futures::{channel::mpsc, StreamExt};
use std::sync::{Arc, RwLock};

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
    FailedConnecting(String),
    Connected,
    LoggedOut,
}

pub enum State {
    NoValidInfo,
    Disconnected {
        url: Arc<String>,
        token: SessionToken,
    },
    Connected {
        url: Arc<String>,
        token: SessionToken,
        socket: implem::WebSocket,
    },
}

pub struct Connection {
    pub state: State,
    pub commands: mpsc::UnboundedReceiver<Command>,
    pub event_cb: Arc<RwLock<Box<dyn Send + Sync + Fn(ConnectionEvent)>>>,
    pub new_objects_sender: async_broadcast::Sender<DynNewObject>,
    pub new_events_sender: async_broadcast::Sender<DynNewEvent>,
    pub new_recreations_sender: async_broadcast::Sender<DynNewRecreation>,
}

impl Connection {
    pub async fn run(mut self) {
        let mut next_command = self.commands.next();
        loop {
            // TODO(api): regularly send GetTime requests for ping/pong checking
            tokio::select! {
                command = next_command => {
                    next_command = self.commands.next();
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
            }

            if let State::Disconnected { url, token } = self.state {
                let socket = match implem::connect(&*url).await {
                    Ok(socket) => socket,
                    Err(err) => {
                        self.event_cb.read().unwrap()(ConnectionEvent::FailedConnecting(err));
                        self.state = State::NoValidInfo;
                        continue;
                    }
                };
                unimplemented!() // TODO(api)
            }
        }
    }
}
