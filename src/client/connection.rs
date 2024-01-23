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

pub enum ConnectionState {
    Connected,
    Disconnected,
    NoValidToken,
}

pub enum State {
    NoValidToken,
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
    pub state_change_cb: Arc<RwLock<Box<dyn Send + Sync + Fn(ConnectionState)>>>,
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
                            self.state_change_cb.read().unwrap()(ConnectionState::Disconnected);
                        }
                        Command::Logout => {
                            self.state = State::NoValidToken;
                            self.state_change_cb.read().unwrap()(ConnectionState::NoValidToken);
                        }
                    }
                }
            }

            if let State::Disconnected { url, token } = self.state {
                unimplemented!() // TODO(api)
            }
        }
    }
}
