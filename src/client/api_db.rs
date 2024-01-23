use futures::{channel::mpsc, StreamExt};

use crate::{
    db_trait::{DynNewEvent, DynNewObject, DynNewRecreation},
    full_object::FullObject,
    BinPtr, CrdbStream, EventId, Object, ObjectId, Query, SessionToken, Timestamp,
};
use std::sync::{Arc, RwLock};

enum Command {
    Login {
        url: Arc<String>,
        token: SessionToken,
    },
    Logout,
}

pub struct ApiDb {
    connection: mpsc::UnboundedSender<Command>,
    connection_state_change_cb: Arc<RwLock<Box<dyn Send + Sync + Fn(ConnectionState)>>>,
    new_objects_receiver: async_broadcast::InactiveReceiver<DynNewObject>,
    new_events_receiver: async_broadcast::InactiveReceiver<DynNewEvent>,
    new_recreations_receiver: async_broadcast::InactiveReceiver<DynNewRecreation>,
}

impl ApiDb {
    pub fn new() -> ApiDb {
        let (mut new_objects_sender, new_objects_receiver) = async_broadcast::broadcast(128);
        let (mut new_events_sender, new_events_receiver) = async_broadcast::broadcast(128);
        let (mut new_recreations_sender, new_recreations_receiver) =
            async_broadcast::broadcast(128);
        new_objects_sender.set_await_active(false);
        new_events_sender.set_await_active(false);
        new_recreations_sender.set_await_active(false);
        let connection_state_change_cb = Arc::new(RwLock::new(Box::new(|_| ()) as _));
        let (connection, commands) = mpsc::unbounded();
        crate::spawn(
            Connection {
                commands,
                state: State::NoValidToken,
                state_change_cb: connection_state_change_cb.clone(),
                new_events_sender,
                new_objects_sender,
                new_recreations_sender,
            }
            .run(),
        );
        ApiDb {
            connection,
            connection_state_change_cb,
            new_objects_receiver: new_objects_receiver.deactivate(),
            new_events_receiver: new_events_receiver.deactivate(),
            new_recreations_receiver: new_recreations_receiver.deactivate(),
        }
    }

    pub fn on_connection_state_change(&self, cb: impl 'static + Send + Sync + Fn(ConnectionState)) {
        *self.connection_state_change_cb.write().unwrap() = Box::new(cb);
    }

    pub fn login(&self, url: Arc<String>, token: SessionToken) {
        self.connection
            .unbounded_send(Command::Login { url, token })
            .expect("connection cannot go away before sender does")
    }

    pub fn logout(&self) {
        self.connection
            .unbounded_send(Command::Logout)
            .expect("connection cannot go away before sender does")
    }

    pub async fn new_objects(&self) -> impl CrdbStream<Item = DynNewObject> {
        self.new_objects_receiver.activate_cloned()
    }

    /// This function returns all new events for events on objects that have been subscribed
    /// on. Objects subscribed on are all the objects that have ever been created with `create`,
    /// or obtained with `get` or `query` and subscribed on, excluding objects explicitly
    /// unsubscribed from
    pub async fn new_events(&self) -> impl CrdbStream<Item = DynNewEvent> {
        self.new_events_receiver.activate_cloned()
    }

    pub async fn new_recreations(&self) -> impl CrdbStream<Item = DynNewRecreation> {
        self.new_recreations_receiver.activate_cloned()
    }

    /// Note that this function unsubscribes ALL the streams that have ever been taken for
    /// this object; and purges it from the local database.
    pub async fn unsubscribe(&self, _ptr: ObjectId) -> crate::Result<()> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn create<T: Object>(
        &self,
        _id: ObjectId,
        _created_at: EventId,
        _object: Arc<T>,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn submit<T: Object>(
        &self,
        _object: ObjectId,
        _event_id: EventId,
        _event: Arc<T::Event>,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn get<T: Object>(&self, _object_id: ObjectId) -> crate::Result<FullObject> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn query<T: Object>(
        &self,
        _ignore_not_modified_on_server_since: Option<Timestamp>,
        _q: &Query,
    ) -> crate::Result<impl CrdbStream<Item = crate::Result<FullObject>>> {
        // unimplemented!() // TODO(api): implement
        Ok(futures::stream::empty())
    }

    pub async fn recreate<T: Object>(
        &self,
        _time: Timestamp,
        _object: ObjectId,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn create_binary(
        &self,
        _binary_id: BinPtr,
        _data: Arc<Vec<u8>>,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn get_binary(&self, _binary_id: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        unimplemented!() // TODO(api): implement
    }
}

pub enum ConnectionState {
    Connected,
    Disconnected,
    NoValidToken,
}

enum State {
    NoValidToken,
    Disconnected {
        url: Arc<String>,
        token: SessionToken,
    },
    Connected {
        url: Arc<String>,
        token: SessionToken,
        // TODO(api): keep running websocket feed
    },
}

struct Connection {
    state: State,
    commands: mpsc::UnboundedReceiver<Command>,
    state_change_cb: Arc<RwLock<Box<dyn Send + Sync + Fn(ConnectionState)>>>,
    new_objects_sender: async_broadcast::Sender<DynNewObject>,
    new_events_sender: async_broadcast::Sender<DynNewEvent>,
    new_recreations_sender: async_broadcast::Sender<DynNewRecreation>,
}

impl Connection {
    async fn run(mut self) {
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
