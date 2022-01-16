use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::{Arc, Mutex};

use guard::guard;
use ignore_result::Ignore;
use tokio::sync::oneshot;
use tokio::{select, task};

use crate::client::errors::BkError;
use crate::client::BookieId;
use crate::meta::types::{BookieRegistrationClient, BookieServiceInfo, BookieUpdate, BookieUpdateStream};

#[derive(Clone)]
struct BookieRegistryState {
    version: i64,
    writable_bookies: HashMap<BookieId, BookieServiceInfo>,
    readable_bookies: HashMap<BookieId, BookieServiceInfo>,
}

#[derive(Clone)]
pub struct BookieRegistrySnapshot {
    state: Arc<BookieRegistryState>,
}

struct BookieRegistryLifetime(MaybeUninit<oneshot::Sender<()>>);

impl Drop for BookieRegistryLifetime {
    fn drop(&mut self) {
        let sender = unsafe { ptr::read(self.0.as_ptr()) };
        sender.send(()).ignore();
    }
}

#[derive(Clone)]
pub struct BookieRegistry {
    snapshot: Arc<Mutex<BookieRegistrySnapshot>>,
    _lifetime: Arc<BookieRegistryLifetime>,
}

impl BookieRegistry {
    pub fn with_bookies(bookies: &str) -> Result<BookieRegistry, BkError> {
        let result: Result<HashMap<BookieId, BookieServiceInfo>, BkError> = bookies
            .split(',')
            .map(BookieId::new)
            .map(|bookie_id| match BookieServiceInfo::from_legacy(bookie_id) {
                Ok(bookie) => Ok((bookie.bookie_id.clone(), bookie)),
                Err(err) => Err(err),
            })
            .collect();
        let bookies = result?;
        let (sender, _) = oneshot::channel();
        let snapshot = BookieRegistrySnapshot {
            state: Arc::new(BookieRegistryState {
                version: 0,
                writable_bookies: bookies,
                readable_bookies: HashMap::new(),
            }),
        };
        let registry = BookieRegistry {
            snapshot: Arc::new(Mutex::new(snapshot)),
            _lifetime: Arc::new(BookieRegistryLifetime(MaybeUninit::new(sender))),
        };
        Ok(registry)
    }

    pub fn snapshot(&self) -> BookieRegistrySnapshot {
        let state = Self::extract_state(&self.snapshot);
        BookieRegistrySnapshot { state }
    }

    pub fn update(&self, snapshot: &mut BookieRegistrySnapshot) {
        if let Some(state) = self.get_newer_state(snapshot.state.version) {
            snapshot.state = state;
        }
    }

    fn get_newer_state(&self, version: i64) -> Option<Arc<BookieRegistryState>> {
        let snapshot = self.snapshot.lock().unwrap();
        if snapshot.state.version > version {
            return Some(snapshot.state.clone());
        }
        None
    }

    fn extract_state(snapshot: &Mutex<BookieRegistrySnapshot>) -> Arc<BookieRegistryState> {
        return snapshot.lock().unwrap().state.clone();
    }

    fn update_bookie(bookies: &mut HashMap<BookieId, BookieServiceInfo>, update: BookieUpdate) {
        match update {
            BookieUpdate::Remove(bookie_id) => {
                bookies.remove(&bookie_id);
            },
            BookieUpdate::Add(bookie) => {
                bookies.insert(bookie.bookie_id.clone(), bookie);
            },
            BookieUpdate::Reconstruction(new_bookies) => {
                let mut new_bookies: HashMap<BookieId, BookieServiceInfo> =
                    new_bookies.into_iter().map(|bookie| (bookie.bookie_id.clone(), bookie)).collect();
                std::mem::swap(bookies, &mut new_bookies);
            },
        };
    }

    fn start(
        snapshot: Arc<Mutex<BookieRegistrySnapshot>>,
        mut receiver: oneshot::Receiver<()>,
        mut writable_updates: Box<dyn BookieUpdateStream>,
        mut readable_updates: Box<dyn BookieUpdateStream>,
    ) {
        task::spawn(async move {
            loop {
                select! {
                    _ = &mut receiver => {
                        break;
                    },
                    update = writable_updates.next() => {
                        guard!(let Ok(result) = update else {
                            continue;
                        });
                        let mut state = Self::extract_state(&snapshot).as_ref().clone();
                        state.version += 1;
                        Self::update_bookie(&mut state.writable_bookies, result);
                        let state = Arc::new(state);
                        let mut snapshot = snapshot.lock().unwrap();
                        snapshot.state = state;
                    },
                    update = readable_updates.next() => {
                        guard!(let Ok(result) = update else {
                            continue;
                        });
                        let mut state = Self::extract_state(&snapshot).as_ref().clone();
                        state.version += 1;
                        Self::update_bookie(&mut state.readable_bookies, result);
                        let state = Arc::new(state);
                        let mut snapshot = snapshot.lock().unwrap();
                        snapshot.state = state;
                    },
                }
            }
        });
    }

    pub async fn new<T: BookieRegistrationClient>(bookie_client: &mut T) -> Result<BookieRegistry, BkError> {
        let (initial_writable_bookies, writable_bookies_stream) = bookie_client.watch_writable_bookies().await?;
        let (initial_readable_bookies, readable_bookies_stream) = bookie_client.watch_readable_bookies().await?;
        let state = Arc::new(BookieRegistryState {
            version: 1,
            writable_bookies: initial_writable_bookies
                .into_iter()
                .map(|bookie| (bookie.bookie_id.clone(), bookie))
                .collect(),
            readable_bookies: initial_readable_bookies
                .into_iter()
                .map(|bookie| (bookie.bookie_id.clone(), bookie))
                .collect(),
        });
        let (sender, receiver) = oneshot::channel();
        let snapshot = Arc::new(Mutex::new(BookieRegistrySnapshot { state }));
        Self::start(snapshot.clone(), receiver, writable_bookies_stream, readable_bookies_stream);
        Ok(BookieRegistry { snapshot, _lifetime: Arc::new(BookieRegistryLifetime(MaybeUninit::new(sender))) })
    }
}

impl BookieRegistrySnapshot {
    pub fn get_service_info(&self, bookie_id: &str) -> Option<&BookieServiceInfo> {
        return self.state.writable_bookies.get(bookie_id).or_else(|| self.state.readable_bookies.get(bookie_id));
    }

    pub fn writable_bookies(&self) -> &HashMap<BookieId, BookieServiceInfo> {
        &self.state.writable_bookies
    }
}
