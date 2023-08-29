use std::collections::{HashMap, VecDeque};
use std::convert::Into;
use std::io;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use bytes::buf::{Buf, BufMut};
use futures::future::FutureExt;
use ignore_result::Ignore;
use prost::Message;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot::{self, Sender};

use super::digest::traits::{Algorithm as _, Digester as _};
use super::digest::{Algorithm as DigestAlgorithm, Digester};
use super::errors::{BkError, ErrorKind};
use super::metadata::{BookieId, EntryId, LedgerId, LedgerLength};
use crate::future::SelectIterable;
use crate::meta::util::{BookieRegistry, BookieRegistrySnapshot};
use crate::proto::*;

pub type Result<T> = std::result::Result<T, BkError>;

static LAST_ADD_CONFIRMED: EntryId = EntryId::INVALID;

/// ledger_id + entry_id + piggyback lac + ledger length
const ENTRY_METADATA_LENGTH: usize = 32;

/// ledger_id + explicit lac
const LAC_METADATA_LENGTH: usize = 16;

const DEFERRED_WRITE: i32 = 1;
const INVALID_ENTRY_ID: i64 = -1;

const MAX_DIGEST_LENGTH: usize = 20;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
struct TxnId(u64);

impl From<u64> for TxnId {
    fn from(txn_id: u64) -> Self {
        TxnId(txn_id)
    }
}

impl From<TxnId> for u64 {
    fn from(txn_id: TxnId) -> u64 {
        txn_id.0
    }
}

pub struct PolledEntry {
    pub last_add_confirmed: EntryId,
    /// Payload could be None in timed out or bookie error.
    pub payload: Option<Vec<u8>>,
}

fn response_status_to_error(status: i32) -> BkError {
    if status == StatusCode::Enoledger as i32 {
        BkError::new(ErrorKind::LedgerNotExisted)
    } else if status == StatusCode::Enoentry as i32 {
        BkError::new(ErrorKind::EntryNotExisted)
    } else if status == StatusCode::Ebadreq as i32 {
        BkError::with_description(ErrorKind::BookieUnexpectedResponse, &"bad request")
    } else if status == StatusCode::Eio as i32 {
        BkError::new(ErrorKind::BookieIoError)
    } else if status == StatusCode::Eua as i32 {
        BkError::new(ErrorKind::UnauthorizedAccess)
    } else if status == StatusCode::Ebadversion as i32 {
        BkError::new(ErrorKind::BookieBadVersion)
    } else if status == StatusCode::Efenced as i32 {
        BkError::new(ErrorKind::LedgerFenced)
    } else if status == StatusCode::Ereadonly as i32 {
        BkError::new(ErrorKind::BookieReadOnly)
    } else if status == StatusCode::Etoomanyrequests as i32 {
        BkError::new(ErrorKind::BookieTooManyRequests)
    } else {
        BkError::with_message(ErrorKind::BookieUnexpectedResponse, format!("unknown status code {}", status))
    }
}

trait Operation {
    type Response;

    fn type_of() -> OperationType;

    fn request_of(self, txn_id: TxnId) -> Request;

    fn extract_response(response: &mut Response) -> Option<Self::Response>;

    fn extract_status(_: &Self::Response) -> i32 {
        StatusCode::Eok as i32
    }

    fn head_of(txn_id: TxnId) -> BkPacketHeader {
        BkPacketHeader {
            version: ProtocolVersion::VersionThree as i32,
            operation: Self::type_of() as i32,
            txn_id: txn_id.into(),
            ..Default::default()
        }
    }

    fn response_of(mut response: Response) -> Result<Self::Response> {
        let op_type = response.header.operation;
        if op_type != Self::type_of() as i32 {
            let msg = format!("expect response type {:?}, got {:?}", Self::type_of(), op_type);
            let err = BkError::with_message(ErrorKind::BookieUnexpectedResponse, msg);
            return Err(err);
        }
        if response.status != StatusCode::Eok as i32 {
            let err = response_status_to_error(response.status);
            return Err(err);
        }
        let Some(inner_response) = Self::extract_response(&mut response) else {
            let err = BkError::with_description(ErrorKind::BookieUnexpectedResponse, &"no response");
            return Err(err);
        };
        let status = Self::extract_status(&inner_response);
        if status != StatusCode::Eok as i32 {
            let err = response_status_to_error(response.status);
            return Err(err);
        }
        Ok(inner_response)
    }
}

impl Operation for ReadRequest {
    type Response = ReadResponse;

    fn type_of() -> OperationType {
        OperationType::ReadEntry
    }

    fn request_of(self, txn_id: TxnId) -> Request {
        Request { header: Self::head_of(txn_id), read_request: Some(self), ..Default::default() }
    }

    fn extract_response(response: &mut Response) -> Option<Self::Response> {
        response.read_response.take()
    }

    fn extract_status(response: &Self::Response) -> i32 {
        response.status
    }
}

impl Operation for AddRequest {
    type Response = AddResponse;

    fn type_of() -> OperationType {
        OperationType::AddEntry
    }

    fn request_of(self, txn_id: TxnId) -> Request {
        Request { header: Self::head_of(txn_id), add_request: Some(self), ..Default::default() }
    }

    fn extract_response(response: &mut Response) -> Option<Self::Response> {
        response.add_response.take()
    }

    fn extract_status(response: &Self::Response) -> i32 {
        response.status
    }
}

impl Operation for WriteLacRequest {
    type Response = WriteLacResponse;

    fn type_of() -> OperationType {
        OperationType::WriteLac
    }

    fn request_of(self, txn_id: TxnId) -> Request {
        Request { header: Self::head_of(txn_id), write_lac_request: Some(self), ..Default::default() }
    }

    fn extract_response(response: &mut Response) -> Option<Self::Response> {
        response.write_lac_response.take()
    }

    fn extract_status(response: &Self::Response) -> i32 {
        response.status
    }
}

impl Operation for ReadLacRequest {
    type Response = ReadLacResponse;

    fn type_of() -> OperationType {
        OperationType::ReadLac
    }

    fn request_of(self, txn_id: TxnId) -> Request {
        Request { header: Self::head_of(txn_id), read_lac_request: Some(self), ..Default::default() }
    }

    fn extract_response(response: &mut Response) -> Option<Self::Response> {
        response.read_lac_response.take()
    }

    fn extract_status(response: &Self::Response) -> i32 {
        response.status
    }
}

impl Operation for StartTlsRequest {
    type Response = StartTlsResponse;

    fn type_of() -> OperationType {
        OperationType::StartTls
    }

    fn request_of(self, txn_id: TxnId) -> Request {
        Request { header: Self::head_of(txn_id), start_tls_request: Some(self), ..Default::default() }
    }

    fn extract_response(response: &mut Response) -> Option<Self::Response> {
        response.start_tls_response.take()
    }
}

impl Operation for ForceLedgerRequest {
    type Response = ForceLedgerResponse;

    fn type_of() -> OperationType {
        OperationType::ForceLedger
    }

    fn request_of(self, txn_id: TxnId) -> Request {
        Request { header: Self::head_of(txn_id), force_ledger_request: Some(self), ..Default::default() }
    }

    fn extract_response(response: &mut Response) -> Option<Self::Response> {
        response.force_ledger_response.take()
    }

    fn extract_status(response: &Self::Response) -> i32 {
        response.status
    }
}

impl Operation for GetBookieInfoRequest {
    type Response = GetBookieInfoResponse;

    fn type_of() -> OperationType {
        OperationType::GetBookieInfo
    }

    fn request_of(self, txn_id: TxnId) -> Request {
        Request { header: Self::head_of(txn_id), get_bookie_info_request: Some(self), ..Default::default() }
    }

    fn extract_response(response: &mut Response) -> Option<Self::Response> {
        response.get_bookie_info_response.take()
    }

    fn extract_status(response: &Self::Response) -> i32 {
        response.status
    }
}

impl Operation for GetListOfEntriesOfLedgerRequest {
    type Response = GetListOfEntriesOfLedgerResponse;

    fn type_of() -> OperationType {
        OperationType::GetListOfEntriesOfLedger
    }

    fn request_of(self, txn_id: TxnId) -> Request {
        Request {
            header: Self::head_of(txn_id),
            get_list_of_entries_of_ledger_request: Some(self),
            ..Default::default()
        }
    }

    fn extract_response(response: &mut Response) -> Option<Self::Response> {
        response.get_list_of_entries_of_ledger_response.take()
    }

    fn extract_status(response: &Self::Response) -> i32 {
        response.status
    }
}

pub struct AddingEntry<'a> {
    pub ledger_id: LedgerId,
    pub entry_id: EntryId,
    pub last_add_confirmed: EntryId,
    pub accumulated_ledger_length: LedgerLength,
    pub payload: &'a [u8],
}

pub struct AddOptions<'a> {
    pub recovery_add: bool,
    pub high_priority: bool,
    pub deferred_sync: bool,
    pub master_key: &'a [u8],
    pub digest_algorithm: &'a DigestAlgorithm,
}

pub struct FetchedEntry {
    pub max_lac: EntryId,
    pub last_add_confirmed: EntryId,
    pub ledger_length: LedgerLength,
    pub payload: Vec<u8>,
}

pub struct ReadOptions<'a> {
    pub fence_ledger: bool,
    pub high_priority: bool,
    pub digest_algorithm: &'a DigestAlgorithm,
    pub master_key: Option<&'a [u8]>,
}

pub struct PollOptions<'a> {
    pub timeout: Duration,
    pub digest_algorithm: &'a DigestAlgorithm,
}

pub struct WriteLacOptions<'a> {
    pub master_key: &'a [u8],
    pub digest_algorithm: &'a DigestAlgorithm,
}

struct EntryMetadata {
    entry_id: EntryId,
    last_add_confirmed: EntryId,
    ledger_length: LedgerLength,
}

struct EntryContent {
    entry_id: EntryId,
    last_add_confirmed: EntryId,
    ledger_length: LedgerLength,
    payload: Vec<u8>,
}

struct EntryBody(Vec<u8>);

impl EntryBody {
    fn corrupted(&mut self) -> BkError {
        BkError::with_description(ErrorKind::EntryInvalidData, &"corrupted data")
    }

    fn extract_explicit_lac(&mut self, ledger_id: LedgerId, mut digester: Digester) -> Result<EntryId> {
        let digest_len = digester.digest_length();
        let prefix_len = LAC_METADATA_LENGTH + digest_len;
        let body = &self.0;
        if body.len() < prefix_len {
            return Err(self.corrupted());
        }
        let mut metadata = &body[..LAC_METADATA_LENGTH];
        digester.update(metadata);
        #[allow(clippy::uninit_assumed_init)]
        let mut buf: [u8; MAX_DIGEST_LENGTH] = unsafe { MaybeUninit::uninit().assume_init() };
        digester.digest(&mut &mut buf[..digest_len]);
        if buf[..digest_len] != body[LAC_METADATA_LENGTH..prefix_len] {
            return Err(self.corrupted());
        }
        let actual_ledger_id = metadata.get_i64();
        if ledger_id != actual_ledger_id {
            let msg = format!("expect ledger id {} in reading lac, got {}", ledger_id, actual_ledger_id);
            let err = BkError::with_message(ErrorKind::BookieUnexpectedResponse, msg);
            return Err(err);
        }
        let lac = metadata.get_i64();
        Ok(EntryId(lac))
    }

    fn verify_entry(
        &mut self,
        ledger_id: LedgerId,
        entry_id: EntryId,
        mut digester: Digester,
    ) -> Result<EntryMetadata> {
        let digest_len = digester.digest_length();
        let prefix_len = ENTRY_METADATA_LENGTH + digest_len;
        let body = &self.0;
        if body.len() < prefix_len {
            return Err(self.corrupted());
        }
        let mut metadata = &body[..ENTRY_METADATA_LENGTH];
        digester.update(metadata);
        digester.update(&body[prefix_len..]);
        #[allow(clippy::uninit_assumed_init)]
        let mut buf: [u8; MAX_DIGEST_LENGTH] = unsafe { MaybeUninit::uninit().assume_init() };
        digester.digest(&mut &mut buf[..digest_len]);
        if buf[..digest_len] != body[ENTRY_METADATA_LENGTH..prefix_len] {
            return Err(self.corrupted());
        }
        let actual_ledger_id = metadata.get_i64();
        let actual_entry_id = metadata.get_i64();
        let last_add_confirmed = metadata.get_i64();
        let accumulated_payload_len = metadata.get_i64();
        let payload_len = body.len() - prefix_len;
        if ledger_id != actual_ledger_id {
            let msg = format!("expect ledger id {} in reading lac, got {}", ledger_id, actual_ledger_id);
            let err = BkError::with_message(ErrorKind::BookieUnexpectedResponse, msg);
            return Err(err);
        }
        if entry_id != EntryId::INVALID && entry_id != actual_entry_id {
            let msg = format!("expect entry id {:?}, got {}", entry_id, actual_entry_id);
            let err = BkError::with_message(ErrorKind::BookieUnexpectedResponse, msg);
            return Err(err);
        }
        if last_add_confirmed >= actual_entry_id {
            return Err(self.corrupted());
        }
        if accumulated_payload_len < payload_len as i64 {
            let msg =
                format!("accumulated ledger length {} < entry payload length {}", accumulated_payload_len, payload_len);
            let err = BkError::with_message(ErrorKind::BookieUnexpectedResponse, msg);
            return Err(err);
        }
        Ok(EntryMetadata {
            entry_id: EntryId(actual_entry_id),
            last_add_confirmed: EntryId(last_add_confirmed),
            ledger_length: accumulated_payload_len.into(),
        })
    }

    fn extract_piggyback_lac(&mut self, ledger_id: LedgerId, entry_id: EntryId, digester: Digester) -> Result<EntryId> {
        let EntryMetadata { last_add_confirmed, .. } = self.verify_entry(ledger_id, entry_id, digester)?;
        Ok(last_add_confirmed)
    }

    fn extract_entry(mut self, ledger_id: LedgerId, entry_id: EntryId, digester: Digester) -> Result<EntryContent> {
        let digest_len = digester.digest_length();
        let EntryMetadata { entry_id: actual_entry_id, last_add_confirmed, ledger_length } =
            self.verify_entry(ledger_id, entry_id, digester)?;
        let mut body = self.0;
        let prefix_len = ENTRY_METADATA_LENGTH + digest_len;
        body.drain(..prefix_len);
        Ok(EntryContent { entry_id: actual_entry_id, last_add_confirmed, ledger_length, payload: body })
    }
}

#[derive(Debug)]
struct MessageRequest {
    txn_id: TxnId,
    bytes: Vec<u8>,
    responser: Sender<Result<Response>>,
}

async fn io_loop(sock: TcpStream, state: Arc<State>, mut request_receiver: mpsc::Receiver<MessageRequest>) {
    let mut read_buf = Vec::with_capacity(4096);
    let mut pending_requests: VecDeque<MessageRequest> = VecDeque::with_capacity(512);
    let mut pending_txns: HashMap<TxnId, oneshot::Sender<Result<Response>>> = HashMap::with_capacity(20);
    let mut io_slices: Vec<std::io::IoSlice<'_>> = Vec::with_capacity(20);
    let mut first_request_offset = 0usize;
    let err = 'io: loop {
        select! {
            _ = sock.readable() => {
                match sock.try_read_buf(&mut read_buf) {
                    Ok(0) =>  {
                        let err = BkError::with_description(ErrorKind::BookieNotAvailable, &"bookie closed");
                        break err;
                    },
                    Ok(_) => {
                        while read_buf.len() > 4 {
                            let mut len_buf = &read_buf[..4];
                            let msg_len = len_buf.get_u32() as usize + 4;
                            if read_buf.len() < msg_len {
                                continue;
                            }
                            let response_buf = &read_buf[4..msg_len];
                            let response = match Response::decode(response_buf) {
                                Err(e) => break 'io BkError::with_description(ErrorKind::BookieUnexpectedResponse, &"decode failure").cause_by(e),
                                Ok(response) => response,
                            };
                            drop(read_buf.drain(..msg_len));
                            let txn_id = TxnId(response.header.txn_id);
                            if let Some(responser) = pending_txns.remove(&txn_id) {
                                responser.send(Ok(response)).ignore();
                            }
                        }
                    },
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            continue;
                        }
                        let err = BkError::with_description(ErrorKind::BookieNotAvailable, &"bookie read failed").cause_by(e);
                        break err;
                    },
                }
            },
            r = request_receiver.recv(), if pending_requests.len() < 512 => {
                match r {
                    Some(request) => {
                        pending_requests.push_back(request);
                    },
                    None => return,
                };
            },
            _ = sock.writable(), if !pending_requests.is_empty() => {
                io_slices.clear();
                io_slices.push(std::io::IoSlice::new(unsafe {&*(&pending_requests[0].bytes[first_request_offset..] as *const [u8])}));
                pending_requests.iter().skip(1).for_each(|r| {
                    io_slices.push(std::io::IoSlice::new(unsafe {&*(&r.bytes[first_request_offset..] as *const [u8])}));
                });
                match sock.try_write_vectored(&io_slices) {
                    Ok(mut n) => {
                        if n < pending_requests[0].bytes.len() - first_request_offset {
                            first_request_offset += n;
                            continue;
                        }
                        n -= pending_requests[0].bytes.len() - first_request_offset;
                        let MessageRequest{txn_id, responser, bytes} = pending_requests.pop_front().unwrap();
                        pending_txns.insert(txn_id, responser);
                        state.release_buf(bytes);
                        first_request_offset = 0;
                        while let Some(request) = pending_requests.front() {
                            if n < request.bytes.len() {
                                first_request_offset = n;
                                break;
                            }
                            let MessageRequest{txn_id, responser, bytes} = pending_requests.pop_front().unwrap();
                            n -= bytes.len();
                            pending_txns.insert(txn_id, responser);
                            state.release_buf(bytes);
                        }
                    },
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            continue;
                        }
                        let err = BkError::with_description(ErrorKind::BookieNotAvailable, &"bookie write failed").cause_by(e);
                        break err;
                    }
                }
            },
        }
    };
    // Set error state before closing receiver to shape happens-before relation:
    // set-error ==> receiver closed ==> sending failed ==> get-error
    unsafe { state.set_err(err.clone()) };
    request_receiver.close();
    pending_requests.into_iter().for_each(|MessageRequest { responser, .. }| responser.send(Err(err.clone())).ignore());
    loop {
        let MessageRequest { responser, .. } = match request_receiver.recv().await {
            None => break,
            Some(request) => request,
        };
        responser.send(Err(err.clone())).ignore();
    }
}

struct State {
    txn_id_counter: AtomicU64,
    err: AtomicPtr<BkError>,
    buffers: Mutex<Vec<Vec<u8>>>,
}

impl State {
    fn new() -> State {
        State {
            txn_id_counter: AtomicU64::new(1),
            err: AtomicPtr::new(std::ptr::null_mut()),
            buffers: Mutex::new(Vec::with_capacity(512)),
        }
    }

    fn next_txn_id(&self) -> TxnId {
        let txn_id = self.txn_id_counter.fetch_add(1, Ordering::Relaxed);
        TxnId::from(txn_id)
    }

    fn release_buf(&self, mut buf: Vec<u8>) {
        buf.clear();
        let mut buffers = self.buffers.lock().unwrap();
        buffers.push(buf);
    }

    fn get_buf(&self) -> Vec<u8> {
        let mut buffers = self.buffers.lock().unwrap();
        let buf = buffers.pop();
        drop(buffers);
        buf.unwrap_or_else(|| Vec::with_capacity(1024))
    }

    unsafe fn get_err(&self) -> Option<BkError> {
        let ptr = self.err.load(Ordering::Relaxed);
        if ptr.is_null() {
            return None;
        }
        let err = &mut *ptr;
        Some(err.clone())
    }

    unsafe fn set_err(&self, err: BkError) {
        let err = Box::into_raw(Box::new(err));
        self.err.store(err, Ordering::Relaxed);
    }
}

impl Drop for State {
    fn drop(&mut self) {
        let err = *self.err.get_mut();
        if err.is_null() {
            return;
        }
        unsafe { Box::from_raw(err) };
    }
}

pub struct Client {
    state: Arc<State>,
    requester: mpsc::Sender<MessageRequest>,
}

pub struct Configuration {}

enum ClientRequest {
    Create { bookie_id: BookieId, responser: oneshot::Sender<Result<Arc<Client>>> },
}

impl std::fmt::Debug for ClientRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            ClientRequest::Create { bookie_id, .. } => write!(f, "client creation request for {}", bookie_id),
        }
    }
}

async fn connect(registry: BookieRegistrySnapshot, bookie_id: BookieId) -> (BookieId, Result<Client>) {
    let bookie = match registry.get_service_info(&bookie_id) {
        None => return (bookie_id, Err(BkError::with_description(ErrorKind::BookieNotAvailable, &"no service info"))),
        Some(bookie) => bookie,
    };
    let endpoint = match bookie.rpc_endpoint() {
        None => return (bookie_id, Err(BkError::with_description(ErrorKind::BookieNotAvailable, &"no rpc endpoint"))),
        Some(endpoint) => endpoint,
    };
    let client = Client::connect((endpoint.host.as_str(), endpoint.port), &Configuration {}).await;
    (bookie_id, client)
}

fn get_client(clients: &RwLock<HashMap<BookieId, Arc<Client>>>, bookie_id: &BookieId) -> Option<Arc<Client>> {
    let clients = clients.read().unwrap();
    return clients.get(bookie_id).cloned();
}

async fn client_request_loop(
    clients: Arc<RwLock<HashMap<BookieId, Arc<Client>>>>,
    mut requester: mpsc::UnboundedReceiver<ClientRequest>,
    registry: BookieRegistry,
) {
    let mut pending_requests: HashMap<BookieId, Vec<_>> = HashMap::new();
    let mut connecting_clients = VecDeque::new();
    let mut registry_snapshot = registry.snapshot();
    let mut released_responsers = Vec::new();
    loop {
        select! {
            r = requester.recv() => {
                let ClientRequest::Create {bookie_id, responser} = match r {
                    None => break,
                    Some(request) => request,
                };
                if let Some(client) = get_client(&clients, &bookie_id) {
                    responser.send(Ok(client)).ignore();
                    continue;
                }
                let entry = pending_requests.entry(bookie_id.clone());
                let responsers = entry.or_insert_with(|| released_responsers.pop().unwrap_or_default());
                if responsers.is_empty() {
                    registry.update(&mut registry_snapshot);
                    connecting_clients.push_back(connect(registry_snapshot.clone(), bookie_id).fuse());
                }
                responsers.push(responser);
            },
            (i, (bookie_id, r)) = SelectIterable::next(&mut connecting_clients) => {
                connecting_clients.swap_remove_back(i);
                let (_, mut responsers) = pending_requests.remove_entry(&bookie_id).unwrap();
                let result = r.map(Arc::new);
                responsers.drain(..).for_each(|responser| {
                    responser.send(result.clone()).ignore();
                });
                if let Ok(client) = result {
                    clients.write().unwrap().insert(bookie_id, client);
                }
                released_responsers.push(responsers);
            },
        }
    }
}

pub(crate) struct PoolledClient {
    clients: Arc<RwLock<HashMap<BookieId, Arc<Client>>>>,
    requester: mpsc::UnboundedSender<ClientRequest>,
}

impl PoolledClient {
    pub fn new(bookie_registory: BookieRegistry) -> PoolledClient {
        let clients = Arc::new(RwLock::new(HashMap::with_capacity(512)));
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(client_request_loop(clients.clone(), receiver, bookie_registory));
        PoolledClient { clients, requester: sender }
    }

    pub fn prepare_ensemble(&self, bookies: &[BookieId]) {
        let mut has_clients = vec![false; bookies.len()];
        let clients = self.clients.read().unwrap();
        for (i, bookie_id) in bookies.iter().enumerate() {
            has_clients[i] = clients.contains_key(bookie_id);
        }
        drop(clients);
        for (i, has) in has_clients.iter().enumerate() {
            if !has {
                let bookie_id = &bookies[i];
                let (sender, _) = oneshot::channel();
                self.requester.send(ClientRequest::Create { bookie_id: bookie_id.clone(), responser: sender }).unwrap();
            }
        }
    }

    fn get_client(&self, bookie_id: &BookieId) -> Option<Arc<Client>> {
        let clients = self.clients.read().unwrap();
        return clients.get(bookie_id.as_str()).cloned();
    }

    async fn create_or_get_client(&self, bookie_id: &BookieId) -> Result<Arc<Client>> {
        if let Some(client) = self.get_client(bookie_id) {
            return Ok(client);
        }
        let (sender, receiver) = oneshot::channel();
        self.requester.send(ClientRequest::Create { bookie_id: bookie_id.to_owned(), responser: sender }).unwrap();
        return receiver.await.unwrap();
    }

    #[allow(dead_code)]
    fn remove_client(&self, bookie_id: &BookieId, client: Arc<Client>) {
        let mut clients = self.clients.write().unwrap();
        match clients.get(bookie_id) {
            Some(other) if Arc::ptr_eq(&client, other) => clients.remove(bookie_id),
            _ => return,
        };
        drop(clients);
    }

    pub async fn read_last_entry(
        &self,
        bookie_id: &BookieId,
        ledger_id: LedgerId,
        options: &ReadOptions<'_>,
    ) -> Result<(EntryId, FetchedEntry)> {
        let client = self.create_or_get_client(bookie_id).await?;
        return client.read_last_entry(ledger_id, options).await;
    }

    pub async fn read_entry(
        &self,
        bookie_id: &BookieId,
        ledger_id: LedgerId,
        entry_id: EntryId,
        options: &ReadOptions<'_>,
    ) -> Result<FetchedEntry> {
        let client = self.create_or_get_client(bookie_id).await?;
        return client.read_entry(ledger_id, entry_id, options).await;
    }

    pub async fn poll_entry(
        &self,
        bookie_id: &BookieId,
        ledger_id: LedgerId,
        entry_id: EntryId,
        options: &PollOptions<'_>,
    ) -> Result<PolledEntry> {
        let client = self.create_or_get_client(bookie_id).await?;
        return client.poll_entry(ledger_id, entry_id, options).await;
    }

    pub async fn write_lac(
        &self,
        bookie_id: &BookieId,
        ledger_id: LedgerId,
        explicit_lac: EntryId,
        options: &WriteLacOptions<'_>,
    ) -> Result<()> {
        let client = self.create_or_get_client(bookie_id).await?;
        return client.write_lac(ledger_id, explicit_lac, options).await;
    }

    pub async fn read_lac(
        &self,
        bookie_id: &BookieId,
        ledger_id: LedgerId,
        digest_algorithm: &DigestAlgorithm,
    ) -> Result<EntryId> {
        let client = self.create_or_get_client(bookie_id).await?;
        return client.read_lac(ledger_id, digest_algorithm).await;
    }

    pub async fn force_ledger(&self, bookie_id: &BookieId, ledger_id: LedgerId) -> Result<()> {
        let client = self.create_or_get_client(bookie_id).await?;
        return client.force_ledger(ledger_id).await;
    }

    pub async fn add_entry(
        &self,
        bookie_id: &BookieId,
        entry: &AddingEntry<'_>,
        options: &AddOptions<'_>,
    ) -> Result<()> {
        let client = self.create_or_get_client(bookie_id).await?;
        return client.add_entry(entry, options).await;
    }
}

impl Client {
    pub async fn connect<A: ToSocketAddrs>(addr: A, _conf: &Configuration) -> Result<Client> {
        impl From<io::Error> for BkError {
            fn from(_: io::Error) -> BkError {
                BkError::with_description(ErrorKind::BookieNotAvailable, &"can't connect to bookie")
            }
        }
        let tcp_stream = TcpStream::connect(addr).await?;
        let state = Arc::new(State::new());
        let (sender, receiver) = mpsc::channel(512);
        tokio::spawn(io_loop(tcp_stream, state.clone(), receiver));
        let client = Client { state, requester: sender };
        Ok(client)
    }

    fn serialize_request(&self, request: &Request) -> Result<Vec<u8>> {
        let mut buf = self.state.get_buf();
        unsafe { buf.set_len(4) };
        if let Err(err) = request.encode(&mut buf) {
            return Err(BkError::with_description(ErrorKind::UnexpectedError, &"fail to encode request").cause_by(err));
        }
        let len = buf.len() as u32 - 4;
        let mut len_buf = &mut buf[..4];
        len_buf.put_u32(len);
        Ok(buf)
    }

    async fn send(&self, txn_id: TxnId, request: &Request) -> Result<oneshot::Receiver<Result<Response>>> {
        let bytes = self.serialize_request(request)?;
        let (sender, receiver) = oneshot::channel();
        let message = MessageRequest { txn_id, bytes, responser: sender };
        if self.requester.send(message).await.is_err() {
            let err = unsafe { self.state.get_err().expect("no error after message channel closed") };
            return Err(err);
        }
        Ok(receiver)
    }

    async fn request<T: Operation>(&self, operation: T) -> Result<T::Response> {
        let txn_id = self.state.next_txn_id();
        let request = operation.request_of(txn_id);
        let receiver = self.send(txn_id, &request).await?;
        let response = match receiver.await.expect("no response after message sent") {
            Ok(response) => response,
            Err(err) => return Err(err),
        };
        T::response_of(response)
    }

    pub async fn fetch_entry_internal(
        &self,
        ledger_id: LedgerId,
        entry_id: EntryId,
        options: &ReadOptions<'_>,
    ) -> Result<(EntryId, FetchedEntry)> {
        let flag = if options.fence_ledger { Some(read_request::Flag::FenceLedger.into()) } else { None };
        let read = ReadRequest {
            flag,
            ledger_id: ledger_id.into(),
            entry_id: entry_id.into(),
            master_key: options.master_key.map(|key| key.to_owned()),
            ..Default::default()
        };
        let response = self.request(read).await?;
        let max_lac = response.max_lac.unwrap_or(INVALID_ENTRY_ID);
        let Some(body) = response.body else {
            return Err(BkError::with_description(ErrorKind::BookieUnexpectedResponse, &"no entry body in response"));
        };
        let body = EntryBody(body);
        let digester = options.digest_algorithm.digester();
        let EntryContent { entry_id: actual_entry_id, payload, last_add_confirmed, ledger_length } =
            body.extract_entry(ledger_id, entry_id, digester)?;
        Ok((actual_entry_id, FetchedEntry {
            last_add_confirmed,
            max_lac: last_add_confirmed.max(EntryId(max_lac)),
            ledger_length,
            payload,
        }))
    }

    pub async fn read_entry(
        &self,
        ledger_id: LedgerId,
        entry_id: EntryId,
        options: &ReadOptions<'_>,
    ) -> Result<FetchedEntry> {
        let (_, entry) = self.fetch_entry_internal(ledger_id, entry_id, options).await?;
        Ok(entry)
    }

    pub async fn read_last_entry(
        &self,
        ledger_id: LedgerId,
        options: &ReadOptions<'_>,
    ) -> Result<(EntryId, FetchedEntry)> {
        return self.fetch_entry_internal(ledger_id, LAST_ADD_CONFIRMED, options).await;
    }

    pub async fn poll_entry(
        &self,
        ledger_id: LedgerId,
        entry_id: EntryId,
        options: &PollOptions<'_>,
    ) -> Result<PolledEntry> {
        let previous_lac = entry_id - 1;
        let read = ReadRequest {
            flag: Some(read_request::Flag::EntryPiggyback.into()),
            ledger_id: ledger_id.into(),
            entry_id: LAST_ADD_CONFIRMED.into(),
            previous_lac: Some(previous_lac.into()),
            time_out: Some(options.timeout.as_millis() as i64),
            ..Default::default()
        };
        let response = self.request(read).await?;
        let Some(max_lac) = response.max_lac else {
            let err = BkError::with_description(ErrorKind::BookieUnexpectedResponse, &"no last add confirmed");
            return Err(err);
        };
        if response.entry_id == LAST_ADD_CONFIRMED.into() {
            return Ok(PolledEntry { last_add_confirmed: EntryId(max_lac), payload: None });
        }
        let Some(body) = response.body else {
            let err = BkError::with_description(ErrorKind::BookieUnexpectedResponse, &"no entry body in response");
            return Err(err);
        };
        let body = EntryBody(body);
        let digester = options.digest_algorithm.digester();
        let EntryContent { payload, last_add_confirmed, .. } = body.extract_entry(ledger_id, entry_id, digester)?;
        Ok(PolledEntry { last_add_confirmed: last_add_confirmed.max(EntryId(max_lac)), payload: Some(payload) })
    }

    pub async fn write_lac(
        &self,
        ledger_id: LedgerId,
        explicit_lac: EntryId,
        options: &WriteLacOptions<'_>,
    ) -> Result<()> {
        let mut digester = options.digest_algorithm.digester();
        let mut body = Vec::with_capacity(LAC_METADATA_LENGTH + digester.digest_length());
        body.put_i64(ledger_id.into());
        body.put_i64(explicit_lac.into());
        digester.update(&body);
        digester.digest(&mut body);
        let request = WriteLacRequest {
            ledger_id: ledger_id.into(),
            lac: explicit_lac.into(),
            master_key: options.master_key.to_vec(),
            body,
        };
        self.request(request).await?;
        Ok(())
    }

    pub async fn read_lac(&self, ledger_id: LedgerId, digest_algorithm: &DigestAlgorithm) -> Result<EntryId> {
        let request = ReadLacRequest { ledger_id: ledger_id.into() };
        let response = self.request(request).await?;
        let explicit_lac = if let Some(body) = response.lac_body {
            let mut body = EntryBody(body);
            body.extract_explicit_lac(ledger_id, digest_algorithm.digester())?
        } else {
            EntryId::INVALID
        };
        let piggyback_lac = if let Some(body) = response.last_entry_body {
            let mut body = EntryBody(body);
            body.extract_piggyback_lac(ledger_id, EntryId::INVALID, digest_algorithm.digester())?
        } else {
            EntryId::INVALID
        };
        Ok(explicit_lac.max(piggyback_lac))
    }

    pub async fn force_ledger(&self, ledger_id: LedgerId) -> Result<()> {
        let request = ForceLedgerRequest { ledger_id: ledger_id.into() };
        let _response = self.request(request).await?;
        Ok(())
    }

    pub async fn add_entry(&self, entry: &AddingEntry<'_>, options: &AddOptions<'_>) -> Result<()> {
        let mut digester = options.digest_algorithm.digester();
        let mut body = Vec::with_capacity(ENTRY_METADATA_LENGTH + digester.digest_length() + entry.payload.len());
        let buf = &mut body;
        buf.put_i64(entry.ledger_id.into());
        buf.put_i64(entry.entry_id.into());
        buf.put_i64(entry.last_add_confirmed.into());
        buf.put_i64(entry.accumulated_ledger_length.into());
        digester.update(&body);
        digester.update(entry.payload);
        digester.digest(&mut body);
        body.extend_from_slice(entry.payload);

        let mut request = AddRequest {
            ledger_id: entry.ledger_id.into(),
            entry_id: entry.entry_id.into(),
            body,
            master_key: Vec::from(options.master_key),
            ..Default::default()
        };
        if options.deferred_sync {
            request.write_flags = Some(DEFERRED_WRITE);
        }
        if options.recovery_add {
            request.flag = Some(add_request::Flag::RecoveryAdd.into());
        }
        self.request(request).await?;
        Ok(())
    }
}
