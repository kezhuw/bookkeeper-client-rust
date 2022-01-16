use std::collections::HashMap;

use async_trait::async_trait;
use compact_str::CompactStr;
use either::Either;
use prost::Message;

use crate::client::errors::{BkError, BkResult, ErrorKind};
use crate::client::metadata::{BookieId, HasLedgerMetadata, LedgerId, LedgerMetadata};
use crate::proto::*;

#[derive(Clone, Debug)]
pub struct Versioned<T> {
    pub value: T,
    pub version: MetaVersion,
}

impl<T> Versioned<T>
where
    T: Clone + std::fmt::Debug,
{
    pub fn new<V: Into<MetaVersion>>(version: V, value: T) -> Versioned<T> {
        Versioned { version: version.into(), value }
    }
}

impl HasLedgerMetadata for Versioned<LedgerMetadata> {
    fn metadata(&self) -> &LedgerMetadata {
        &self.value
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct MetaVersion(pub(crate) i64);

impl From<i64> for MetaVersion {
    fn from(u: i64) -> Self {
        MetaVersion(u)
    }
}

impl From<MetaVersion> for i64 {
    fn from(version: MetaVersion) -> i64 {
        version.0
    }
}

#[derive(Clone)]
pub struct BookieServiceInfo {
    pub bookie_id: BookieId,
    pub properties: HashMap<String, String>,
    pub endpoints: Vec<BookieEndpoint>,
}

impl BookieServiceInfo {
    pub fn rpc_endpoint(&self) -> Option<&BookieEndpoint> {
        self.endpoint_for_protocol("bookie-rpc")
    }

    pub fn endpoint_for_protocol(&self, protocol: &str) -> Option<&BookieEndpoint> {
        return self.endpoints.iter().find(|bookie| bookie.protocol == protocol);
    }

    pub fn from_legacy(bookie_id: BookieId) -> Result<BookieServiceInfo, BkError> {
        let parts: Vec<&str> = bookie_id.as_str().split(':').collect();
        if parts.len() != 2 {
            let err =
                BkError::with_message(ErrorKind::MetaInvalidData, format!("invalid legacy bookie id {}", bookie_id));
            return Err(err);
        }
        let port: u16 = match parts[1].parse() {
            Err(_) => {
                let err = BkError::with_message(
                    ErrorKind::MetaUnexpectedResponse,
                    format!("invalid legacy bookie id {}", bookie_id),
                );
                return Err(err);
            },
            Ok(port) => port,
        };
        let host = parts[0].into();
        Ok(BookieServiceInfo {
            bookie_id,
            endpoints: vec![BookieEndpoint { id: "bookie".into(), host, port, protocol: "bookie-rpc".into() }],
            properties: HashMap::new(),
        })
    }

    pub fn from_protobuf(bookie_id: BookieId, bytes: &[u8]) -> Result<BookieServiceInfo, BkError> {
        let bookie_info_pb = match BookieServiceInfoFormat::decode(bytes) {
            Err(_) => {
                let err = BkError::with_description(ErrorKind::MetaInvalidData, &"invalid bookie info format");
                return Err(err);
            },
            Ok(bookie) => bookie,
        };
        Ok(BookieServiceInfo {
            bookie_id,
            endpoints: bookie_info_pb.endpoints.into_iter().map(|endpoint| endpoint.into()).collect(),
            properties: bookie_info_pb.properties,
        })
    }
}

#[derive(Clone)]
pub struct BookieEndpoint {
    pub id: CompactStr,
    pub host: CompactStr,
    pub port: u16,
    pub protocol: CompactStr,
}

impl From<bookie_service_info_format::Endpoint> for BookieEndpoint {
    fn from(endpoint: bookie_service_info_format::Endpoint) -> BookieEndpoint {
        BookieEndpoint {
            id: endpoint.id.into(),
            host: endpoint.host.into(),
            port: endpoint.port as u16,
            protocol: endpoint.protocol.into(),
        }
    }
}

pub enum BookieUpdate {
    Add(BookieServiceInfo),
    Remove(BookieId),
    Reconstruction(Vec<BookieServiceInfo>),
}

#[async_trait]
pub trait BookieUpdateStream: Send {
    async fn next(&mut self) -> BkResult<BookieUpdate>;
}

#[async_trait]
pub trait BookieRegistrationClient {
    async fn watch_readable_bookies(
        &mut self,
    ) -> Result<(Vec<BookieServiceInfo>, Box<dyn BookieUpdateStream>), BkError>;

    async fn watch_writable_bookies(
        &mut self,
    ) -> Result<(Vec<BookieServiceInfo>, Box<dyn BookieUpdateStream>), BkError>;
}

#[async_trait]
pub trait LedgerIdStoreClient {
    async fn generate_ledger_id(&self) -> Result<LedgerId, BkError>;
}

#[async_trait]
pub trait LedgerMetadataStream: Send {
    async fn cancel(&mut self);

    async fn next(&mut self) -> Result<Versioned<LedgerMetadata>, BkError>;
}

#[async_trait]
pub trait LedgerMetadataStoreClient {
    async fn create_ledger_metadata(&self, metadata: &LedgerMetadata) -> Result<MetaVersion, BkError>;

    async fn remove_ledger_metadata(
        &self,
        ledger_id: LedgerId,
        expected_version: Option<MetaVersion>,
    ) -> Result<(), BkError>;

    async fn read_ledger_metadata(&self, ledger_id: LedgerId) -> Result<Versioned<LedgerMetadata>, BkError>;

    async fn watch_ledger_metadata(
        &self,
        ledger_id: LedgerId,
        start_version: MetaVersion,
    ) -> BkResult<Box<dyn LedgerMetadataStream>>;

    async fn write_ledger_metadata(
        &self,
        metadata: &LedgerMetadata,
        expected_version: MetaVersion,
    ) -> Result<Either<Versioned<LedgerMetadata>, MetaVersion>, BkError>;
}

#[async_trait]
pub trait MetaStore: LedgerMetadataStoreClient + LedgerIdStoreClient + BookieRegistrationClient + Send + Sync {}
