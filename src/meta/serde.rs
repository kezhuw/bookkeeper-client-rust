use bytes::BufMut;
use const_format::formatcp;
use guard::guard;
use prost::Message;

use super::types::BookieServiceInfo;
use crate::client::errors::{BkError, ErrorKind};
use crate::client::{BookieId, LedgerId, LedgerMetadata};
use crate::proto::*;

pub fn deserialize_bookie_service_info(
    bookie_path: &str,
    path: &[u8],
    bytes: &[u8],
) -> Result<BookieServiceInfo, BkError> {
    let bookie_id = deserialize_bookie_id(bookie_path, path)?;
    if bytes.is_empty() {
        return BookieServiceInfo::from_legacy(bookie_id);
    }
    BookieServiceInfo::from_protobuf(bookie_id, bytes)
}

pub fn deserialize_bookie_id(bookie_path: &str, bytes: &[u8]) -> Result<BookieId, BkError> {
    if bytes.len() <= bookie_path.len() {
        let err = BkError::with_description(ErrorKind::MetaUnexpectedResponse, &"not valid bookie path");
        return Err(err);
    }
    if let Ok(s) = std::str::from_utf8(&bytes[bookie_path.len()..]) {
        return Ok(BookieId::new(s));
    };
    let err = BkError::with_description(ErrorKind::InvalidMetadata, &"bookie id not utf8");
    Err(err)
}

const METADATA_VERSION_PREFIX: &str = "BookieMetadataFormatVersion\t";
const METADATA_VERSION_TERMINATOR: u8 = b'\n';

fn ledger_metadata_buf(version: i32) -> Vec<u8> {
    assert_eq!(version, 3);
    let version_str: &str = formatcp!("{}", 3i32);
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    buf.put_slice(METADATA_VERSION_PREFIX.as_bytes());
    buf.put_slice(version_str.as_bytes());
    buf.put_u8(METADATA_VERSION_TERMINATOR);
    buf
}

pub fn serialize_ledger_metadata(metadata: &LedgerMetadata) -> Result<Vec<u8>, BkError> {
    if metadata.format_version != 3 {
        let err = BkError::with_description(ErrorKind::MetaUnexpectedResponse, &"unnsupported metadata format version");
        return Err(err);
    }
    let metadata_pb: LedgerMetadataFormat = metadata.clone().into();
    let mut buf = ledger_metadata_buf(3);
    metadata_pb.encode_length_delimited(&mut buf).unwrap();
    Ok(buf)
}

pub fn deserialize_ledger_metadata(ledger_id: LedgerId, bytes: &[u8]) -> Result<LedgerMetadata, BkError> {
    let mut splits = bytes.splitn(2, |u| *u == METADATA_VERSION_TERMINATOR);
    let version_line = splits.next().unwrap();
    guard!(let Some(metadata_bytes) = splits.next() else {
        return Err(BkError::with_description(ErrorKind::MetaInvalidData, &"no ledger metadata"));
    });
    if !version_line.starts_with(METADATA_VERSION_PREFIX.as_bytes()) {
        return Err(BkError::with_description(ErrorKind::MetaInvalidData, &"invalid ledger metadata"));
    }
    guard!(let Ok(version_str) = std::str::from_utf8(&version_line[METADATA_VERSION_PREFIX.len()..]) else {
        return Err(BkError::with_description(ErrorKind::MetaInvalidData, &"invalid ledger metadata format version"));
    });
    guard!(let Ok(version) = version_str.parse::<i32>() else {
        return Err(BkError::with_description(ErrorKind::MetaInvalidData, &"invalid ledger metadata format version"));
    });

    if version != 3 {
        return Err(BkError::with_description(ErrorKind::MetaInvalidData, &"unsupported metadata version"));
    }
    guard!(let Ok(metadata_pb) = LedgerMetadataFormat::decode_length_delimited(metadata_bytes) else {
        return Err(BkError::with_description(ErrorKind::MetaInvalidData, &"invalid serialized ledger metadata"));
    });
    let metadata: LedgerMetadata = LedgerMetadata { ledger_id, format_version: version, ..metadata_pb.try_into()? };
    Ok(metadata)
}
