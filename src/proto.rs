#![allow(clippy::all)]

tonic::include_proto!("bookkeeper");

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use ledger_metadata_format::{CMetadataMapEntry, DigestType, Segment, State};

use super::client;
use super::client::errors::{BkError, ErrorKind};
use super::client::{BookieId, EntryId, LedgerEnsemble, LedgerId, LedgerMetadata, LedgerState};

impl TryFrom<i32> for State {
    type Error = BkError;

    fn try_from(i: i32) -> Result<State, BkError> {
        let state = match i {
            1 => State::Open,
            2 => State::InRecovery,
            3 => State::Closed,
            _ => {
                let err =
                    BkError::with_message(ErrorKind::InvalidMetadata, format!("invalid ledger state number {}", i));
                return Err(err);
            },
        };
        Ok(state)
    }
}

impl TryFrom<i32> for DigestType {
    type Error = BkError;

    fn try_from(i: i32) -> Result<DigestType, BkError> {
        let digest_type = match i {
            1 => DigestType::Crc32,
            2 => DigestType::Hmac,
            3 => DigestType::Crc32c,
            4 => DigestType::Dummy,
            _ => {
                let err = BkError::with_message(ErrorKind::InvalidMetadata, format!("invalid digest type {i}"));
                return Err(err);
            },
        };
        Ok(digest_type)
    }
}

impl From<LedgerState> for State {
    fn from(state: LedgerState) -> State {
        match state {
            LedgerState::Closed => State::Closed,
            LedgerState::InRecovery => State::InRecovery,
            LedgerState::Open => State::Open,
        }
    }
}

impl From<State> for LedgerState {
    fn from(state: State) -> LedgerState {
        match state {
            State::Closed => LedgerState::Closed,
            State::InRecovery => LedgerState::InRecovery,
            State::Open => LedgerState::Open,
        }
    }
}

impl From<client::DigestType> for DigestType {
    fn from(r#type: client::DigestType) -> DigestType {
        match r#type {
            client::DigestType::CRC32 => DigestType::Crc32,
            client::DigestType::MAC => DigestType::Hmac,
            client::DigestType::CRC32C => DigestType::Crc32c,
            client::DigestType::DUMMY => DigestType::Dummy,
        }
    }
}

impl From<DigestType> for client::DigestType {
    fn from(r#type: DigestType) -> client::DigestType {
        match r#type {
            DigestType::Crc32 => client::DigestType::CRC32,
            DigestType::Hmac => client::DigestType::MAC,
            DigestType::Crc32c => client::DigestType::CRC32C,
            DigestType::Dummy => client::DigestType::DUMMY,
        }
    }
}

fn to_custom_metadata(entries: Vec<CMetadataMapEntry>) -> HashMap<String, Vec<u8>> {
    entries
        .into_iter()
        .filter_map(|entry| {
            let key = match entry.key {
                None => return None,
                Some(key) => key,
            };
            let value = match entry.value {
                None => return None,
                Some(value) => value,
            };
            Some((key, value))
        })
        .collect()
}

fn to_ledger_ensembles(segments: Vec<Segment>) -> Vec<LedgerEnsemble> {
    segments
        .into_iter()
        .map(|s| LedgerEnsemble {
            first_entry_id: EntryId(s.first_entry_id),
            bookies: s.ensemble_member.into_iter().map(|m| BookieId::new(&m)).collect(),
        })
        .collect()
}

fn to_ledger_segments(ensembles: Vec<LedgerEnsemble>) -> Vec<Segment> {
    ensembles
        .into_iter()
        .map(|e| Segment {
            first_entry_id: e.first_entry_id.into(),
            ensemble_member: e.bookies.into_iter().map(|b| b.as_str().to_string()).collect(),
        })
        .collect()
}

fn to_proto_custom_metadata(custom: HashMap<String, Vec<u8>>) -> Vec<CMetadataMapEntry> {
    custom.into_iter().map(|(key, value)| CMetadataMapEntry { key: Some(key), value: Some(value) }).collect()
}

impl TryFrom<LedgerMetadataFormat> for LedgerMetadata {
    type Error = BkError;

    fn try_from(format: LedgerMetadataFormat) -> Result<LedgerMetadata, BkError> {
        let state: State = format.state.try_into()?;
        let digest_type: DigestType = format.digest_type.unwrap_or(DigestType::Dummy as i32).try_into()?;
        let metadata = LedgerMetadata {
            format_version: 3,

            ledger_id: LedgerId(0),
            ensemble_size: format.ensemble_size as u32,
            write_quorum_size: format.quorum_size as u32,
            ack_quorum_size: format.ack_quorum_size.map(|n| n as u32).unwrap_or(format.quorum_size as u32),
            password: format.password.unwrap_or_default(),
            custom_metadata: to_custom_metadata(format.custom_metadata),
            ensembles: to_ledger_ensembles(format.segment),

            state: state.into(),
            length: format.length.into(),
            last_entry_id: EntryId(format.last_entry_id.unwrap_or(-1i64)),
            digest_type: digest_type.into(),
            creator_token: format.c_token.unwrap_or(0),
            creation_time: format.ctime.map(|t| SystemTime::UNIX_EPOCH + Duration::from_millis(t as u64)),
        };
        Ok(metadata)
    }
}

impl From<LedgerMetadata> for LedgerMetadataFormat {
    fn from(metadata: LedgerMetadata) -> LedgerMetadataFormat {
        let state: State = metadata.state.into();
        let digest_type: DigestType = metadata.digest_type.into();
        LedgerMetadataFormat {
            ensemble_size: metadata.ensemble_size as i32,
            quorum_size: metadata.write_quorum_size as i32,
            ack_quorum_size: Some(metadata.ack_quorum_size as i32),
            custom_metadata: to_proto_custom_metadata(metadata.custom_metadata),
            segment: to_ledger_segments(metadata.ensembles),
            password: Some(metadata.password),
            c_token: if metadata.creator_token == 0 { None } else { Some(metadata.creator_token) },
            ctime: metadata.creation_time.map(|t| t.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as i64),
            state: state.into(),
            digest_type: Some(digest_type as i32),
            length: metadata.length.into(),
            last_entry_id: Some(metadata.last_entry_id.into()),
        }
    }
}
