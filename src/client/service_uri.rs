use std::str::FromStr;

use compact_str::CompactStr;

use super::errors::{BkError, ErrorKind};

pub struct ServiceUri {
    pub scheme: CompactStr,
    pub spec: CompactStr,
    pub address: CompactStr,
    pub path: CompactStr,
}

impl FromStr for ServiceUri {
    type Err = BkError;

    fn from_str(mut s: &str) -> Result<ServiceUri, BkError> {
        let leading = match s.find("://") {
            None => return Err(BkError::new(ErrorKind::InvalidServiceUri)),
            Some(i) => {
                let leading = &s[0..i];
                s = &s[i + 3..];
                leading
            },
        };
        let (scheme, spec) = match leading.find('+') {
            None => (leading, Default::default()),
            Some(i) => (&leading[0..i], &leading[i + 1..]),
        };
        let address = match s.find('/') {
            None => return Err(BkError::new(ErrorKind::InvalidServiceUri)),
            Some(i) => {
                let address = &s[0..i];
                s = &s[i..];
                address
            },
        };
        let path = s;
        if scheme.is_empty() || address.is_empty() {
            return Err(BkError::new(ErrorKind::InvalidServiceUri));
        }
        Ok(ServiceUri { scheme: scheme.into(), spec: spec.into(), address: address.into(), path: path.into() })
    }
}
