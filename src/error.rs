use std::fmt::{self, Debug, Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug)]
enum Message {
    None,
    Static(&'static &'static str),
    Dynamic(Box<str>),
}

/// Error wraps error kind with concrete message and cause.
#[derive(Clone, Debug)]
pub struct Error<T: Clone + Debug + Display> {
    kind: T,
    message: Message,
    cause: Option<Arc<dyn std::error::Error + Send + Sync + 'static>>,
}

impl<K> Error<K>
where
    K: Clone + Debug + Display,
{
    pub(crate) fn new(kind: K) -> Error<K> {
        Error { kind, message: Message::None, cause: None }
    }

    pub(crate) fn with_description(kind: K, description: &'static &'static str) -> Error<K> {
        Error { kind, message: Message::Static(description), cause: None }
    }

    pub(crate) fn with_message<S: Into<Box<str>>>(kind: K, message: S) -> Error<K> {
        Error { kind, message: Message::Dynamic(message.into()), cause: None }
    }

    pub(crate) fn cause_by<E: std::error::Error + Send + Sync + 'static>(self, e: E) -> Self {
        let cause = Arc::new(e);
        Error { cause: Some(cause), ..self }
    }

    /// Returns error kind.
    pub fn kind(&self) -> K {
        self.kind.clone()
    }
}

impl<K> Display for Error<K>
where
    K: Clone + Debug + Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        match &self.message {
            Message::None => write!(f, "{}", self.kind),
            Message::Static(message) => write!(f, "{}: {}", self.kind, message),
            Message::Dynamic(message) => write!(f, "{}: {}", self.kind, &message),
        }
    }
}

impl<K> std::error::Error for Error<K>
where
    K: Clone + Debug + Display,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            None => None,
            Some(arced) => Some(arced.as_ref()),
        }
    }
}
