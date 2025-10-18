use std::error::Error;
use std::fmt::Display;

pub type Result<T> = std::result::Result<T, ServiceError>;

#[derive(Debug)]
pub enum ServiceError {
    Internal(String),
}

impl ServiceError {
    pub fn internal(msg: String) -> Self {
        Self::Internal(msg)
    }
}

impl<E: Error> From<E> for ServiceError {
    fn from(value: E) -> Self {
        Self::Internal(value.to_string())
    }
}

impl Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val = match &self {
            ServiceError::Internal(msg) => msg,
        };

        write!(f, "{}", val)
    }
}
