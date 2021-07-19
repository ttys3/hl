// std imports
use std::boxed::Box;
use std::io;
use std::num::{ParseIntError, TryFromIntError};
use std::path::PathBuf;

// third-party imports
use ansi_term::Colour;
use config::ConfigError;
use thiserror::Error;

/// Error is an error which may occur in the application.
#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    ParseIntError(#[from] ParseIntError),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
    #[error("failed to load configuration: {0}")]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Infallible(#[from] std::convert::Infallible),
    #[error(transparent)]
    Capnp(#[from] capnp::Error),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    #[error(transparent)]
    Boxed(#[from] Box<dyn std::error::Error + std::marker::Send>),
    #[error("file {filename:?} not found")]
    FileNotFoundError { filename: String },
    #[error("invalid level {value:?}, use any of {valid_values:?}")]
    InvalidLevel {
        value: String,
        valid_values: Vec<String>,
    },
    #[error("invalid field kind {value:?}, use any of {valid_values:?}")]
    InvalidFieldKind {
        value: String,
        valid_values: Vec<String>,
    },
    #[error(
        "invalid size {0:?}, use {:?} or {:?} format for IEC units or {:?} format for SI units",
        "64K",
        "64KiB",
        "64KB"
    )]
    InvalidSize(String),
    #[error("cannot recognize time {0:?}")]
    UnrecognizedTime(String),
    #[error("unknown theme {name:?}, use any of {known:?}")]
    UnknownTheme { name: String, known: Vec<String> },
    #[error("zero size")]
    ZeroSize,
    #[error("failed to parse utf-8 string: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("failed to parse yaml: {0}")]
    YamlError(#[from] serde_yaml::Error),
    #[error("inconsistent index: {details}")]
    InconsistentIndex { details: String },
    #[error("failed to open file '{}' for reading: {source}", HILITE.paint(.path.to_string_lossy()))]
    FailedToOpenFileForReading {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to open file '{}' for writing: {source}", HILITE.paint(.path.to_string_lossy()))]
    FailedToOpenFileForWriting {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to get metadata of file '{}': {source}", HILITE.paint(.path.to_string_lossy()))]
    FailedToGetFileMetadata {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("invalid index header")]
    InvalidIndexHeader,
}

/// Result is an alias for standard result with bound Error type.
pub type Result<T> = std::result::Result<T, Error>;

pub const HILITE: Colour = Colour::Yellow;
