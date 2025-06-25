//! Error returned from get operations

use iroh::endpoint::{self, ClosedStream};
use n0_snafu::SpanTrace;
use nested_enum_utils::common_fields;
use snafu::{Backtrace, Snafu};

use crate::api::ExportBaoError;

/// Failures for a get operation
#[common_fields({
    backtrace: Option<Backtrace>,
    #[snafu(implicit)]
    span_trace: SpanTrace,
})]
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum GetError {

    #[snafu(transparent)]
    ExportBao {
        source: ExportBaoError,
    },

    #[snafu(transparent)]
    IrpcError {
        source: irpc::Error,
    },

    #[snafu(transparent)]
    IrpcSendError {
        source: irpc::channel::SendError,
    },

    #[snafu(transparent)]
    IrpcRecvError {
        source: irpc::channel::RecvError,
    },

    TokioSendError {},

    #[snafu(transparent)]
    ClosedStream { source: ClosedStream },

    #[snafu(transparent)]
    Io { source: std::io::Error },

    /// Our download request is invalid.
    #[snafu(display("Our download request is invalid"))]
    BadRequest { source: anyhow::Error },

    PostcardSer {
        source: postcard::Error,
    },

    /// Operation failed on the local node.
    #[snafu(display("Operation failed on the local node"))]
    LocalFailure { source: anyhow::Error },

    #[snafu(transparent)]
    ConnectionError {
        source: endpoint::ConnectionError,
    },

    #[snafu(transparent)]
    ReadError {
        source: endpoint::ReadError,
    },

    #[snafu(transparent)]
    WriteError {
        source: endpoint::WriteError,
    },

    #[snafu(transparent)]
    ConnectedNextError {
        source: crate::get::fsm::ConnectedNextError,
    },

    #[snafu(transparent)]
    AtBlobHeaderNextError {
        source: crate::get::fsm::AtBlobHeaderNextError,
    },

    #[snafu(transparent)]
    DecodeError {
        source: crate::get::fsm::DecodeError,
    }
}

pub type GetResult<T> = std::result::Result<T, GetError>;