use std::error::Error as StdError;
use std::fmt::Debug;

use kube::runtime::finalizer::Error as FError;
use thiserror::Error as ThisError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(ThisError, Debug)]
pub enum Error {
    /// Any error originating from the `kube-rs` crate
    #[error("Kubernetes reported error: {source}")]
    KubeError {
        #[from]
        source: kube::Error,
    },
    #[error("{0}")]
    UserInputError(String),
    #[error("Unnamed k8s object")]
    UnnamedObject,
    #[error(transparent)]
    Finalizer(#[from] FinalizerError),
    #[error("RwLock poisoned: {0}")]
    RwLockPoisoned(String),

    /// Can be used for implementors of kuberators to return their errors
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

#[derive(ThisError, Debug)]
pub enum FinalizerError {
    #[error("Failed to apply object, error: {0}")]
    ApplyFailed(String),
    #[error("Failed to clean up object: {0}")]
    CleanupFailed(String),
    #[error(transparent)]
    AddRemove(#[from] kube::Error),
    #[error("Object has no name")]
    UnnamedObject,
    #[error("Invalid finalizer")]
    InvalidFinalizer,
}

impl Error {
    pub(crate) fn from<K: StdError + 'static>(e: FError<K>) -> Self {
        match e {
            FError::ApplyFailed(e) => {
                Error::Finalizer(FinalizerError::ApplyFailed(format!("Failed to apply object: {e}")))
            }
            FError::CleanupFailed(e) => {
                Error::Finalizer(FinalizerError::CleanupFailed(format!("Failed to apply object: {e}")))
            }
            FError::AddFinalizer(e) | FError::RemoveFinalizer(e) => Error::Finalizer(FinalizerError::from(e)),
            FError::UnnamedObject => Error::Finalizer(FinalizerError::UnnamedObject),
            FError::InvalidFinalizer => Error::Finalizer(FinalizerError::InvalidFinalizer),
        }
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        Error::RwLockPoisoned(e.to_string())
    }
}
