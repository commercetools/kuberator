use std::error::Error as StdError;
use std::fmt::Debug;

use kube::runtime::finalizer::Error as FError;
use thiserror::Error as ThisError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(ThisError, Debug)]
pub enum Error {
    /// Any error originating from the `kube-rs` crate
    #[error("Kubernetes reported error: {source}")]
    Kube {
        #[from]
        source: kube::Error,
    },
    #[error("{0}")]
    UserInput(String),
    #[error("Unnamed k8s object")]
    UnnamedObject,
    #[error(transparent)]
    Finalizer(#[from] FinalizerError),
    #[error("RwLock poisoned: {0}")]
    RwLockPoisoned(String),
    #[error("Invalid ApiProvider configuration")]
    InvalidApiProviderConfig,
    #[error("Failed to emit event: {0}")]
    EmitEventFailed(String),

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
                Error::Finalizer(FinalizerError::CleanupFailed(format!("Failed to clean up object: {e}")))
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

#[cfg(test)]
mod tests {
    use super::*;
    use kube::runtime::finalizer::Error as FError;
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_kube_error_conversion() {
        // Given: A kube::Error
        let kube_err = kube::Error::Api(kube::core::ErrorResponse {
            status: "Failure".to_string(),
            message: "test error".to_string(),
            reason: "BadRequest".to_string(),
            code: 400,
        });

        // When: Converting to Error
        let error: Error = kube_err.into();

        // Then: Should be KubeError variant
        assert!(matches!(error, Error::Kube { .. }));
        assert!(error.to_string().contains("Kubernetes reported error"));
    }

    #[test]
    fn test_user_input_error() {
        // Given: A user input error message
        let message = "Invalid configuration".to_string();

        // When: Creating UserInputError
        let error = Error::UserInput(message.clone());

        // Then: Should display the message
        assert_eq!(error.to_string(), message);
    }

    #[test]
    fn test_unnamed_object_error() {
        // Given: An unnamed object error
        let error = Error::UnnamedObject;

        // When: Converting to string
        let error_string = error.to_string();

        // Then: Should display appropriate message
        assert_eq!(error_string, "Unnamed k8s object");
    }

    #[test]
    fn test_finalizer_error_apply_failed() {
        // Given: A FError::ApplyFailed
        let inner_error = std::io::Error::other("network timeout");
        let ferror: FError<std::io::Error> = FError::ApplyFailed(inner_error);

        // When: Converting to Error
        let error = Error::from(ferror);

        // Then: Should be Finalizer(ApplyFailed) with correct message
        match error {
            Error::Finalizer(FinalizerError::ApplyFailed(msg)) => {
                assert!(msg.contains("Failed to apply object"));
                assert!(msg.contains("network timeout"));
            }
            _ => panic!("Expected Finalizer(ApplyFailed), got {:?}", error),
        }
    }

    #[test]
    fn test_finalizer_error_cleanup_failed() {
        // Given: A FError::CleanupFailed
        let inner_error = std::io::Error::other("cleanup error");
        let ferror: FError<std::io::Error> = FError::CleanupFailed(inner_error);

        // When: Converting to Error
        let error = Error::from(ferror);

        // Then: Should be Finalizer(CleanupFailed) with correct message
        match error {
            Error::Finalizer(FinalizerError::CleanupFailed(msg)) => {
                assert!(msg.contains("Failed to clean up object"));
                assert!(msg.contains("cleanup error"));
            }
            _ => panic!("Expected Finalizer(CleanupFailed), got {:?}", error),
        }
    }

    #[test]
    fn test_finalizer_error_add_finalizer() {
        // Given: A FError::AddFinalizer with kube error
        let kube_err = kube::Error::Api(kube::core::ErrorResponse {
            status: "Failure".to_string(),
            message: "cannot add finalizer".to_string(),
            reason: "Conflict".to_string(),
            code: 409,
        });
        let ferror: FError<std::io::Error> = FError::AddFinalizer(kube_err);

        // When: Converting to Error
        let error = Error::from(ferror);

        // Then: Should be Finalizer(AddRemove)
        match error {
            Error::Finalizer(FinalizerError::AddRemove(_)) => {
                // Success
            }
            _ => panic!("Expected Finalizer(AddRemove), got {:?}", error),
        }
    }

    #[test]
    fn test_finalizer_error_remove_finalizer() {
        // Given: A FError::RemoveFinalizer with kube error
        let kube_err = kube::Error::Api(kube::core::ErrorResponse {
            status: "Failure".to_string(),
            message: "cannot remove finalizer".to_string(),
            reason: "Conflict".to_string(),
            code: 409,
        });
        let ferror: FError<std::io::Error> = FError::RemoveFinalizer(kube_err);

        // When: Converting to Error
        let error = Error::from(ferror);

        // Then: Should be Finalizer(AddRemove)
        match error {
            Error::Finalizer(FinalizerError::AddRemove(_)) => {
                // Success
            }
            _ => panic!("Expected Finalizer(AddRemove), got {:?}", error),
        }
    }

    #[test]
    fn test_finalizer_error_unnamed_object() {
        // Given: A FError::UnnamedObject
        let ferror: FError<std::io::Error> = FError::UnnamedObject;

        // When: Converting to Error
        let error = Error::from(ferror);

        // Then: Should be Finalizer(UnnamedObject)
        assert!(matches!(error, Error::Finalizer(FinalizerError::UnnamedObject)));
    }

    #[test]
    fn test_finalizer_error_invalid_finalizer() {
        // Given: A FError::InvalidFinalizer
        let ferror: FError<std::io::Error> = FError::InvalidFinalizer;

        // When: Converting to Error
        let error = Error::from(ferror);

        // Then: Should be Finalizer(InvalidFinalizer)
        assert!(matches!(error, Error::Finalizer(FinalizerError::InvalidFinalizer)));
    }

    #[test]
    fn test_poison_error_conversion() {
        // Given: A poisoned RwLock
        let lock = Arc::new(RwLock::new(42));
        let lock_clone = Arc::clone(&lock);

        // Poison the lock by panicking inside a write
        let _ = std::panic::catch_unwind(|| {
            let mut guard = lock_clone.write().unwrap();
            *guard = 100;
            panic!("poisoning the lock");
        });

        // When: Trying to acquire the poisoned lock
        let result = lock.read();
        assert!(result.is_err());

        let poison_err = result.unwrap_err();
        let error: Error = poison_err.into();

        // Then: Should be RwLockPoisoned error
        match error {
            Error::RwLockPoisoned(msg) => {
                assert!(msg.contains("poison"));
            }
            _ => panic!("Expected RwLockPoisoned, got {:?}", error),
        }
    }

    #[test]
    fn test_anyhow_error_conversion() {
        // Given: An anyhow error
        let anyhow_err = anyhow::anyhow!("something went wrong");

        // When: Converting to Error
        let error: Error = anyhow_err.into();

        // Then: Should be Anyhow variant
        assert!(matches!(error, Error::Anyhow(_)));
        assert!(error.to_string().contains("something went wrong"));
    }

    #[test]
    fn test_rwlock_poisoned_error_display() {
        // Given: A RwLockPoisoned error
        let error = Error::RwLockPoisoned("lock is poisoned".to_string());

        // When: Converting to string
        let error_string = error.to_string();

        // Then: Should display the message
        assert!(error_string.contains("RwLock poisoned"));
        assert!(error_string.contains("lock is poisoned"));
    }

    #[test]
    fn test_invalid_api_provider_config_error() {
        // Given: An InvalidApiProviderConfig error
        let error = Error::InvalidApiProviderConfig;

        // When: Converting to string
        let error_string = error.to_string();

        // Then: Should display appropriate message
        assert_eq!(error_string, "Invalid ApiProvider configuration");
    }

    #[test]
    fn test_finalizer_error_display() {
        // Given: Different finalizer errors
        let apply_failed = FinalizerError::ApplyFailed("test".to_string());
        let cleanup_failed = FinalizerError::CleanupFailed("test".to_string());
        let unnamed = FinalizerError::UnnamedObject;
        let invalid = FinalizerError::InvalidFinalizer;

        // When: Converting to strings
        // Then: Should display appropriate messages
        assert!(apply_failed.to_string().contains("Failed to apply object"));
        assert!(cleanup_failed.to_string().contains("Failed to clean up object"));
        assert_eq!(unnamed.to_string(), "Object has no name");
        assert_eq!(invalid.to_string(), "Invalid finalizer");
    }
}
