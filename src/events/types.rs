use std::fmt::Debug;
use std::fmt::Display;
use strum::AsRefStr;
use strum::Display as StrumDisplay;

/// Type of Kubernetes event
#[derive(Debug, Clone, Copy, PartialEq, Eq, StrumDisplay, AsRefStr)]
pub enum EventType {
    /// Normal events represent informational messages about successful operations
    Normal,
    /// Warning events represent errors, failures, or issues that need attention
    Warning,
}

/// Trait for event reasons - allows domain-specific event reasons to be used with the generic event system
///
/// Implement this trait for your own event reason enums to integrate with Kubernetes event emission.
/// Event reasons should follow Kubernetes conventions (CamelCase).
///
/// Use `#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, AsRefStr)]` from strum to automatically
/// implement this trait for your enum.
pub trait Reason: Debug + Display + AsRef<str> + Clone + Send + Sync + 'static {}

/// Data structure representing a Kubernetes event, generic over the reason type
#[derive(Debug)]
pub struct EventData<R: Reason> {
    pub type_: EventType,
    pub reason: R,
    pub message: String,
    pub action: Option<String>,
}

impl<R: Reason> EventData<R> {
    /// Create a Normal event
    pub fn normal(reason: R, message: impl Into<String>) -> Self {
        Self {
            type_: EventType::Normal,
            reason,
            message: message.into(),
            action: None,
        }
    }

    /// Create a Warning event
    pub fn warning(reason: R, message: impl Into<String>) -> Self {
        Self {
            type_: EventType::Warning,
            reason,
            message: message.into(),
            action: None,
        }
    }

    /// Add an optional action to the event
    pub fn with_action(mut self, action: impl Into<String>) -> Self {
        self.action = Some(action.into());
        self
    }
}
