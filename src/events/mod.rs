//! Kubernetes event emission for operators.
//!
//! This module provides generic event emission functionality for Kubernetes operators.
//! Events are observability-only and never fail reconciliation.
//!
//! # Example
//! ```rust,ignore
//! use kuberator::events::{EmitEvent, EventRecorder, EventData, Reason};
//! use kuberator::cache::StaticApiProvider;
//! use kuberator::cache::CachingStrategy;
//! use strum::{Display, AsRefStr};
//! use std::sync::Arc;
//!
//! #[derive(Debug, Clone, Copy, Display, AsRefStr)]
//! enum MyEventReason {
//!     ResourceCreated,
//!     ResourceUpdated,
//! }
//! impl Reason for MyEventReason {}
//!
//! // In your operator setup:
//! let event_api_provider = Arc::new(StaticApiProvider::new(
//!     client,
//!     vec!["default"],
//!     CachingStrategy::Adhoc,
//! ));
//!
//! let event_recorder = Arc::new(EventRecorder::new(
//!     event_api_provider,
//!     "my-operator"
//! ));
//!
//! // In your reconciliation:
//! event_recorder.emit(
//!     &resource,
//!     EventData::normal(MyEventReason::ResourceCreated, "Resource was created")
//! ).await;
//! ```

pub mod types;

mod recorder;

pub use recorder::EventRecorder;
pub use types::EventData;
pub use types::EventType;
pub use types::Reason;

use async_trait::async_trait;
use kube::Resource;

use crate::error::Result;
use crate::TryResource;

/// Trait for emitting Kubernetes events
#[async_trait]
pub trait EmitEvent<R>: Send + Sync
where
    R: Reason,
{
    /// Try to emit a Kubernetes event, returning any errors
    ///
    /// Use this when you need explicit error handling. For most cases,
    /// prefer `emit()` which logs errors without failing reconciliation.
    async fn try_emit<K>(&self, object: &K, event: EventData<R>) -> Result<()>
    where
        K: Resource<DynamicType = ()> + TryResource + Clone + Send + Sync;

    /// Emit a Kubernetes event, logging but not propagating errors
    ///
    /// Events are observability only and should not fail reconciliation.
    /// This is the primary API for emitting events in reconciliation loops.
    async fn emit<K>(&self, object: &K, event: EventData<R>)
    where
        K: Resource<DynamicType = ()> + TryResource + Clone + Send + Sync,
    {
        let reason = event.reason.to_owned();
        if let Err(e) = self.try_emit(object, event).await {
            tracing::warn!(
                error = %e,
                reason = %reason,
                "Failed to emit event"
            );
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::error::Error;
    use crate::events::types::EventType;
    use k8s_openapi::api::core::v1::ConfigMap;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::sync::Arc;
    use std::sync::Mutex;
    use strum::{AsRefStr, Display};

    // Test event reason enum
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Display, AsRefStr)]
    pub enum TestEventReason {
        ResourceCreated,
        ResourceUpdated,
        ResourceDeleted,
        ReconciliationFailed,
    }

    impl Reason for TestEventReason {}

    /// Mock event recorder for testing
    #[derive(Clone)]
    pub struct MockEventRecorder<R: Reason> {
        events: Arc<Mutex<Vec<(String, R, String)>>>,
    }

    impl<R: Reason> Default for MockEventRecorder<R> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<R: Reason> MockEventRecorder<R> {
        pub fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub fn events(&self) -> Vec<(String, R, String)> {
            self.events.lock().unwrap().clone()
        }

        pub fn clear(&self) {
            self.events.lock().unwrap().clear();
        }
    }

    #[async_trait]
    impl<R: Reason> EmitEvent<R> for MockEventRecorder<R> {
        async fn try_emit<K>(&self, object: &K, event: EventData<R>) -> Result<()>
        where
            K: Resource<DynamicType = ()> + TryResource + Clone + Send + Sync,
        {
            self.events
                .lock()
                .unwrap()
                .push((object.try_name()?.to_owned(), event.reason, event.message));

            Ok(())
        }
    }

    /// Mock that can be configured to fail on demand for testing error handling
    struct FailingMockEventRecorder<R: Reason> {
        should_fail: bool,
        events: Arc<Mutex<Vec<(String, R, String)>>>,
    }

    impl<R: Reason> FailingMockEventRecorder<R> {
        fn new_succeeding() -> Self {
            Self {
                should_fail: false,
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn new_failing() -> Self {
            Self {
                should_fail: true,
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn events(&self) -> Vec<(String, R, String)> {
            self.events.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl<R: Reason> EmitEvent<R> for FailingMockEventRecorder<R> {
        async fn try_emit<K>(&self, object: &K, event: EventData<R>) -> Result<()>
        where
            K: Resource<DynamicType = ()> + TryResource + Clone + Send + Sync,
        {
            if self.should_fail {
                return Err(Error::EmitEventFailed("Simulated failure".to_string()));
            }

            self.events
                .lock()
                .unwrap()
                .push((object.try_name()?.to_owned(), event.reason, event.message));

            Ok(())
        }
    }

    fn create_test_resource() -> ConfigMap {
        ConfigMap {
            metadata: ObjectMeta {
                name: Some("test-resource".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_mock_event_recorder() {
        let recorder = MockEventRecorder::<TestEventReason>::new();

        let resource = create_test_resource();

        recorder
            .try_emit(
                &resource,
                EventData::normal(TestEventReason::ResourceCreated, "Resource was created"),
            )
            .await
            .unwrap();

        let events = recorder.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "test-resource");
        assert_eq!(events[0].1, TestEventReason::ResourceCreated);
        assert_eq!(events[0].2, "Resource was created");
    }

    #[test]
    fn test_normal_event_creation() {
        // Given: A reason and message
        let reason = TestEventReason::ResourceCreated;
        let message = "Resource has been created successfully";

        // When: Creating a normal event
        let event = EventData::normal(reason, message);

        // Then: The event has correct type, reason, and message
        assert_eq!(event.type_, EventType::Normal);
        assert_eq!(event.reason, TestEventReason::ResourceCreated);
        assert_eq!(event.message, "Resource has been created successfully");
        assert_eq!(event.action, None);
    }

    #[test]
    fn test_warning_event_creation() {
        // Given: A reason and message
        let reason = TestEventReason::ReconciliationFailed;
        let message = "Failed to reconcile resource configuration";

        // When: Creating a warning event
        let event = EventData::warning(reason, message);

        // Then: The event has Warning type
        assert_eq!(event.type_, EventType::Warning);
        assert_eq!(event.reason, TestEventReason::ReconciliationFailed);
        assert_eq!(event.message, "Failed to reconcile resource configuration");
        assert_eq!(event.action, None);
    }

    #[test]
    fn test_event_with_action() {
        // Given: A normal event
        let event = EventData::normal(TestEventReason::ResourceCreated, "Resource created");

        // When: Adding an action
        let event = event.with_action("CreateResource");

        // Then: The event has the action set
        assert_eq!(event.action, Some("CreateResource".to_string()));
    }

    #[test]
    fn test_event_message_accepts_string() {
        // Given: A String instead of &str
        let message = String::from("Dynamic message");

        // When: Creating an event with String
        let event = EventData::normal(TestEventReason::ResourceCreated, message);

        // Then: The message is correctly stored
        assert_eq!(event.message, "Dynamic message");
    }

    #[tokio::test]
    async fn test_try_emit_success_returns_ok() {
        // Given: A successful event recorder and a resource
        let recorder = FailingMockEventRecorder::new_succeeding();
        let resource = create_test_resource();

        // When: Calling try_emit with a normal event
        let result = recorder
            .try_emit(
                &resource,
                EventData::normal(TestEventReason::ResourceCreated, "Test message"),
            )
            .await;

        // Then: The result is Ok and the event was recorded
        assert!(result.is_ok());
        let events = recorder.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "test-resource");
        assert_eq!(events[0].2, "Test message");
    }

    #[tokio::test]
    async fn test_try_emit_failure_returns_error() {
        // Given: A failing event recorder and a resource
        let recorder = FailingMockEventRecorder::new_failing();
        let resource = create_test_resource();

        // When: Calling try_emit
        let result = recorder
            .try_emit(
                &resource,
                EventData::normal(TestEventReason::ResourceCreated, "Test message"),
            )
            .await;

        // Then: The result is an error
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Simulated failure"));
    }

    #[tokio::test]
    async fn test_emit_success_completes_without_error() {
        // Given: A successful event recorder and a resource
        let recorder = FailingMockEventRecorder::new_succeeding();
        let resource = create_test_resource();

        // When: Calling emit (silent-fail version)
        recorder
            .emit(
                &resource,
                EventData::normal(TestEventReason::ResourceCreated, "Test message"),
            )
            .await;

        // Then: The event was recorded (no panic, no error propagation)
        let events = recorder.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "test-resource");
    }

    #[tokio::test]
    async fn test_emit_failure_swallows_error_and_continues() {
        // Given: A failing event recorder and a resource
        let recorder = FailingMockEventRecorder::new_failing();
        let resource = create_test_resource();

        // When: Calling emit (should not panic or propagate error)
        recorder
            .emit(
                &resource,
                EventData::normal(TestEventReason::ResourceCreated, "Test message"),
            )
            .await;

        // Then: No panic occurred and no events were recorded (failure was swallowed)
        let events = recorder.events();
        assert_eq!(events.len(), 0);
    }

    #[tokio::test]
    async fn test_emit_calls_try_emit_internally() {
        // Given: A successful event recorder
        let recorder = FailingMockEventRecorder::new_succeeding();
        let resource = create_test_resource();

        // When: Calling emit
        recorder
            .emit(
                &resource,
                EventData::normal(TestEventReason::ResourceUpdated, "Update message"),
            )
            .await;

        // Then: try_emit was called (evidenced by recorded event)
        let events = recorder.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].1, TestEventReason::ResourceUpdated);
        assert_eq!(events[0].2, "Update message");
    }

    #[tokio::test]
    async fn test_multiple_events_are_recorded_sequentially() {
        // Given: A recorder and a resource
        let recorder = MockEventRecorder::<TestEventReason>::new();
        let resource = create_test_resource();

        // When: Emitting multiple events
        recorder
            .emit(
                &resource,
                EventData::normal(TestEventReason::ResourceCreated, "Created"),
            )
            .await;

        recorder
            .emit(
                &resource,
                EventData::normal(TestEventReason::ResourceUpdated, "Updated"),
            )
            .await;

        recorder
            .emit(
                &resource,
                EventData::normal(TestEventReason::ResourceDeleted, "Deleted"),
            )
            .await;

        // Then: All events are recorded in order
        let events = recorder.events();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].1, TestEventReason::ResourceCreated);
        assert_eq!(events[1].1, TestEventReason::ResourceUpdated);
        assert_eq!(events[2].1, TestEventReason::ResourceDeleted);
    }

    #[tokio::test]
    async fn test_clear_removes_all_events() {
        // Given: A recorder with some events
        let recorder = MockEventRecorder::<TestEventReason>::new();
        let resource = create_test_resource();

        recorder
            .emit(
                &resource,
                EventData::normal(TestEventReason::ResourceCreated, "Created"),
            )
            .await;

        assert_eq!(recorder.events().len(), 1);

        // When: Clearing the events
        recorder.clear();

        // Then: All events are removed
        assert_eq!(recorder.events().len(), 0);
    }
}
