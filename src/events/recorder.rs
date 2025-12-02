use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use k8s_openapi::api::core::v1::Event;
use k8s_openapi::api::core::v1::EventSource;
use k8s_openapi::api::core::v1::ObjectReference;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::api::PostParams;
use kube::Resource;

use crate::cache::ProvideApi;
use crate::error::Result;
use crate::events::types::{EventData, Reason};
use crate::events::EmitEvent;
use crate::TryResource;

/// Implementation of EmitEvent that creates Kubernetes Event resources
pub struct EventRecorder<P>
where
    P: ProvideApi<Event> + Send + Sync,
{
    api_provider: Arc<P>,
    component: Cow<'static, str>,
}

impl<P> EventRecorder<P>
where
    P: ProvideApi<Event> + Send + Sync,
{
    /// Create a new EventRecorder
    ///
    /// # Arguments
    /// * `api_provider` - API provider for Event resources
    /// * `component` - Component name that will appear in events (e.g., "my-operator")
    pub fn new(api_provider: Arc<P>, component: impl Into<Cow<'static, str>>) -> Self {
        Self {
            api_provider,
            component: component.into(),
        }
    }
}

#[async_trait]
impl<P, R> EmitEvent<R> for EventRecorder<P>
where
    P: ProvideApi<Event> + Send + Sync,
    R: Reason,
{
    #[tracing::instrument(
        skip(self, object),
        fields(
            object_kind = %K::kind(&()),
            object_name = %object.try_name().unwrap_or_default(),
            object_namespace = %object.try_namespace().unwrap_or_default(),
            event_type = %event.type_,
            event_reason = %event.reason,
        )
    )]
    async fn try_emit<K>(&self, object: &K, event: EventData<R>) -> Result<()>
    where
        K: Resource<DynamicType = ()> + TryResource + Clone + Send + Sync,
    {
        let namespace = object.try_namespace()?;
        let name = object.try_name()?;

        let events = self.api_provider.get(&namespace)?;

        let now = Utc::now();
        let event_name = format!("{name}.{now}", now = now.timestamp());

        let k8s_event = Event {
            metadata: ObjectMeta {
                name: Some(event_name),
                namespace: Some(namespace.to_owned()),
                ..Default::default()
            },
            involved_object: ObjectReference {
                api_version: Some(K::api_version(&()).to_string()),
                kind: Some(K::kind(&()).to_string()),
                name: Some(name.to_owned()),
                namespace: Some(namespace),
                uid: object.meta().uid.to_owned(),
                resource_version: object.meta().resource_version.to_owned(),
                ..Default::default()
            },
            reason: Some(event.reason.to_string()),
            message: Some(event.message),
            type_: Some(event.type_.to_string()),
            first_timestamp: Some(Time(now)),
            last_timestamp: Some(Time(now)),
            source: Some(EventSource {
                component: Some(self.component.to_string()),
                ..Default::default()
            }),
            reporting_component: Some(self.component.to_string()),
            action: event.action,
            ..Default::default()
        };

        events.create(&PostParams::default(), &k8s_event).await?;

        Ok(())
    }
}
