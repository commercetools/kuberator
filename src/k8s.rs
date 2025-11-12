//! Generic Kubernetes repository implementation for common use cases.
//!
//! This module provides a ready-to-use [`K8sRepository`] that implements the [`crate::Finalize`] trait,
//! eliminating the need for boilerplate code in most operators.

use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use k8s_openapi::NamespaceResourceScope;
use kube::Resource;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::cache::ProvideApi;
use crate::Finalize;

/// A generic Kubernetes repository that implements [`crate::Finalize`] for any resource and API provider.
///
/// This struct eliminates boilerplate by providing a ready-to-use implementation of the
/// [`crate::Finalize`] trait. Instead of creating your own repository struct and implementing [`crate::Finalize`]
/// manually, you can use this generic implementation directly.
///
/// # Type Parameters
///
/// - `R` - The Custom Resource type (e.g., `MyCrd`)
/// - `P` - The API provider implementation ([`crate::cache::StaticApiProvider`], [`crate::cache::CachedApiProvider`] or custom implementation)
///
/// # Example
///
/// ```
/// use kuberator::k8s::K8sRepository;
/// use kuberator::cache::StaticApiProvider;
/// use kuberator::{Finalize, Context};
/// use k8s_openapi::api::core::v1::ConfigMap;
/// # use std::sync::Arc;
/// # use kube::runtime::controller::Action;
///
/// // Without K8sRepository (manual implementation):
/// struct MyK8sRepo {
///     api_provider: StaticApiProvider<ConfigMap>,
/// }
///
/// impl Finalize<ConfigMap, StaticApiProvider<ConfigMap>> for MyK8sRepo {
///     fn api_provider(&self) -> &StaticApiProvider<ConfigMap> {
///         &self.api_provider
///     }
/// }
///
/// // With K8sRepository (zero boilerplate):
/// type MyK8sRepoAlias = K8sRepository<ConfigMap, StaticApiProvider<ConfigMap>>;
///
/// // Usage in Context:
/// struct MyContext {
///     repo: Arc<K8sRepository<ConfigMap, StaticApiProvider<ConfigMap>>>,
/// }
///
/// #[async_trait::async_trait]
/// impl Context<ConfigMap, K8sRepository<ConfigMap, StaticApiProvider<ConfigMap>>, StaticApiProvider<ConfigMap>> for MyContext {
///     fn k8s_repository(&self) -> Arc<K8sRepository<ConfigMap, StaticApiProvider<ConfigMap>>> {
///         Arc::clone(&self.repo)
///     }
///     fn finalizer(&self) -> &'static str { "example.com/finalizer" }
///     async fn handle_apply(&self, object: Arc<ConfigMap>) -> kuberator::error::Result<Action> {
///         Ok(Action::await_change())
///     }
///     async fn handle_cleanup(&self, object: Arc<ConfigMap>) -> kuberator::error::Result<Action> {
///         Ok(Action::await_change())
///     }
/// }
/// ```
///
/// # When to Use
///
/// Use [`K8sRepository`] when:
/// - You don't need custom repository logic beyond what [`crate::Finalize`] provides
/// - You want to minimize boilerplate code
/// - Your operator doesn't require additional state in the repository
///
/// Create a custom repository struct when:
/// - You need to store additional state
/// - You need custom methods beyond the [`crate::Finalize`] trait
/// - You want more control over the repository implementation
pub struct K8sRepository<R, P>
where
    R: Resource<Scope = NamespaceResourceScope> + Serialize + DeserializeOwned + Debug + Clone + Send + Sync + 'static,
    R::DynamicType: Default,
    P: ProvideApi<R>,
{
    api_provider: P,
    phantom: PhantomData<R>,
}

impl<R, P> K8sRepository<R, P>
where
    R: Resource<Scope = NamespaceResourceScope> + Serialize + DeserializeOwned + Debug + Clone + Send + Sync + 'static,
    R::DynamicType: Default,
    P: ProvideApi<R> + Send + Sync,
{
    /// Creates a new [`K8sRepository`] with the given API provider.
    ///
    /// # Example
    ///
    /// ```
    /// use kuberator::k8s::K8sRepository;
    /// use kuberator::cache::StaticApiProvider;
    /// use k8s_openapi::api::core::v1::ConfigMap;
    /// # use kube::Client;
    ///
    /// # async fn example() {
    /// # let client = Client::try_default().await.unwrap();
    /// let api_provider = StaticApiProvider::new(client, vec!["default", "production"]);
    /// let repo: K8sRepository<ConfigMap, _> = K8sRepository::new(api_provider);
    /// # }
    /// ```
    pub fn new(api_provider: P) -> Self {
        K8sRepository {
            api_provider,
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<R, P> Finalize<R, P> for K8sRepository<R, P>
where
    R: Resource<Scope = NamespaceResourceScope> + Serialize + DeserializeOwned + Debug + Clone + Send + Sync + 'static,
    R::DynamicType: Default,
    P: ProvideApi<R> + Send + Sync,
{
    fn api_provider(&self) -> &P {
        &self.api_provider
    }
}
