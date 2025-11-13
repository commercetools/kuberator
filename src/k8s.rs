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
    /// use kuberator::cache::{StaticApiProvider, CachingStrategy};
    /// use k8s_openapi::api::core::v1::ConfigMap;
    /// # use kube::Client;
    ///
    /// # async fn example() {
    /// # let client = Client::try_default().await.unwrap();
    /// let api_provider = StaticApiProvider::new(
    ///     client,
    ///     vec!["default", "production"],
    ///     CachingStrategy::Strict
    /// );
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

#[cfg(test)]
mod tests {
    use super::*;

    use k8s_openapi::api::core::v1::ConfigMap;
    use kube::client::Body;
    use kube::Client;
    use kube::CustomResource;
    use schemars::JsonSchema;
    use serde::Deserialize;
    use serde::Serialize;
    use tower_test::mock;

    use crate::cache::CachedApiProvider;
    use crate::cache::CachingStrategy;
    use crate::cache::StaticApiProvider;

    fn test_client() -> Client {
        let (mock_service, _handle) = mock::pair::<http::Request<Body>, http::Response<hyper::body::Incoming>>();

        Client::new(mock_service, "default")
    }

    #[derive(CustomResource, Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
    #[kube(group = "test.kuberator.io", version = "v1", kind = "TestResource", namespaced)]
    #[allow(dead_code)]
    struct TestResourceSpec {
        value: String,
    }

    #[tokio::test]
    async fn test_k8s_repository_new_with_static_provider() {
        // Given: A client and StaticApiProvider
        let client = test_client();
        let api_provider = StaticApiProvider::new(client, vec!["default"], CachingStrategy::Strict);

        // When: Creating a new K8sRepository
        let repo = K8sRepository::<ConfigMap, _>::new(api_provider);

        // Then: Repository should be created successfully (test compiles and runs)
        let _ = repo;
    }

    #[tokio::test]
    async fn test_k8s_repository_new_with_cached_provider() {
        // Given: A client and CachedApiProvider
        let client = test_client();
        let api_provider = CachedApiProvider::new(client);

        // When: Creating a new K8sRepository
        let repo = K8sRepository::<ConfigMap, _>::new(api_provider);

        // Then: Repository should be created successfully (test compiles and runs)
        let _ = repo;
    }

    #[tokio::test]
    async fn test_k8s_repository_api_provider_returns_reference() {
        // Given: A K8sRepository with StaticApiProvider
        let client = test_client();
        let api_provider = StaticApiProvider::new(client.clone(), vec!["default"], CachingStrategy::Strict);
        let repo = K8sRepository::<ConfigMap, _>::new(api_provider);

        // When: Calling api_provider()
        let returned_provider = repo.api_provider();

        // Then: Should return a reference to the api_provider
        // We verify this by checking we can call methods on it
        let _ = returned_provider.get("default");
    }

    #[tokio::test]
    async fn test_k8s_repository_with_custom_resource() {
        // Given: A client and StaticApiProvider for custom resource
        let client = test_client();
        let api_provider = StaticApiProvider::new(client, vec!["default", "production"], CachingStrategy::Adhoc);

        // When: Creating a K8sRepository for custom resource
        let repo = K8sRepository::<TestResource, _>::new(api_provider);

        // Then: Repository should be created successfully with custom resource type
        let provider = repo.api_provider();
        let _ = provider.get("default");
        let _ = provider.get("production");
    }

    #[tokio::test]
    async fn test_k8s_repository_with_extendable_strategy() {
        // Given: A client and StaticApiProvider with Extendable strategy
        let client = test_client();
        let api_provider = StaticApiProvider::new(client, vec!["default"], CachingStrategy::Extendable);

        // When: Creating a K8sRepository
        let repo = K8sRepository::<ConfigMap, _>::new(api_provider);

        // Then: Repository should be created and support extendable namespaces
        let provider = repo.api_provider();
        let _ = provider.get("default");
        let _ = provider.get("new-namespace"); // Should work with Extendable
    }

    #[tokio::test]
    async fn test_k8s_repository_finalize_trait_implementation() {
        // Given: A K8sRepository
        let client = test_client();
        let api_provider = StaticApiProvider::new(client, vec!["default"], CachingStrategy::Strict);
        let repo = K8sRepository::<ConfigMap, _>::new(api_provider);

        // When: Using Finalize trait methods
        let provider_ref = Finalize::api_provider(&repo);

        // Then: Should return correct api_provider reference
        let _ = provider_ref.get("default");
    }

    #[tokio::test]
    async fn test_k8s_repository_multiple_namespaces() {
        // Given: A StaticApiProvider with multiple namespaces
        let client = test_client();
        let namespaces = vec!["default", "kube-system", "production", "staging"];
        let api_provider = StaticApiProvider::new(client, namespaces.clone(), CachingStrategy::Strict);

        // When: Creating a K8sRepository
        let repo = K8sRepository::<ConfigMap, _>::new(api_provider);

        // Then: Should be able to access all namespaces through api_provider
        let provider = repo.api_provider();
        for namespace in namespaces {
            let api = provider.get(namespace);
            assert!(api.is_ok(), "Should get API for namespace: {}", namespace);
        }
    }
}
