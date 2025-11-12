//! API caching utilities for improved performance in operators that frequently access the same namespaces.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::RwLock;

use k8s_openapi::NamespaceResourceScope;
use kube::Api;
use kube::Client;
use kube::Resource;
use serde::de::DeserializeOwned;

use crate::error::Error;
use crate::error::Result;

/// Abstraction for obtaining [`kube::Api`] instances, allowing different caching strategies.
///
/// This trait decouples the [`crate::Finalize`] trait from the specific caching implementation,
/// enabling you to choose the optimal strategy for your operator without changing your
/// reconciliation logic.
///
/// # Implementations
///
/// - [`StaticApiProvider`] - Lock-free, pre-populated cache (fastest, recommended when namespaces are known)
/// - [`CachedApiProvider`] - RwLock-based, lazy-loading cache (flexible, for dynamic namespaces)
///
/// # Example
///
/// ```
/// use kuberator::cache::{ProvideApi, StaticApiProvider, CachedApiProvider};
/// use kuberator::Finalize;
/// use k8s_openapi::api::core::v1::ConfigMap;
///
/// // Your repository can be generic over any ProvideApi implementation
/// struct MyK8sRepo<P: ProvideApi<ConfigMap> + Send + Sync> {
///     api_provider: P,
/// }
///
/// impl<P: ProvideApi<ConfigMap> + Send + Sync> Finalize<ConfigMap, P> for MyK8sRepo<P> {
///     fn api_provider(&self) -> &P {
///         &self.api_provider
///     }
/// }
///
/// // This struct can be instantiated with either StaticApiProvider or CachedApiProvider
/// // See their respective documentation for usage examples
/// ```
pub trait ProvideApi<R>
where
    R: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    R::DynamicType: Default,
{
    /// Gets an [`Arc<Api>`] instance for the given namespace.
    ///
    /// Returns an error if the namespace is not available (e.g., not in the cache for [`StaticApiProvider`]).
    fn get(&self, namespace: &str) -> Result<Arc<Api<R>>>;
}

/// A helper struct that caches [`Arc<Api>`] instances per namespace to avoid repeatedly creating them.
///
/// This is particularly useful when your operator frequently accesses the same namespaces,
/// as it eliminates the overhead of creating new Api instances for each operation.
///
/// # Performance Benefits
///
/// - **Cache hits**: Only increment an Arc reference count (extremely cheap, just atomic increment)
/// - **Cache misses**: Create Api once per namespace, then reuse
/// - **No string allocations**: Namespace strings are allocated once and shared via Arc
/// - **Concurrent reads**: Multiple threads can read from cache simultaneously via RwLock
///
/// # Example
///
/// ```rust,ignore
/// use kuberator::cache::CachedApiProvider;
/// use kube::Client;
/// use k8s_openapi::api::core::v1::ConfigMap;
///
/// struct MyK8sRepo {
///     api_cache: CachedApiProvider<ConfigMap>,
/// }
///
/// impl MyK8sRepo {
///     fn new(client: Client) -> Self {
///         Self {
///             api_cache: CachedApiProvider::new(client),
///         }
///     }
/// }
/// ```
pub struct CachedApiProvider<R>
where
    R: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    R::DynamicType: Default,
{
    client: Client,
    cache: RwLock<HashMap<String, Arc<Api<R>>>>,
}

impl<R> CachedApiProvider<R>
where
    R: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    R::DynamicType: Default,
{
    /// Creates a new [CachedApiProvider] with the given Kubernetes client.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            cache: RwLock::new(HashMap::new()),
        }
    }
}

impl<R> ProvideApi<R> for CachedApiProvider<R>
where
    R: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    R::DynamicType: Default,
{
    /// Gets or creates an [`Arc<Api>`] instance for the given namespace.
    ///
    /// This method first checks the cache for an existing Api instance. If found, it returns
    /// an Arc clone (just incrementing the reference count). If not found, it creates a new
    /// Api instance, wraps it in Arc, caches it, and returns a clone of the Arc.
    ///
    /// # Performance
    ///
    /// - **Cache hits**: Only require a read lock + Arc clone (fast, concurrent-friendly)
    /// - **Cache misses**: Require a write lock (slower, but only happens once per namespace)
    /// - **Arc cloning**: Just increments a reference count (atomic operation, very cheap)
    ///
    /// # Example
    ///
    /// ```
    /// # use kuberator::cache::{CachedApiProvider, ProvideApi};
    /// # use k8s_openapi::api::core::v1::ConfigMap;
    /// # use kube::Client;
    /// # async fn example() -> kuberator::error::Result<()> {
    /// # let client = Client::try_default().await.unwrap();
    /// # let cache: CachedApiProvider<ConfigMap> = CachedApiProvider::new(client);
    /// let api = cache.get("default")?;
    /// // api is Arc<Api<ConfigMap>>, cloning it is very cheap
    /// let api_clone = api.clone(); // Just increments reference count
    /// # Ok(())
    /// # }
    /// ```
    fn get(&self, namespace: &str) -> Result<Arc<Api<R>>> {
        // Fast path: try to get from cache with read lock
        {
            let cache = self.cache.read()?;
            if let Some(api) = cache.get(namespace) {
                return Ok(Arc::clone(api));
            }
        }

        // Slow path: create and cache with write lock
        let mut cache = self.cache.write()?;

        // Double-check in case another thread created it while we waited for write lock
        if let Some(api) = cache.get(namespace) {
            return Ok(Arc::clone(api));
        }

        let api = Arc::new(Api::<R>::namespaced(self.client.clone(), namespace));
        cache.insert(namespace.to_string(), Arc::clone(&api));

        Ok(api)
    }
}

/// A static (immutable) API provider that pre-caches all namespace Api instances at construction.
///
/// Unlike [CachedApiProvider], this struct has **zero locking overhead** because the cache is
/// never modified after construction. This makes it ideal when you know all namespaces upfront.
///
/// # Performance Benefits
///
/// - **No locks**: Direct HashMap lookup with zero synchronization overhead
/// - **Fastest possible**: Just HashMap lookup + Arc clone (< 5ns on modern CPUs)
/// - **No contention**: Multiple threads can access without any coordination
/// - **Predictable**: Every lookup has identical performance (no cache miss path)
///
/// # Example
///
/// ```rust,ignore
/// use kuberator::cache::StaticApiProvider;
/// use kube::Client;
/// use k8s_openapi::api::core::v1::ConfigMap;
///
/// struct MyK8sRepo {
///     api_cache: StaticApiProvider<ConfigMap>,
/// }
///
/// impl MyK8sRepo {
///     fn new(client: Client, namespaces: Vec<String>) -> Self {
///         Self {
///             api_cache: StaticApiProvider::new(client, namespaces),
///         }
///     }
/// }
/// ```
pub struct StaticApiProvider<R>
where
    R: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    R::DynamicType: Default,
{
    cache: HashMap<String, Arc<Api<R>>>,
}

impl<R> StaticApiProvider<R>
where
    R: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    R::DynamicType: Default,
{
    /// Creates a new [StaticApiProvider] with Api instances pre-cached for the given namespaces.
    ///
    /// After construction, the cache is immutable - no locking needed!
    ///
    /// # Example
    ///
    /// ```
    /// use kuberator::cache::StaticApiProvider;
    /// use k8s_openapi::api::core::v1::ConfigMap;
    /// # use kube::Client;
    ///
    /// # async fn example() {
    /// # let client = Client::try_default().await.unwrap();
    /// let namespaces = vec!["default", "kube-system", "production"];
    /// let cache: StaticApiProvider<ConfigMap> = StaticApiProvider::new(client, namespaces);
    /// # }
    /// ```
    pub fn new<I, S>(client: Client, namespaces: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut cache = HashMap::new();
        for namespace in namespaces {
            let api = Arc::new(Api::<R>::namespaced(client.clone(), namespace.as_ref()));
            cache.insert(namespace.as_ref().to_string(), api);
        }

        Self { cache }
    }
}

impl<R> ProvideApi<R> for StaticApiProvider<R>
where
    R: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    R::DynamicType: Default,
{
    /// Gets the Api instance for the given namespace.
    ///
    /// Returns an error if the namespace wasn't in the initial set provided to [StaticApiProvider::new].
    ///
    /// This operation has **zero locking overhead** - just a HashMap lookup and Arc clone.
    fn get(&self, namespace: &str) -> Result<Arc<Api<R>>> {
        self.cache.get(namespace).map(Arc::clone).ok_or_else(|| {
            Error::UserInputError(format!(
                "Namespace '{}' not found in static cache. Did you include it during initialization?",
                namespace
            ))
        })
    }
}
