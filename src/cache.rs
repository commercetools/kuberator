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

/// Defines how [StaticApiProvider] handles namespace cache misses.
///
/// Different strategies offer different trade-offs between strictness, performance, and flexibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachingStrategy {
    /// Only allow access to namespaces that were pre-cached during initialization.
    ///
    /// Returns an error if a namespace is not in the cache. This is the safest option
    /// and guarantees no runtime Api allocations.
    ///
    /// **Performance**: Lock-free HashMap lookup (~5ns)
    Strict,

    /// Create Api instances on-the-fly for uncached namespaces without caching them.
    ///
    /// If a namespace is in the cache, returns the cached instance. Otherwise, creates
    /// a new Api instance each time (not cached). Useful when you have a core set of
    /// frequently-accessed namespaces but occasionally need to access others.
    ///
    /// **Performance**:
    /// - Cached: Lock-free HashMap lookup (~5ns)
    /// - Uncached: Api creation per call (~100ns)
    Adhoc,

    /// Lazily create and cache Api instances on first access (extendable cache).
    ///
    /// Similar to [CachedApiProvider], but can be pre-populated with known namespaces.
    /// Uses RwLock for thread-safe dynamic caching.
    ///
    /// **Performance**:
    /// - Cached: RwLock read + HashMap lookup (~10-15ns)
    /// - First access: RwLock write + Api creation (~100ns, one-time per namespace)
    Extendable,
}

/// Abstraction for obtaining [`kube::Api`] instances, allowing different caching strategies.
///
/// This trait decouples the [`crate::Finalize`] trait from the specific caching implementation,
/// enabling you to choose the optimal strategy for your operator without changing your
/// reconciliation logic.
///
/// # Implementations
///
/// - [`StaticApiProvider`] - Flexible provider supporting [Strict](CachingStrategy::Strict),
///   [Adhoc](CachingStrategy::Adhoc), and [Extendable](CachingStrategy::Extendable) caching strategies
/// - [`CachedApiProvider`] - RwLock-based, lazy-loading cache (flexible, for dynamic namespaces)
///
/// # Example
///
/// ```
/// use kuberator::cache::{ProvideApi, StaticApiProvider, CachedApiProvider, CachingStrategy};
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
/// // This trait can be instantiated with different providers and strategies
/// // See their respective documentation for usage examples
/// ```
pub trait ProvideApi<R>
where
    R: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    R::DynamicType: Default,
{
    /// Gets an [`Arc<Api>`] instance for the given namespace.
    ///
    /// Returns an error if the namespace is not available. The exact behavior depends on
    /// the implementation:
    /// - [`StaticApiProvider`] with [Strict](CachingStrategy::Strict): Errors if not pre-cached
    /// - [`StaticApiProvider`] with [Adhoc](CachingStrategy::Adhoc): Creates on-the-fly if not cached
    /// - [`StaticApiProvider`] with [Extendable](CachingStrategy::Extendable): Creates and caches if not cached
    /// - [`CachedApiProvider`]: Creates and caches on first access
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

/// Internal storage backend for [StaticApiProvider] that adapts to different caching strategies.
enum CacheStorage<R>
where
    R: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    R::DynamicType: Default,
{
    /// Immutable HashMap for Strict and Adhoc strategies (no locking overhead)
    Static(HashMap<String, Arc<Api<R>>>),
    /// RwLock-protected HashMap for Extendable strategy (supports dynamic caching)
    Dynamic(RwLock<HashMap<String, Arc<Api<R>>>>),
}

/// A flexible API provider supporting multiple caching strategies.
///
/// Depending on the [CachingStrategy] chosen, this provider can behave as:
/// - **Strict**: Lock-free, pre-populated cache (fastest, errors on unknown namespaces)
/// - **Adhoc**: Lock-free for cached namespaces, creates Api on-the-fly for others (no caching of misses)
/// - **Extendable**: RwLock-based lazy loading (like [CachedApiProvider], but can be pre-populated)
///
/// # Performance by Strategy
///
/// | Strategy   | Cached Access | Uncached Access | Locking |
/// |------------|---------------|-----------------|---------|
/// | Strict     | ~5ns          | Error           | None    |
/// | Adhoc      | ~5ns          | ~100ns          | None    |
/// | Extendable | ~10-15ns      | ~100ns (cached) | RwLock  |
///
/// # Example
///
/// ```rust,ignore
/// use kuberator::cache::{StaticApiProvider, CachingStrategy};
/// use kube::Client;
/// use k8s_openapi::api::core::v1::ConfigMap;
///
/// // Strict: Only allow pre-defined namespaces
/// let strict = StaticApiProvider::new(
///     client.clone(),
///     vec!["default", "kube-system"],
///     CachingStrategy::Strict
/// );
///
/// // Adhoc: Pre-cache common namespaces, create others on-the-fly
/// let adhoc = StaticApiProvider::new(
///     client.clone(),
///     vec!["default"], // Common ones cached
///     CachingStrategy::Adhoc // Others created as needed
/// );
///
/// // Extendable: Start with known namespaces, dynamically cache new ones
/// let extendable = StaticApiProvider::new(
///     client.clone(),
///     vec!["default"],
///     CachingStrategy::Extendable // New namespaces cached on first access
/// );
/// ```
pub struct StaticApiProvider<R>
where
    R: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    R::DynamicType: Default,
{
    client: Client,
    strategy: CachingStrategy,
    cache: CacheStorage<R>,
}

impl<R> StaticApiProvider<R>
where
    R: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    R::DynamicType: Default,
{
    /// Creates a new [StaticApiProvider] with the specified caching strategy.
    ///
    /// The behavior depends on the chosen [CachingStrategy]:
    /// - **Strict**: Only allows access to pre-cached namespaces, errors otherwise
    /// - **Adhoc**: Pre-caches given namespaces, creates others on-the-fly without caching
    /// - **Extendable**: Pre-caches given namespaces, creates and caches others on first access
    ///
    /// # Example
    ///
    /// ```
    /// use kuberator::cache::{StaticApiProvider, CachingStrategy};
    /// use k8s_openapi::api::core::v1::ConfigMap;
    /// # use kube::Client;
    ///
    /// # async fn example() {
    /// # let client = Client::try_default().await.unwrap();
    /// // Strict mode - only these namespaces allowed
    /// let strict: StaticApiProvider<ConfigMap> = StaticApiProvider::new(
    ///     client.clone(),
    ///     vec!["default", "kube-system"],
    ///     CachingStrategy::Strict
    /// );
    ///
    /// // Adhoc mode - these cached, others created on demand
    /// let adhoc: StaticApiProvider<ConfigMap> = StaticApiProvider::new(
    ///     client.clone(),
    ///     vec!["default"],
    ///     CachingStrategy::Adhoc
    /// );
    /// # }
    /// ```
    pub fn new<I, S>(client: Client, namespaces: I, strategy: CachingStrategy) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut map = HashMap::new();

        for namespace in namespaces {
            let api = Arc::new(Api::<R>::namespaced(client.clone(), namespace.as_ref()));
            map.insert(namespace.as_ref().to_string(), api);
        }

        let cache = match strategy {
            CachingStrategy::Strict | CachingStrategy::Adhoc => CacheStorage::Static(map),
            CachingStrategy::Extendable => CacheStorage::Dynamic(RwLock::new(map)),
        };

        Self {
            client,
            strategy,
            cache,
        }
    }
}

impl<R> ProvideApi<R> for StaticApiProvider<R>
where
    R: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    R::DynamicType: Default,
{
    /// Gets an Api instance for the given namespace according to the configured [CachingStrategy].
    ///
    /// # Behavior by Strategy
    ///
    /// - **Strict**: Returns cached Api or error if namespace not pre-cached
    /// - **Adhoc**: Returns cached Api if available, otherwise creates new Api without caching
    /// - **Extendable**: Returns cached Api if available, otherwise creates, caches, and returns new Api
    ///
    /// # Performance
    ///
    /// - **Strict/Adhoc (cache hit)**: ~5ns (lock-free HashMap lookup)
    /// - **Adhoc (cache miss)**: ~100ns (Api creation, not cached)
    /// - **Extendable (cache hit)**: ~10-15ns (RwLock read + HashMap lookup)
    /// - **Extendable (cache miss)**: ~100ns (RwLock write + Api creation, one-time per namespace)
    fn get(&self, namespace: &str) -> Result<Arc<Api<R>>> {
        match (&self.cache, self.strategy) {
            // Strict: Only return pre-cached namespaces
            (CacheStorage::Static(map), CachingStrategy::Strict) => {
                map.get(namespace).map(Arc::clone).ok_or_else(|| {
                    Error::UserInputError(format!(
                        "Namespace '{namespace}' not found in static cache. Did you include it during initialization?"
                    ))
                })
            }

            // Adhoc: Return cached if available, otherwise create on-the-fly (don't cache)
            (CacheStorage::Static(map), CachingStrategy::Adhoc) => {
                if let Some(api) = map.get(namespace) {
                    return Ok(Arc::clone(api));
                }

                Ok(Arc::new(Api::<R>::namespaced(self.client.clone(), namespace)))
            }

            // Extendable: Return cached if available, otherwise create and cache
            (CacheStorage::Dynamic(lock), CachingStrategy::Extendable) => {
                // Fast path: try to get from cache with read lock
                {
                    let cache = lock.read()?;
                    if let Some(api) = cache.get(namespace) {
                        return Ok(Arc::clone(api));
                    }
                }

                // Slow path: create and cache with write lock
                let mut cache = lock.write()?;

                // Double-check in case another thread created it while we waited
                if let Some(api) = cache.get(namespace) {
                    return Ok(Arc::clone(api));
                }

                let api = Arc::new(Api::<R>::namespaced(self.client.clone(), namespace));
                cache.insert(namespace.to_string(), Arc::clone(&api));

                Ok(api)
            }

            // Invalid state combinations (shouldn't happen with correct construction)
            _ => Err(Error::InvalidApiProviderConfig),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::ConfigMap;
    use kube::client::Body;
    use kube::Client;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;
    use tower_test::mock;

    // This creates a client that doesn't require a real Kubernetes cluster
    fn test_client() -> Client {
        let (mock_service, _handle) = mock::pair::<http::Request<Body>, http::Response<hyper::body::Incoming>>();

        Client::new(mock_service, "default")
    }

    #[tokio::test]
    async fn test_static_provider_strict_cached_namespace() {
        // Given: A StaticApiProvider with Strict strategy pre-caching two namespaces
        let client = test_client();
        let provider: StaticApiProvider<ConfigMap> =
            StaticApiProvider::new(client, vec!["default", "kube-system"], CachingStrategy::Strict);

        // When: Requesting pre-cached namespaces
        let result_default = provider.get("default");
        let result_kube_system = provider.get("kube-system");

        // Then: Both requests should succeed
        assert!(result_default.is_ok());
        assert!(result_kube_system.is_ok());
    }

    #[tokio::test]
    async fn test_static_provider_strict_uncached_namespace() {
        // Given: A StaticApiProvider with Strict strategy pre-caching only one namespace
        let client = test_client();
        let provider: StaticApiProvider<ConfigMap> =
            StaticApiProvider::new(client, vec!["default"], CachingStrategy::Strict);

        // When: Requesting a namespace that was not pre-cached
        let result = provider.get("unknown-namespace");

        // Then: Should return an error with appropriate message
        assert!(result.is_err());
        if let Err(Error::UserInputError(msg)) = result {
            assert!(msg.contains("unknown-namespace"));
            assert!(msg.contains("not found in static cache"));
        } else {
            panic!("Expected UserInputError");
        }
    }

    #[tokio::test]
    async fn test_static_provider_adhoc_cached_namespace() {
        // Given: A StaticApiProvider with Adhoc strategy pre-caching two namespaces
        let client = test_client();
        let provider: StaticApiProvider<ConfigMap> =
            StaticApiProvider::new(client, vec!["default", "kube-system"], CachingStrategy::Adhoc);

        // When: Requesting a pre-cached namespace
        let result = provider.get("default");

        // Then: Request should succeed
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_static_provider_adhoc_uncached_namespace() {
        // Given: A StaticApiProvider with Adhoc strategy pre-caching only one namespace
        let client = test_client();
        let provider: StaticApiProvider<ConfigMap> =
            StaticApiProvider::new(client, vec!["default"], CachingStrategy::Adhoc);

        // When: Requesting the same uncached namespace twice
        let result1 = provider.get("unknown-namespace");
        let result2 = provider.get("unknown-namespace");

        // Then: Both requests should succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        // And: The Arc pointers should be different (new Api created each time, not cached)
        assert!(!Arc::ptr_eq(&result1.unwrap(), &result2.unwrap()));
    }

    #[tokio::test]
    async fn test_static_provider_extendable_cached_namespace() {
        // Given: A StaticApiProvider with Extendable strategy pre-caching two namespaces
        let client = test_client();
        let provider: StaticApiProvider<ConfigMap> =
            StaticApiProvider::new(client, vec!["default", "kube-system"], CachingStrategy::Extendable);

        // When: Requesting a pre-cached namespace
        let result = provider.get("default");

        // Then: Request should succeed
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_static_provider_extendable_uncached_namespace() {
        // Given: A StaticApiProvider with Extendable strategy pre-caching only one namespace
        let client = test_client();
        let provider: StaticApiProvider<ConfigMap> =
            StaticApiProvider::new(client, vec!["default"], CachingStrategy::Extendable);

        // When: Requesting the same uncached namespace twice
        let result1 = provider.get("new-namespace");
        let result2 = provider.get("new-namespace");

        // Then: Both requests should succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        // And: The Arc pointers should be the same (cached after first access)
        assert!(Arc::ptr_eq(&result1.unwrap(), &result2.unwrap()));
    }

    #[tokio::test]
    async fn test_static_provider_extendable_thread_safety() {
        // Given: A StaticApiProvider with Extendable strategy shared across 10 threads
        let client = test_client();
        let provider = Arc::new(StaticApiProvider::<ConfigMap>::new(
            client,
            vec!["default"],
            CachingStrategy::Extendable,
        ));

        let num_threads = 10;
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = vec![];

        // When: All threads simultaneously access the same uncached namespace
        for _ in 0..num_threads {
            let provider = Arc::clone(&provider);
            let barrier = Arc::clone(&barrier);

            let handle = thread::spawn(move || {
                // Synchronize thread start to maximize contention
                barrier.wait();
                provider.get("concurrent-namespace")
            });

            handles.push(handle);
        }

        // Then: Collect all results
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // And: All threads should succeed
        assert!(results.iter().all(|r| r.is_ok()));

        // And: All should return the same cached Api instance (proves double-checked locking works)
        let first_api = results[0].as_ref().unwrap();
        for result in &results[1..] {
            assert!(Arc::ptr_eq(first_api, result.as_ref().unwrap()));
        }
    }

    #[tokio::test]
    async fn test_cached_provider_lazy_loading() {
        // Given: A CachedApiProvider with no pre-cached namespaces
        let client = test_client();
        let provider: CachedApiProvider<ConfigMap> = CachedApiProvider::new(client);

        // When: Requesting the same namespace twice
        let result1 = provider.get("lazy-namespace");
        let result2 = provider.get("lazy-namespace");

        // Then: Both requests should succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        // And: The Arc pointers should be the same (cached after first access)
        assert!(Arc::ptr_eq(&result1.unwrap(), &result2.unwrap()));
    }

    #[tokio::test]
    async fn test_cached_provider_multiple_namespaces() {
        // Given: A CachedApiProvider with no pre-cached namespaces
        let client = test_client();
        let provider: CachedApiProvider<ConfigMap> = CachedApiProvider::new(client);

        // When: Requesting multiple different namespaces, with one requested twice
        let ns1 = provider.get("namespace-1");
        let ns2 = provider.get("namespace-2");
        let ns1_again = provider.get("namespace-1");

        // Then: All requests should succeed
        assert!(ns1.is_ok());
        assert!(ns2.is_ok());
        assert!(ns1_again.is_ok());

        // And: The same namespace should return the same cached instance
        assert!(Arc::ptr_eq(&ns1.unwrap(), &ns1_again.unwrap()));
    }
}
