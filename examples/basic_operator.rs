//! Basic Operator Example
//!
//! This example demonstrates a minimal working Kubernetes operator using Kuberator.
//! It watches for ConfigMap resources and logs whenever they are created, updated, or deleted.
//!
//! To run this example:
//! ```bash
//! cargo run --example basic_operator
//! ```
//!
//! Note: This example requires a working Kubernetes cluster and kubectl configuration.

use std::sync::Arc;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::runtime::controller::Action;
use kube::runtime::watcher::Config;
use kube::{Api, Client};
use kuberator::cache::{CachingStrategy, StaticApiProvider};
use kuberator::error::Result as KubeResult;
use kuberator::k8s::K8sRepository;
use kuberator::{Context, Reconcile, TryResource};

// Type alias for our repository (using the generic K8sRepository)
type MyK8sRepo = K8sRepository<ConfigMap, StaticApiProvider<ConfigMap>>;

// Define the context that holds our operator logic
struct MyContext {
    repo: Arc<MyK8sRepo>,
}

#[async_trait]
impl Context<ConfigMap, MyK8sRepo, StaticApiProvider<ConfigMap>> for MyContext {
    fn k8s_repository(&self) -> Arc<MyK8sRepo> {
        Arc::clone(&self.repo)
    }

    fn finalizer(&self) -> &'static str {
        "example.kuberator.io/basic-operator"
    }

    async fn handle_apply(&self, object: Arc<ConfigMap>) -> KubeResult<Action> {
        let name = object.try_name()?;
        let namespace = object.try_namespace()?;

        tracing::info!("ConfigMap {}/{} was created or updated", namespace, name);

        // In a real operator, you would perform your reconciliation logic here:
        // - Create/update related resources
        // - Interact with external systems
        // - Update status

        Ok(Action::await_change())
    }

    async fn handle_cleanup(&self, object: Arc<ConfigMap>) -> KubeResult<Action> {
        let name = object.try_name()?;
        let namespace = object.try_namespace()?;

        tracing::info!("ConfigMap {}/{} is being deleted - cleaning up", namespace, name);

        // In a real operator, you would perform cleanup here:
        // - Delete related resources
        // - Clean up external systems
        // - Remove finalizers

        Ok(Action::await_change())
    }
}

// Define the reconciler that starts the controller
struct MyReconciler {
    context: Arc<MyContext>,
    crd_api: Api<ConfigMap>,
}

#[async_trait]
impl Reconcile<ConfigMap, MyContext, MyK8sRepo, StaticApiProvider<ConfigMap>> for MyReconciler {
    fn destruct(self) -> (Api<ConfigMap>, Config, Arc<MyContext>) {
        (self.crd_api, Config::default(), self.context)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    tracing::info!("Starting Basic Operator");

    // Create Kubernetes client
    let client = Client::try_default().await?;

    // Create API provider with Strict caching strategy
    // We know we'll only watch the "default" namespace
    let api_provider = StaticApiProvider::new(client.clone(), vec!["default"], CachingStrategy::Strict);

    // Create repository and context
    let k8s_repo = K8sRepository::new(api_provider);
    let context = MyContext {
        repo: Arc::new(k8s_repo),
    };

    // Create reconciler
    let reconciler = MyReconciler {
        context: Arc::new(context),
        crd_api: Api::namespaced(client, "default"),
    };

    tracing::info!("Watching ConfigMaps in 'default' namespace");

    // Start the reconciler (synchronously for simplicity)
    // Use start_concurrent(Some(10), None) for production
    reconciler.start::<futures::future::Ready<()>>(None).await;

    Ok(())
}
