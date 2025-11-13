//! Operator with Status Example
//!
//! This example demonstrates using the ObserveGeneration pattern to track
//! the reconciliation state of resources.
//!
//! To run this example:
//! ```bash
//! cargo run --example with_status
//! ```
//!
//! Note: This example requires a working Kubernetes cluster and a CRD installed.

use std::sync::Arc;

use async_trait::async_trait;
use kube::runtime::controller::Action;
use kube::runtime::watcher::Config;
use kube::{Api, Client, CustomResource};
use kuberator::cache::{CachingStrategy, StaticApiProvider};
use kuberator::error::Result as KubeResult;
use kuberator::k8s::K8sRepository;
use kuberator::{Context, Finalize, ObserveGeneration, Reconcile};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// Define a Custom Resource
#[derive(CustomResource, Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
#[kube(
    group = "example.kuberator.io",
    version = "v1",
    kind = "MyResource",
    plural = "myresources",
    namespaced,
    status = "MyResourceStatus"
)]
pub struct MyResourceSpec {
    pub message: String,
}

// Define the status object with ObserveGeneration
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
pub struct MyResourceStatus {
    pub state: String,
    pub observed_generation: Option<i64>,
}

impl ObserveGeneration for MyResourceStatus {
    fn add(&mut self, observed_generation: i64) {
        self.observed_generation = Some(observed_generation);
    }
}

// Type alias for our repository
type MyK8sRepo = K8sRepository<MyResource, StaticApiProvider<MyResource>>;

// Define the context
struct MyContext {
    repo: Arc<MyK8sRepo>,
}

#[async_trait]
impl Context<MyResource, MyK8sRepo, StaticApiProvider<MyResource>> for MyContext {
    fn k8s_repository(&self) -> Arc<MyK8sRepo> {
        Arc::clone(&self.repo)
    }

    fn finalizer(&self) -> &'static str {
        "myresources.example.kuberator.io/finalizer"
    }

    async fn handle_apply(&self, object: Arc<MyResource>) -> KubeResult<Action> {
        let name = object.metadata.name.as_ref().unwrap();
        let namespace = object.metadata.namespace.as_ref().unwrap();

        log::info!(
            "Reconciling MyResource {}/{}",
            namespace,
            name
        );

        // Check if we've already processed this generation
        if let Some(status) = &object.status {
            if let (Some(observed), Some(current)) = (status.observed_generation, object.metadata.generation) {
                if observed >= current {
                    log::info!("Already reconciled generation {}, skipping", current);
                    return Ok(Action::await_change());
                }
            }
        }

        log::info!("Processing new generation");

        // Perform your reconciliation logic here
        log::info!("Message: {}", object.spec.message);

        // Update status with ObserveGeneration pattern
        let status = MyResourceStatus {
            state: "Ready".to_string(),
            observed_generation: None, // Will be set by update_status()
        };

        self.repo.update_status(&object, status).await?;

        log::info!("Status updated successfully");

        Ok(Action::await_change())
    }

    async fn handle_cleanup(&self, object: Arc<MyResource>) -> KubeResult<Action> {
        let name = object.metadata.name.as_ref().unwrap();
        let namespace = object.metadata.namespace.as_ref().unwrap();

        log::info!(
            "Cleaning up MyResource {}/{}",
            namespace,
            name
        );

        // Update status before cleanup
        let status = MyResourceStatus {
            state: "Terminating".to_string(),
            observed_generation: None,
        };

        let _ = self.repo.update_status(&object, status).await;

        Ok(Action::await_change())
    }
}

// Define the reconciler
struct MyReconciler {
    context: Arc<MyContext>,
    crd_api: Api<MyResource>,
}

#[async_trait]
impl Reconcile<MyResource, MyContext, MyK8sRepo, StaticApiProvider<MyResource>> for MyReconciler {
    fn destruct(self) -> (Api<MyResource>, Config, Arc<MyContext>) {
        (self.crd_api, Config::default(), self.context)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    env_logger::init();

    log::info!("Starting Operator with Status Example");

    // Create Kubernetes client
    let client = Client::try_default().await?;

    // Create API provider
    let api_provider = StaticApiProvider::new(
        client.clone(),
        vec!["default"],
        CachingStrategy::Strict,
    );

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

    log::info!("Watching MyResources in 'default' namespace");
    log::info!("This example requires the MyResource CRD to be installed");

    // Start the reconciler
    reconciler.start::<futures::future::Ready<()>>(None).await;

    Ok(())
}
