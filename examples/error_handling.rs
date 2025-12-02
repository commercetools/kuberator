//! Error Handling with Status Example
//!
//! This example demonstrates error handling with the WithStatusError pattern,
//! which allows you to report errors in the resource status.
//!
//! To run this example:
//! ```bash
//! cargo run --example error_handling
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
use kuberator::{AsStatusError, Context, Finalize, ObserveGeneration, Reconcile, TryResource, WithStatusError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;

// Define a Custom Resource
#[derive(CustomResource, Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
#[kube(
    group = "example.kuberator.io",
    version = "v1",
    kind = "MyApp",
    plural = "myapps",
    namespaced,
    status = "MyAppStatus"
)]
pub struct MyAppSpec {
    pub replicas: i32,
    pub image: String,
}

// Define custom error types
#[derive(ThisError, Debug)]
pub enum MyAppError {
    #[error("Invalid replica count: {0}")]
    InvalidReplicas(i32),
    #[error("Invalid image format: {0}")]
    InvalidImage(String),
    #[error("External service unavailable")]
    ServiceUnavailable,
}

// Convert to Kuberator error
impl From<MyAppError> for kuberator::error::Error {
    fn from(error: MyAppError) -> kuberator::error::Error {
        kuberator::error::Error::Anyhow(anyhow::anyhow!(error))
    }
}

// Define status error representation
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
pub struct MyAppStatusError {
    pub code: String,
    pub message: String,
}

// Implement AsStatusError to convert app errors to status errors
impl AsStatusError<MyAppStatusError> for MyAppError {
    fn as_status_error(&self) -> MyAppStatusError {
        match self {
            MyAppError::InvalidReplicas(count) => MyAppStatusError {
                code: "INVALID_REPLICAS".to_string(),
                message: format!("Replica count {} is invalid (must be between 1 and 10)", count),
            },
            MyAppError::InvalidImage(image) => MyAppStatusError {
                code: "INVALID_IMAGE".to_string(),
                message: format!("Image '{}' has invalid format", image),
            },
            MyAppError::ServiceUnavailable => MyAppStatusError {
                code: "SERVICE_UNAVAILABLE".to_string(),
                message: "External service is temporarily unavailable".to_string(),
            },
        }
    }
}

// Define the status object
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
pub struct MyAppStatus {
    pub state: String,
    pub observed_generation: Option<i64>,
    pub error: Option<MyAppStatusError>,
}

impl ObserveGeneration for MyAppStatus {
    fn add(&mut self, observed_generation: i64) {
        self.observed_generation = Some(observed_generation);
    }
}

impl WithStatusError<MyAppError, MyAppStatusError> for MyAppStatus {
    fn add(&mut self, error: MyAppStatusError) {
        self.error = Some(error);
    }
}

// Type alias for our repository
type MyK8sRepo = K8sRepository<MyApp, StaticApiProvider<MyApp>>;

// Define the context
struct MyContext {
    repo: Arc<MyK8sRepo>,
}

impl MyContext {
    // Validation logic
    fn validate_spec(&self, spec: &MyAppSpec) -> Result<(), MyAppError> {
        // Validate replicas
        if spec.replicas < 1 || spec.replicas > 10 {
            return Err(MyAppError::InvalidReplicas(spec.replicas));
        }

        // Validate image format
        if !spec.image.contains(':') {
            return Err(MyAppError::InvalidImage(spec.image.clone()));
        }

        Ok(())
    }
}

#[async_trait]
impl Context<MyApp, MyK8sRepo, StaticApiProvider<MyApp>> for MyContext {
    fn k8s_repository(&self) -> Arc<MyK8sRepo> {
        Arc::clone(&self.repo)
    }

    fn finalizer(&self) -> &'static str {
        "myapps.example.kuberator.io/finalizer"
    }

    async fn handle_apply(&self, object: Arc<MyApp>) -> KubeResult<Action> {
        let name = object.try_name()?;
        let namespace = object.try_namespace()?;

        tracing::info!("Reconciling MyApp {}/{}", namespace, name);

        // Validate the spec
        match self.validate_spec(&object.spec) {
            Ok(_) => {
                tracing::info!("Validation passed");

                // Perform reconciliation
                tracing::info!(
                    "Deploying {} replicas with image {}",
                    object.spec.replicas,
                    object.spec.image
                );

                // Update status without error
                let status = MyAppStatus {
                    state: "Ready".to_string(),
                    observed_generation: None,
                    error: None,
                };

                self.repo.update_status(&object, status).await?;

                Ok(Action::await_change())
            }
            Err(e) => {
                tracing::error!("Validation failed: {}", e);

                // Update status with error
                let status = MyAppStatus {
                    state: "Error".to_string(),
                    observed_generation: None,
                    error: None, // Will be set by update_status_with_error
                };

                self.repo.update_status_with_error(&object, status, Some(&e)).await?;

                // Requeue after 60 seconds
                Ok(Action::requeue(std::time::Duration::from_secs(60)))
            }
        }
    }

    async fn handle_cleanup(&self, object: Arc<MyApp>) -> KubeResult<Action> {
        let name = object.try_name()?;
        let namespace = object.try_namespace()?;

        tracing::info!("Cleaning up MyApp {}/{}", namespace, name);

        // Update status
        let status = MyAppStatus {
            state: "Terminating".to_string(),
            observed_generation: None,
            error: None,
        };

        let _ = self.repo.update_status(&object, status).await;

        Ok(Action::await_change())
    }
}

// Define the reconciler
struct MyReconciler {
    context: Arc<MyContext>,
    crd_api: Api<MyApp>,
}

#[async_trait]
impl Reconcile<MyApp, MyContext, MyK8sRepo, StaticApiProvider<MyApp>> for MyReconciler {
    fn destruct(self) -> (Api<MyApp>, Config, Arc<MyContext>) {
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

    tracing::info!("Starting Error Handling Example");

    // Create Kubernetes client
    let client = Client::try_default().await?;

    // Create API provider
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

    tracing::info!("Watching MyApps in 'default' namespace");
    tracing::info!("This example requires the MyApp CRD to be installed");
    tracing::info!("Try creating MyApps with invalid replicas (<1 or >10) to see error handling");

    // Start the reconciler
    reconciler.start::<futures::future::Ready<()>>(None).await;

    Ok(())
}
