//! `Kuberator`is a Kubernetes Operator Framework designed to simplify the process of
//! building Kubernetes Operators. It is still in its early stages and a work in progress.
//!
//! ## Usage
//!
//! It's best to follow an example to understand how to use `kuberator` in it's current form.
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! use async_trait::async_trait;
//! use kube::runtime::controller::Action;
//! use kube::runtime::watcher::Config;
//! use kube::Api;
//! use kube::Client;
//! use kube::CustomResource;
//! use kuberator::cache::StaticApiProvider;
//! use kuberator::cache::CachingStrategy;
//! use kuberator::error::Result as KubeResult;
//! use kuberator::Context;
//! use kuberator::Finalize;
//! use kuberator::Reconcile;
//! use schemars::JsonSchema;
//! use serde::Deserialize;
//! use serde::Serialize;
//!
//! // First, we need to define a custom resource that the operator will manage.
//! #[derive(CustomResource, Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
//! #[kube(
//!     group = "commercetools.com",
//!     version = "v1",
//!     kind = "MyCrd",
//!     plural = "mycrds",
//!     shortname = "mc",
//!     derive = "PartialEq",
//!     namespaced
//! )]
//! pub struct MySpec {
//!     pub my_property: String,
//! }
//!
//! // The core of the operator is the implementation of the `Reconcile` trait, which requires
//! // us to first implement the `Context` and `Finalize` traits for certain structs.
//!
//! // Option 1: Use the generic K8sRepository (recommended for simple cases)
//! use kuberator::k8s::K8sRepository;
//! type MyK8sRepo = K8sRepository<MyCrd, StaticApiProvider<MyCrd>>;
//!
//! // Option 2: Create a custom repository (if you need custom state or methods)
//! // struct MyK8sRepo {
//! //     api_provider: StaticApiProvider<MyCrd>,
//! // }
//! //
//! // impl Finalize<MyCrd, StaticApiProvider<MyCrd>> for MyK8sRepo {
//! //     fn api_provider(&self) -> &StaticApiProvider<MyCrd> {
//! //         &self.api_provider
//! //     }
//! // }
//!
//! // The `Context` trait must be implemented on a struct that serves as the core of the
//! // operator. It contains the logic for handling the custom resource object, including
//! // creation, updates, and deletion.
//! struct MyContext {
//!     repo: Arc<MyK8sRepo>,
//! }
//!
//! #[async_trait]
//! impl Context<MyCrd, MyK8sRepo, StaticApiProvider<MyCrd>> for MyContext {
//!     // The only requirement is to provide a unique finalizer name and an Arc to an
//!     // implementation of the `Finalize` trait.
//!     fn k8s_repository(&self) -> Arc<MyK8sRepo> {
//!         Arc::clone(&self.repo)
//!     }
//!
//!     fn finalizer(&self) -> &'static str {
//!         "mycrds.commercetools.com/finalizers"
//!     }
//!
//!     // The core of the `Context` trait consists of the two hook functions `handle_apply`
//!     // and `handle_cleanup`.Keep in mind that both functions must be idempotent.
//!
//!     // The `handle_apply` function is triggered whenever a custom resource object is
//!     // created or updated.
//!     async fn handle_apply(&self, object: Arc<MyCrd>) -> KubeResult<Action> {
//!         // do whatever you want with your custom resource object
//!         println!("My property is: {}", object.spec.my_property);
//!         Ok(Action::await_change())
//!     }
//!
//!     // The `handle_cleanup` function is triggered when a custom resource object is deleted.
//!     async fn handle_cleanup(&self, object: Arc<MyCrd>) -> KubeResult<Action> {
//!         // do whatever you want with your custom resource object
//!         println!("My property is: {}", object.spec.my_property);
//!         Ok(Action::await_change())
//!     }
//! }
//!
//! // The final step is to implement the `Reconcile` trait on a struct that holds the context.
//! // The Reconciler is responsible for starting the controller runtime and managing the
//! // reconciliation loop.
//!
//! // The `destruct` function is used to retrieve the Api, Config, and context.
//! // And that’s basically it!
//! struct MyReconciler {
//!     context: Arc<MyContext>,
//!     crd_api: Api<MyCrd>,
//! }
//!
//! #[async_trait]
//! impl Reconcile<MyCrd, MyContext, MyK8sRepo, StaticApiProvider<MyCrd>> for MyReconciler {
//!     fn destruct(self) -> (Api<MyCrd>, Config, Arc<MyContext>) {
//!         (self.crd_api, Config::default(), self.context)
//!     }
//! }
//!
//! // Now we can wire everything together in the main function and start the reconciler.
//! // It will continuously watch for custom resource objects and invoke the `handle_apply` and
//! // `handle_cleanup` functions as part of the reconciliation loop.
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let client = Client::try_default().await?;
//!
//!     // Using the generic K8sRepository:
//!     let api_provider = StaticApiProvider::new(
//!         client.clone(),
//!         vec!["default"],
//!         CachingStrategy::Strict
//!     );
//!     let k8s_repo = K8sRepository::new(api_provider);
//!
//!     // Or if using custom repository:
//!     // let k8s_repo = MyK8sRepo {
//!     //     api_provider: StaticApiProvider::new(
//!     //         client.clone(),
//!     //         vec!["default"],
//!     //         CachingStrategy::Strict
//!     //     ),
//!     // };
//!
//!     let context = MyContext {
//!         repo: Arc::new(k8s_repo),
//!     };
//!
//!     let reconciler = MyReconciler {
//!         context: Arc::new(context),
//!         crd_api: Api::namespaced(client, "default"),
//!     };
//!
//!     // Start the reconciler, which will handle the reconciliation loop synchronously.
//!     reconciler.start(None).await;
//!
//!     // If you want to run the reconciler asynchronously, you can use the `start_concurrent` method.
//!     // reconciler.start_concurrent(Some(10), None).await;
//!
//!     Ok(())
//! }
//! ```
//!
//! The second parameter of the `start` and `start_concurrent` methods is an optional graceful shutdown
//! signal. You can pass a future that resolves when you want to shut down the reconciler, like an OS
//! signal, e.g. `SIGTERM`.
//!
//! ```rust,ignore
//! use tokio::signal;
//! use futures::select;
//!
//! let shutdown_signal = async {
//!     let mut interrupt = signal::ctrl_c();
//!     let mut terminate = signal::unix::signal(signal::unix::SignalKind::terminate())
//!         .expect("Failed to install SIGTERM handler");
//!
//!     select! {
//!         _ = interrupt => log::info!("Received SIGINT, shutting down"),
//!         _ = terminate.recv() => log::info!("Received SIGTERM, shutting down"),
//!     }
//! };
//!
//! reconciler.start(Some(shutdown_signal)).await;
//! ```
//!
//!
//! ## Error Handling
//!
//! Kuberator provides a dedicated error type. When implementing `Reconcile::handle_apply`
//! and `Reconcile::handle_cleanup`, you must return this error in your Result, or use
//! `kuberator::error::Result` directly.
//!
//! To convert your custom error into a Kuberator error, implement `From` for your error
//! type and wrap it using `Error::Anyhow`.
//!
//! Your `error.rs` file could look something like this:
//!
//! ```
//! use std::fmt::Debug;
//!
//! use kuberator::error::Error as KubeError;
//! use thiserror::Error as ThisError;
//!
//! pub type Result<T> = std::result::Result<T, Error>;
//!
//! #[derive(ThisError, Debug)]
//! pub enum Error {
//!     #[error("Kube error: {0}")]
//!     Kube(#[from] kube::Error),
//! }
//!
//! impl From<Error> for KubeError {
//!     fn from(error: Error) -> KubeError {
//!         KubeError::Anyhow(anyhow::anyhow!(error))
//!     }
//! }
//! ```
//!
//! With this approach, you can conveniently handle your custom errors using the `?` operator
//! and return them as Kuberator errors.
//!
//! ## Status Object Handling
//!
//! Kuberator provides helper methods to facilitate the **Observed Generation Pattern.** To use
//! this pattern, you need to implement the ObserveGeneration trait for your status object.
//!
//! Let's say this is your `status.rs` file:
//!
//! ```
//! use kuberator::ObserveGeneration;
//!
//! pub struct MyStatus {
//!     pub status: State,
//!     pub observed_generation: Option<i64>,
//! }
//!
//! pub enum State {
//!     Created,
//!     Updated,
//!     Deleted,
//! }
//!
//! impl ObserveGeneration for MyStatus {
//!     fn add(&mut self, observed_generation: i64) {
//!         self.observed_generation = Some(observed_generation);
//!     }
//! }
//! ```
//!
//! With this implementation, you can utilize the `update_status()` method provided by the
//! `Finalize` trait.
//!
//! This allows you to:
//! (a) Keep your resource status up to date.
//! (b) Compare it against the current generation of the resource (`object.meta().generation`)
//! to determine whether you have already processed this version or if it is a new show in
//! the reconciliation cycle.
//!
//! This pattern is particularly useful for ensuring idempotency in your reconciliation logic.
//!
//! ## Status Object Error handling
//!
//! If you want to handle errors in your status object, you can implement the `WithStatusError` trait.
//! This trait allows you to define how errors should be handled and reported in the status object.
//!
//! ```rust
//! use kuberator::WithStatusError;
//! use kuberator::AsStatusError;
//! use serde::Deserialize;
//! use serde::Serialize;
//! use schemars::JsonSchema;
//!
//! // You have a custom error type
//! enum MyError {
//!     NotFound,
//!     InvalidInput,
//! }
//!
//! // Additionally, you have you status object with a specific status error
//! #[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
//! struct MyStatus {
//!     pub status: String,
//!     pub observed_generation: Option<i64>,
//!     pub error: Option<MyStatusError>,
//! }
//!
//! #[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
//! pub struct MyStatusError {
//!     pub message: String,
//! }
//!
//! // Implement the `AsStatusError` trait for your custom error type
//! impl AsStatusError<MyStatusError> for MyError {
//!     fn as_status_error(&self) -> MyStatusError {
//!         match self {
//!             MyError::NotFound => MyStatusError {
//!                 message: "Resource not found".to_string(),
//!             },
//!             MyError::InvalidInput => MyStatusError {
//!                 message: "Invalid input provided".to_string(),
//!             },
//!         }
//!     }
//! }
//!
//! // And implement the `WithStatusError` trait that links your error and status error
//! impl WithStatusError<MyError, MyStatusError> for MyStatus {
//!     fn add(&mut self, error: MyStatusError) {
//!         self.error = Some(error);
//!     }
//! }
//! ```
//!
//! With this implementation, you can utilize the `update_status_with_error()` method provided by the
//! [Finalize] trait. This method takes care of adding the (optional) error to your status object.
//!
//! This way, you can handle errors in your status object and report them accordingly. You control how
//! errors are represented in the status object from the source up, by implementing `AsStatusError` for
//! your error type.
//!

pub mod cache;
pub mod error;
pub mod k8s;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::StreamExt;
use futures::TryFuture;
use k8s_openapi::NamespaceResourceScope;
use kube::api::ObjectMeta;
use kube::api::Patch;
use kube::api::PatchParams;
use kube::runtime::controller::Action;
use kube::runtime::controller::Error as KubeControllerError;
use kube::runtime::finalizer;
use kube::runtime::finalizer::Event;
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher::Config;
use kube::runtime::Controller;
use kube::Api;
use kube::Resource;
use kube::ResourceExt;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::cache::ProvideApi;
use crate::error::Error;
use crate::error::Result;

const REQUEUE_AFTER_ERROR_SECONDS: u64 = 60;

type ReconciliationResult<R, RE, QE> = StdResult<(ObjectRef<R>, Action), KubeControllerError<RE, QE>>;

/// The Reconcile trait takes care of the starting of the controller and the reconciliation loop.
///
/// The user needs to implement the [Reconcile::destruct] method as well as a component F that
/// implements [Finalize] and a component C that implements [Context] and uses component F.
#[async_trait]
pub trait Reconcile<R, C, F, P>: Sized
where
    R: Resource<Scope = NamespaceResourceScope> + Serialize + DeserializeOwned + Debug + Clone + Send + Sync + 'static,
    R::DynamicType: Default + Eq + Hash + Clone + Debug + Unpin,
    C: Context<R, F, P> + 'static,
    F: Finalize<R, P>,
    P: ProvideApi<R> + 'static,
{
    /// Starts the controller and runs the reconciliation loop, where as reconciliations run
    /// synchronously. If you want asynchronous reconciliations, use [Reconcile::start_concurrent].
    async fn start<G>(self, graceful_trigger: Option<G>)
    where
        G: Future<Output = ()> + Send + Sync + 'static,
    {
        let (crd_api, config, context) = self.destruct();

        controller(crd_api, config, graceful_trigger)
            .run(Self::reconcile, Self::error_policy, context)
            .for_each(Self::handle_reconciliation_result)
            .await;
    }

    /// Starts the controller and runs the reconciliation loop, where as reconciliations run
    /// concurrently.
    ///
    /// `limit` is the maximum number of concurrent reconciliations that can be processed.
    /// If it is set to `None` there is no hard limit on the number of concurrent reconciliations.
    /// `Some(0)` has the same effect as `None`.
    async fn start_concurrent<G>(self, limit: Option<usize>, graceful_trigger: Option<G>)
    where
        G: Future<Output = ()> + Send + Sync + 'static,
    {
        let (crd_api, config, context) = self.destruct();

        controller(crd_api, config, graceful_trigger)
            .run(Self::reconcile, Self::error_policy, context)
            .for_each_concurrent(limit, Self::handle_reconciliation_result)
            .await;
    }

    /// Callback method for the controller that is called when a resource is reconciled and that
    /// is hooked into [Reconcile::start].
    async fn reconcile(resource: Arc<R>, context: Arc<C>) -> Result<Action> {
        Ok(context.handle_reconciliation(resource).await?)
    }

    /// Callback method for the controller that is called when an error occurs during
    /// reconciliation. The method is hooked into [Reconcile::start].
    fn error_policy(resource: Arc<R>, error: &Error, context: Arc<C>) -> Action {
        context.handle_error(resource, error, Self::requeue_after_error_seconds())
    }

    /// Handles the result of a reconciliation and is called by the controller in the
    /// [Reconcile::start] method for each reconiliation.
    async fn handle_reconciliation_result<RE, QE>(reconciliation_result: ReconciliationResult<R, RE, QE>)
    where
        RE: Debug + Send,
        QE: Debug + Send,
    {
        match reconciliation_result {
            Ok(resource) => {
                log::info!("Reconciliation successful. Resource: {resource:?}");
            }
            Err(error) => {
                log::error!("Reconciliation error: {error:?}");
            }
        }
    }

    /// Returns the duration after which a resource is requeued after an error occurred.
    ///
    /// The method is used as a default implementation for the [Reconcile::error_policy] method.
    /// Feel free to override it in your struct implementing the [Reconcile] trait to suit your
    /// specific needs.
    fn requeue_after_error_seconds() -> Option<Duration> {
        Some(Duration::from_secs(REQUEUE_AFTER_ERROR_SECONDS))
    }

    /// Destructs components from the implementing struct that are injected into the
    /// controller in the [Reconcile::start] method.
    fn destruct(self) -> (Api<R>, Config, Arc<C>);
}

fn controller<R, G>(crd_api: Api<R>, config: Config, graceful_trigger: Option<G>) -> Controller<R>
where
    R: Resource<Scope = NamespaceResourceScope> + Serialize + DeserializeOwned + Debug + Clone + Send + Sync + 'static,
    R::DynamicType: Default + Eq + Hash + Clone + Debug + Unpin,
    G: Future<Output = ()> + Send + Sync + 'static,
{
    let controller = Controller::new(crd_api, config);

    if let Some(trigger) = graceful_trigger {
        return controller.graceful_shutdown_on(trigger);
    };

    controller
}

/// The Context trait takes care of the apply and cleanup logic of a resource.
#[async_trait]
pub trait Context<R, F, P>: Send + Sync
where
    R: Resource<Scope = NamespaceResourceScope> + Serialize + DeserializeOwned + Debug + Clone + Send + Sync + 'static,
    R::DynamicType: Default,
    F: Finalize<R, P>,
    P: ProvideApi<R>,
{
    /// Handles a successful reconciliation of a resource.
    ///
    /// The method is called by the controller in [Reconcile::start] when a resource is reconciled.
    /// The method takes care of the finalizers of the resource as well as the [Context::handle_apply]
    /// and [Context::handle_cleanup] logic.
    async fn handle_reconciliation(&self, object: Arc<R>) -> Result<Action> {
        let action = self
            .k8s_repository()
            .finalize(self.finalizer(), object, |event| async {
                match event {
                    Event::Apply(object) => self.handle_apply(object).await,
                    Event::Cleanup(object) => self.handle_cleanup(object).await,
                }
            })
            .await?;

        Ok(action)
    }

    /// Handles an error that occurs during reconciliation.
    ///
    /// The method is called by the controller in [Reconcile::start] when an error occurs during
    /// reconciliation.
    ///
    /// This method is used as a default implementation for the [Reconcile::error_policy] method
    /// that logs the error and requeues the resource or not depending on the requeue parameter.
    ///
    /// Feel free to override it in your struct implementing the [Context] trait to suit your
    /// specific needs.
    fn handle_error(&self, object: Arc<R>, error: &Error, requeue: Option<Duration>) -> Action {
        log::error!(resource:serde = object; "Reconciliation error:\n{error:?}.");
        requeue.map_or_else(Action::await_change, Action::requeue)
    }

    /// Returns the k8s repository provided by the struct implementing the [Context] trait.
    fn k8s_repository(&self) -> Arc<F>;

    /// Returns the resources specific finalizer name provided by the struct implementing the
    /// [Context] trait.
    fn finalizer(&self) -> &'static str;

    /// Handles the apply logic of a resource.
    ///
    /// This method needs to be implemented by the struct implementing the [Context] trait. It
    /// handles all the logic that is needed to apply a resource: creation & updates.
    ///
    /// This method needs to be idempotent and tolerate being executed several times (even if
    /// previously cancelled).
    async fn handle_apply(&self, object: Arc<R>) -> Result<Action>;

    /// Handles the cleanup logic of a resource.
    ///
    /// This method needs to be implemented by the struct implementing the [Context] trait. It
    /// handles all the logic that is needed to clean up a resource: the deletion.
    ///
    /// This method needs to be idempotent and tolerate being executed several times (even if
    /// previously cancelled).
    async fn handle_cleanup(&self, object: Arc<R>) -> Result<Action>;
}

/// The Finalize trait takes care of the finalizers of a resource as well the reconciliation
/// changes to the k8s resource itself.
///
/// This component needs to be implemented by a struct that is used by the [Context] component,
/// something like a k8s repository, as it interacts with the k8s api directly.
#[async_trait]
pub trait Finalize<R, P>: Send + Sync
where
    R: Resource<Scope = NamespaceResourceScope> + Serialize + DeserializeOwned + Debug + Clone + Send + Sync + 'static,
    R::DynamicType: Default,
    P: ProvideApi<R>,
{
    /// Returns the provider for k8s Api.
    fn api_provider(&self) -> &P;

    /// Delegates the finalization logic to the [kube::runtime::finalizer::finalizer] function
    /// of the kube runtime by utilizing the reconcile function injected by the [Context].
    ///
    /// As this method is dealing with meta.finalizers of a component it might trigger
    /// a new reconiliation of the resource. This happens in case of the creation and the
    /// deletion of the resource.
    async fn finalize<ReconcileFut>(
        &self,
        finalizer_name: &str,
        object: Arc<R>,
        reconcile: impl FnOnce(Event<R>) -> ReconcileFut + Send,
    ) -> Result<Action>
    where
        ReconcileFut: TryFuture<Ok = Action> + Send,
        ReconcileFut::Error: StdError + Send + 'static,
    {
        let api = self.api_provider().get(&object.try_namespace()?)?;
        finalizer(&api, finalizer_name, object, reconcile)
            .await
            .map_err(Error::from)
    }

    /// Updates the status object of a resource.
    ///
    /// Updating the status does not trigger a new reconiliation loop.
    async fn update_status<S>(&self, object: &R, mut status: S) -> Result<()>
    where
        S: Serialize + ObserveGeneration + Debug + Send + Sync,
    {
        let api = self.api_provider().get(&object.try_namespace()?)?;

        status.with_observed_gen(object.meta());
        let new_status = Status { status };
        api.patch_status(object.try_name()?, &PatchParams::default(), &Patch::Merge(&new_status))
            .await?;

        Ok(())
    }

    /// Updates the status object of a resource with an optional error.
    ///
    /// Updating the status does not trigger a new reconiliation loop.
    async fn update_status_with_error<S, A, E>(&self, object: &R, mut status: S, error: Option<&A>) -> Result<()>
    where
        S: Serialize + ObserveGeneration + WithStatusError<A, E> + Debug + Send + Sync,
        A: AsStatusError<E> + Send + Sync,
        E: Serialize + Debug + PartialEq + Clone + JsonSchema,
        E: for<'de> Deserialize<'de>,
    {
        if let Some(error) = error {
            status.with_status_error(error);
        }
        self.update_status(object, status).await
    }
}

/// The Status struct is a serializable wrapper around the status object S of a resource.
///
/// The status object S needs to implement the [ObserveGeneration] trait.
#[derive(Debug, Serialize)]
struct Status<S>
where
    S: Serialize + ObserveGeneration + Debug,
{
    status: S,
}

/// The ObserveGeneration trait is used to update the observed generation of a resource.
///
/// The user needs to implement the [ObserveGeneration::add] method to update the observed generation.
pub trait ObserveGeneration {
    /// Updates the observed generation of a resource, e.g. updating a property of the status
    /// object that implements [ObserveGeneration].
    fn add(&mut self, observed_generation: i64);

    /// Updates the observed generation of a resource with the generation of the resource's
    /// metadata.
    fn with_observed_gen(&mut self, meta: &ObjectMeta) {
        let observed_generation = meta.generation;
        if let Some(observed_generation) = observed_generation {
            self.add(observed_generation)
        }
    }
}

/// The AsStatusError trait is used to convert a resource into a status error.
///
/// This way a user can convert control how the errors that are shown in the status objects
/// are shown to the user.
pub trait AsStatusError<E>
where
    E: Serialize + Debug + PartialEq + Clone + JsonSchema,
    E: for<'de> Deserialize<'de>,
{
    /// Converts the self into a status error.
    fn as_status_error(&self) -> E;
}

/// The WithStatusError trait is used to add a status error to a resource.
///
/// The user needs to implement the [WithStatusError::add] method to handle the logic to how
/// the status error is added to the resource to then be able to use the convenience method
/// [WithStatusError::with_status_error].
pub trait WithStatusError<A, E>
where
    A: AsStatusError<E>,
    E: Serialize + Debug + PartialEq + Clone + JsonSchema,
    E: for<'de> Deserialize<'de>,
{
    /// Adds a status error to the status object of a resource.
    fn add(&mut self, error: E);

    /// Takes an error that implements the [AsStatusError] trait and adds it to the status as
    /// a status error.
    fn with_status_error(&mut self, error: &A) {
        self.add(error.as_status_error());
    }
}

/// The TryResource trait is used to try to extract the name and the namespace of a resources
/// metadata and encapsulates the error handling.
pub trait TryResource {
    fn try_name(&self) -> Result<&str>;
    fn try_namespace(&self) -> Result<String>;
}

impl<R> TryResource for R
where
    R: Resource,
{
    fn try_name(&self) -> Result<&str> {
        self.meta().name.as_deref().ok_or(Error::UnnamedObject)
    }

    fn try_namespace(&self) -> Result<String> {
        self.namespace().ok_or(Error::UserInputError({
            "Expected resource to be namespaced. Can't deploy to an unknown namespace.".to_owned()
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::ConfigMap;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    // Test structs for ObserveGeneration
    #[derive(Debug, Clone)]
    struct TestStatus {
        #[allow(dead_code)]
        state: String,
        observed_generation: Option<i64>,
    }

    impl ObserveGeneration for TestStatus {
        fn add(&mut self, observed_generation: i64) {
            self.observed_generation = Some(observed_generation);
        }
    }

    // Test structs for AsStatusError and WithStatusError
    #[derive(Debug, PartialEq, Clone)]
    enum TestError {
        NotFound,
        InvalidInput(String),
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
    struct TestStatusError {
        message: String,
        code: i32,
    }

    impl AsStatusError<TestStatusError> for TestError {
        fn as_status_error(&self) -> TestStatusError {
            match self {
                TestError::NotFound => TestStatusError {
                    message: "Resource not found".to_string(),
                    code: 404,
                },
                TestError::InvalidInput(msg) => TestStatusError {
                    message: format!("Invalid input: {}", msg),
                    code: 400,
                },
            }
        }
    }

    #[derive(Debug, Clone)]
    struct TestStatusWithError {
        #[allow(dead_code)]
        state: String,
        error: Option<TestStatusError>,
    }

    impl WithStatusError<TestError, TestStatusError> for TestStatusWithError {
        fn add(&mut self, error: TestStatusError) {
            self.error = Some(error);
        }
    }

    #[tokio::test]
    async fn test_observe_generation_add() {
        // Given: A test status object with no observed generation
        let mut status = TestStatus {
            state: "Running".to_string(),
            observed_generation: None,
        };

        // When: Adding an observed generation
        status.add(42);

        // Then: The observed generation should be set
        assert_eq!(status.observed_generation, Some(42));
    }

    #[tokio::test]
    async fn test_observe_generation_with_observed_gen() {
        // Given: A test status object and metadata with a generation
        let mut status = TestStatus {
            state: "Running".to_string(),
            observed_generation: None,
        };
        let meta = ObjectMeta {
            generation: Some(123),
            ..Default::default()
        };

        // When: Calling with_observed_gen
        status.with_observed_gen(&meta);

        // Then: The observed generation should be extracted from metadata
        assert_eq!(status.observed_generation, Some(123));
    }

    #[tokio::test]
    async fn test_observe_generation_with_observed_gen_no_generation() {
        // Given: A test status object and metadata without a generation
        let mut status = TestStatus {
            state: "Running".to_string(),
            observed_generation: None,
        };
        let meta = ObjectMeta {
            generation: None,
            ..Default::default()
        };

        // When: Calling with_observed_gen
        status.with_observed_gen(&meta);

        // Then: The observed generation should remain None
        assert_eq!(status.observed_generation, None);
    }

    #[tokio::test]
    async fn test_as_status_error_not_found() {
        // Given: A TestError::NotFound error
        let error = TestError::NotFound;

        // When: Converting to status error
        let status_error = error.as_status_error();

        // Then: Should produce correct status error
        assert_eq!(status_error.message, "Resource not found");
        assert_eq!(status_error.code, 404);
    }

    #[tokio::test]
    async fn test_as_status_error_invalid_input() {
        // Given: A TestError::InvalidInput error with a message
        let error = TestError::InvalidInput("Bad format".to_string());

        // When: Converting to status error
        let status_error = error.as_status_error();

        // Then: Should produce correct status error with formatted message
        assert_eq!(status_error.message, "Invalid input: Bad format");
        assert_eq!(status_error.code, 400);
    }

    #[tokio::test]
    async fn test_with_status_error_add() {
        // Given: A test status object without error
        let mut status = TestStatusWithError {
            state: "Running".to_string(),
            error: None,
        };
        let error = TestStatusError {
            message: "Something went wrong".to_string(),
            code: 500,
        };

        // When: Adding a status error
        status.add(error.clone());

        // Then: The error should be added to status
        assert_eq!(status.error, Some(error));
    }

    #[tokio::test]
    async fn test_with_status_error_with_status_error() {
        // Given: A test status object without error and a TestError
        let mut status = TestStatusWithError {
            state: "Running".to_string(),
            error: None,
        };
        let error = TestError::InvalidInput("Missing field".to_string());

        // When: Converting and adding the error using with_status_error
        status.with_status_error(&error);

        // Then: The error should be converted and added to status
        assert!(status.error.is_some());
        let status_error = status.error.unwrap();
        assert_eq!(status_error.message, "Invalid input: Missing field");
        assert_eq!(status_error.code, 400);
    }

    #[tokio::test]
    async fn test_try_resource_try_name_success() {
        // Given: A ConfigMap with a name
        let config_map = ConfigMap {
            metadata: ObjectMeta {
                name: Some("my-config".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        // When: Calling try_name
        let result = config_map.try_name();

        // Then: Should return the name
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "my-config");
    }

    #[tokio::test]
    async fn test_try_resource_try_name_unnamed() {
        // Given: A ConfigMap without a name
        let config_map = ConfigMap {
            metadata: ObjectMeta {
                name: None,
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        // When: Calling try_name
        let result = config_map.try_name();

        // Then: Should return UnnamedObject error
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::UnnamedObject));
    }

    #[tokio::test]
    async fn test_try_resource_try_namespace_success() {
        // Given: A ConfigMap with a namespace
        let config_map = ConfigMap {
            metadata: ObjectMeta {
                name: Some("my-config".to_string()),
                namespace: Some("production".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        // When: Calling try_namespace
        let result = config_map.try_namespace();

        // Then: Should return the namespace
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "production");
    }

    #[tokio::test]
    async fn test_try_resource_try_namespace_missing() {
        // Given: A ConfigMap without a namespace
        let config_map = ConfigMap {
            metadata: ObjectMeta {
                name: Some("my-config".to_string()),
                namespace: None,
                ..Default::default()
            },
            ..Default::default()
        };

        // When: Calling try_namespace
        let result = config_map.try_namespace();

        // Then: Should return UserInputError
        assert!(result.is_err());
        if let Err(Error::UserInputError(msg)) = result {
            assert!(msg.contains("Expected resource to be namespaced"));
        } else {
            panic!("Expected UserInputError");
        }
    }

    // ==================== Tests for Finalize, Context, Reconcile ====================

    use crate::cache::{CachingStrategy, StaticApiProvider};
    use kube::client::Body;
    use kube::runtime::controller::Action;
    use kube::CustomResource;
    use kube::{Api, Client};
    use std::time::Duration;
    use tower_test::mock;

    #[derive(CustomResource, Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
    #[kube(
        group = "test.kuberator.io",
        version = "v1",
        kind = "TestResource",
        plural = "testresources",
        namespaced
    )]
    struct TestResourceSpec {
        value: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
    #[allow(dead_code)]
    struct TestResourceStatus {
        state: String,
        observed_generation: Option<i64>,
    }

    impl ObserveGeneration for TestResourceStatus {
        fn add(&mut self, observed_generation: i64) {
            self.observed_generation = Some(observed_generation);
        }
    }

    struct MockFinalize {
        api_provider: StaticApiProvider<TestResource>,
    }

    #[async_trait]
    impl Finalize<TestResource, StaticApiProvider<TestResource>> for MockFinalize {
        fn api_provider(&self) -> &StaticApiProvider<TestResource> {
            &self.api_provider
        }
    }

    struct MockContext {
        finalize: Arc<MockFinalize>,
        apply_called: Arc<std::sync::Mutex<bool>>,
        cleanup_called: Arc<std::sync::Mutex<bool>>,
    }

    #[async_trait]
    impl Context<TestResource, MockFinalize, StaticApiProvider<TestResource>> for MockContext {
        fn k8s_repository(&self) -> Arc<MockFinalize> {
            Arc::clone(&self.finalize)
        }

        fn finalizer(&self) -> &'static str {
            "test.kuberator.io/finalizer"
        }

        async fn handle_apply(&self, _object: Arc<TestResource>) -> Result<Action> {
            *self.apply_called.lock().unwrap() = true;
            Ok(Action::await_change())
        }

        async fn handle_cleanup(&self, _object: Arc<TestResource>) -> Result<Action> {
            *self.cleanup_called.lock().unwrap() = true;
            Ok(Action::await_change())
        }
    }

    fn test_client_for_traits() -> Client {
        let (mock_service, _handle) = mock::pair::<http::Request<Body>, http::Response<hyper::body::Incoming>>();
        Client::new(mock_service, "default")
    }

    #[tokio::test]
    async fn test_finalize_api_provider() {
        // Given: A MockFinalize with a StaticApiProvider
        let client = test_client_for_traits();
        let api_provider = StaticApiProvider::new(client, vec!["default"], CachingStrategy::Strict);
        let finalize = MockFinalize { api_provider };

        // When: Calling api_provider()
        let provider = finalize.api_provider();

        // Then: Should return a reference to the provider
        let result = provider.get("default");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_context_k8s_repository() {
        // Given: A MockContext with a MockFinalize
        let client = test_client_for_traits();
        let api_provider = StaticApiProvider::new(client, vec!["default"], CachingStrategy::Strict);
        let finalize = Arc::new(MockFinalize { api_provider });
        let context = MockContext {
            finalize: Arc::clone(&finalize),
            apply_called: Arc::new(std::sync::Mutex::new(false)),
            cleanup_called: Arc::new(std::sync::Mutex::new(false)),
        };

        // When: Calling k8s_repository()
        let repo = context.k8s_repository();

        // Then: Should return the repository
        assert!(repo.api_provider().get("default").is_ok());
    }

    #[tokio::test]
    async fn test_context_finalizer() {
        // Given: A MockContext
        let client = test_client_for_traits();
        let api_provider = StaticApiProvider::new(client, vec!["default"], CachingStrategy::Strict);
        let finalize = Arc::new(MockFinalize { api_provider });
        let context = MockContext {
            finalize,
            apply_called: Arc::new(std::sync::Mutex::new(false)),
            cleanup_called: Arc::new(std::sync::Mutex::new(false)),
        };

        // When: Calling finalizer()
        let finalizer_name = context.finalizer();

        // Then: Should return the correct finalizer name
        assert_eq!(finalizer_name, "test.kuberator.io/finalizer");
    }

    #[tokio::test]
    async fn test_context_handle_apply() {
        // Given: A MockContext and a test resource
        let client = test_client_for_traits();
        let api_provider = StaticApiProvider::new(client, vec!["default"], CachingStrategy::Strict);
        let finalize = Arc::new(MockFinalize { api_provider });
        let apply_called = Arc::new(std::sync::Mutex::new(false));
        let context = MockContext {
            finalize,
            apply_called: Arc::clone(&apply_called),
            cleanup_called: Arc::new(std::sync::Mutex::new(false)),
        };

        let test_resource = TestResource::new(
            "test-resource",
            TestResourceSpec {
                value: "test-value".to_string(),
            },
        );

        // When: Calling handle_apply
        let result = context.handle_apply(Arc::new(test_resource)).await;

        // Then: Should succeed and mark apply as called
        assert!(result.is_ok());
        assert!(*apply_called.lock().unwrap());
    }

    #[tokio::test]
    async fn test_context_handle_cleanup() {
        // Given: A MockContext and a test resource
        let client = test_client_for_traits();
        let api_provider = StaticApiProvider::new(client, vec!["default"], CachingStrategy::Strict);
        let finalize = Arc::new(MockFinalize { api_provider });
        let cleanup_called = Arc::new(std::sync::Mutex::new(false));
        let context = MockContext {
            finalize,
            apply_called: Arc::new(std::sync::Mutex::new(false)),
            cleanup_called: Arc::clone(&cleanup_called),
        };

        let test_resource = TestResource::new(
            "test-resource",
            TestResourceSpec {
                value: "test-value".to_string(),
            },
        );

        // When: Calling handle_cleanup
        let result = context.handle_cleanup(Arc::new(test_resource)).await;

        // Then: Should succeed and mark cleanup as called
        assert!(result.is_ok());
        assert!(*cleanup_called.lock().unwrap());
    }

    #[tokio::test]
    async fn test_context_handle_error() {
        // Given: A MockContext, an error, and a test resource
        let client = test_client_for_traits();
        let api_provider = StaticApiProvider::new(client, vec!["default"], CachingStrategy::Strict);
        let finalize = Arc::new(MockFinalize { api_provider });
        let context = MockContext {
            finalize,
            apply_called: Arc::new(std::sync::Mutex::new(false)),
            cleanup_called: Arc::new(std::sync::Mutex::new(false)),
        };

        let test_resource = TestResource::new(
            "test-resource",
            TestResourceSpec {
                value: "test-value".to_string(),
            },
        );
        let error = Error::UserInputError("Test error".to_string());

        // When: Calling handle_error with requeue duration
        let action = context.handle_error(Arc::new(test_resource.clone()), &error, Some(Duration::from_secs(30)));

        // Then: Should return requeue action with the specified duration
        let expected_requeue = Action::requeue(Duration::from_secs(30));
        assert_eq!(action, expected_requeue);

        // When: Calling handle_error without requeue
        let action_no_requeue = context.handle_error(Arc::new(test_resource), &error, None);

        // Then: Should return await_change action
        let expected_await_change = Action::await_change();
        assert_eq!(action_no_requeue, expected_await_change);
    }

    #[tokio::test]
    async fn test_reconcile_requeue_after_error_seconds() {
        // Mock Reconcile implementation
        struct MockReconcile;

        impl Reconcile<TestResource, MockContext, MockFinalize, StaticApiProvider<TestResource>> for MockReconcile {
            fn destruct(self) -> (Api<TestResource>, kube::runtime::watcher::Config, Arc<MockContext>) {
                unimplemented!("Not needed for this test")
            }
        }

        // When: Calling requeue_after_error_seconds with default implementation
        let duration = MockReconcile::requeue_after_error_seconds();

        // Then: Should return the default 60 seconds
        assert_eq!(duration, Some(Duration::from_secs(60)));
    }
}
