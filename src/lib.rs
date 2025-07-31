//! `Kuberator`is a Kubernetes Operator Framework designed to simplify the process of
//! building Kubernetes Operators. It is still in its early stages and a work in progress.
//!
//! ## Usage
//!
//! It's best to follow an example to understand how to use `kuberator` in it's current form.
//!
//! ```
//! use std::sync::Arc;
//!
//! use async_trait::async_trait;
//! use kube::runtime::controller::Action;
//! use kube::runtime::watcher::Config;
//! use kube::Api;
//! use kube::Client;
//! use kube::CustomResource;
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
//! // The `Finalize` trait will be implemented on a Kubernetes repository-like structure
//! // and is responsible for handling the finalizer logic.
//! struct MyK8sRepo {
//!     client: Client,
//! }
//!
//! impl Finalize<MyCrd> for MyK8sRepo {
//!     fn client(&self) -> Client {
//!         self.client.clone()
//!     }
//! }
//!
//! // The `Context` trait must be implemented on a struct that serves as the core of the
//! // operator. It contains the logic for handling the custom resource object, including
//! // creation, updates, and deletion.
//! struct MyContext {
//!     repo: Arc<MyK8sRepo>,
//! }
//!
//! #[async_trait]
//! impl Context<MyCrd, MyK8sRepo> for MyContext {
//!     // The only requirement is to provide a unique finalizer name and an Arc to an
//!     // implementation of the `Finalize` trait.
//!     fn k8s_repository(&self) -> Arc<MyK8sRepo> {
//!         self.repo.clone()
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
//! impl Reconcile<MyCrd, MyContext, MyK8sRepo> for MyReconciler {
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
//!     let k8s_repo = MyK8sRepo {
//!         client: client.clone(),
//!     };
//!     let context = MyContext {
//!         repo: Arc::new(k8s_repo),
//!     };
//!
//!     let reconciler = MyReconciler {
//!         context: Arc::new(context),
//!         crd_api: Api::namespaced(client, "default"),
//!     };
//!
//!     reconciler.start(Some(10)).await;
//!
//!
//!     // Start the reconciler, which will handle the reconciliation loop synchronously.
//!     reconciler.start().await;
//!
//!     // If you want to run the reconciler asynchronously, you can use the `start_concurrent` method.
//!     // reconciler.start_concurrent(Some(10)).await;
//!
//!     Ok(())
//! }
//! ```
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
//! impl WithStatusError<MyError, MyStatusError> for AtlasClusterStatus {
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

pub mod error;

use std::error::Error as StdError;
use std::fmt::Debug;
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
use kube::Client;
use kube::Resource;
use kube::ResourceExt;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::error::Error;
use crate::error::Result;

const REQUEUE_AFTER_ERROR_SECONDS: u64 = 60;

type ReconciliationResult<R, RE, QE> = StdResult<(ObjectRef<R>, Action), KubeControllerError<RE, QE>>;

/// The Reconcile trait takes care of the starting of the controller and the reconciliation loop.
///
/// The user needs to implement the [Reconcile::destruct] method as well as a component F that
/// implements [Finalize] and a component C that implements [Context] and uses component F.
#[async_trait]
pub trait Reconcile<R, C, F>: Sized
where
    R: Resource<Scope = NamespaceResourceScope> + Serialize + DeserializeOwned + Debug + Clone + Send + Sync + 'static,
    R::DynamicType: Default + Eq + Hash + Clone + Debug + Unpin,
    C: Context<R, F> + 'static,
    F: Finalize<R>,
{
    /// Starts the controller and runs the reconciliation loop, where as reconciliations run
    /// synchronously. If you want asynchronous reconciliations, use [Reconcile::start_concurrent].
    async fn start(self) {
        let (crd_api, config, context) = self.destruct();
        Controller::new(crd_api, config)
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
    async fn start_concurrent(self, limit: Option<usize>) {
        let (crd_api, config, context) = self.destruct();
        Controller::new(crd_api, config)
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

/// The Context trait takes care of the apply and cleanup logic of a resource.
#[async_trait]
pub trait Context<R, F>: Send + Sync
where
    R: Resource<Scope = NamespaceResourceScope> + Serialize + DeserializeOwned + Debug + Clone + Send + Sync + 'static,
    R::DynamicType: Default,
    F: Finalize<R>,
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
pub trait Finalize<R>: Send + Sync
where
    R: Resource<Scope = NamespaceResourceScope> + Serialize + DeserializeOwned + Debug + Clone + Send + Sync + 'static,
    R::DynamicType: Default,
{
    /// Returns the k8s client that is used to interact with the k8s api.
    fn client(&self) -> Client;

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
        let api = Api::<R>::namespaced(self.client(), &object.try_namespace()?);
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
        let api = Api::<R>::namespaced(self.client(), &object.try_namespace()?);

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
