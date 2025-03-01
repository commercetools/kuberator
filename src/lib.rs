pub mod error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::hash::Hash;
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
use kube::runtime::finalizer;
use kube::runtime::finalizer::Event;
use kube::runtime::watcher::Config;
use kube::runtime::Controller;
use kube::Api;
use kube::Client;
use kube::Resource;
use kube::ResourceExt;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::Error;
use crate::error::Result;

const REQUEUE_AFTER_ERROR_SECONDS: u64 = 60;

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
    /// Starts the controller and runs the reconciliation loop.
    async fn start(self) {
        let (crd_api, config, context) = self.destruct();
        Controller::new(crd_api, config)
            .run(Self::reconcile, Self::error_policy, context)
            .for_each(|reconciliation_result| async move {
                match reconciliation_result {
                    Ok(resource) => {
                        log::info!("Reconciliation successful. Resource: {resource:?}");
                    }
                    Err(error) => {
                        log::error!("Reconciliation error: {error:?}");
                    }
                }
            })
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
    async fn update_status<S>(&self, object: Arc<R>, mut status: S) -> Result<()>
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
