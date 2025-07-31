# Kuberator - Kubernetes Operator Framework

`Kuberator`is a Kubernetes Operator Framework designed to simplify the process of 
building Kubernetes Operators. It is still in its early stages and a work in progress. 

## Usage

It's best to follow an example to understand how to use `kuberator` in it's current form.

```rust
use std::sync::Arc;

use async_trait::async_trait;
use kube::runtime::controller::Action;
use kube::runtime::watcher::Config;
use kube::Api;
use kube::Client;
use kube::CustomResource;
use kuberator::error::Result as KubeResult;
use kuberator::Context;
use kuberator::Finalize;
use kuberator::Reconcile;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

// First, we need to define a custom resource that the operator will manage.
#[derive(CustomResource, Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[kube(
    group = "commercetools.com",
    version = "v1",
    kind = "MyCrd",
    plural = "mycrds",
    shortname = "mc",
    derive = "PartialEq",
    namespaced
)]
pub struct MySpec {
    pub my_property: String,
}

// The core of the operator is the implementation of the [Reconcile] trait, which requires
// us to first implement the [Context] and [Finalize] traits for certain structs.

// The [Finalize] trait will be implemented on a Kubernetes repository-like structure
// and is responsible for handling the finalizer logic.
struct MyK8sRepo {
    client: Client,
}

impl Finalize<MyCrd> for MyK8sRepo {
    fn client(&self) -> Client {
        self.client.clone()
    }
}

// The [Context] trait must be implemented on a struct that serves as the core of the
// operator. It contains the logic for handling the custom resource object, including
// creation, updates, and deletion.
struct MyContext {
    repo: Arc<MyK8sRepo>,
}

#[async_trait]
impl Context<MyCrd, MyK8sRepo> for MyContext {
    // The only requirement is to provide a unique finalizer name and an Arc to an
    // implementation of the [Finalize] trait.
    fn k8s_repository(&self) -> Arc<MyK8sRepo> {
        self.repo.clone()
    }

    fn finalizer(&self) -> &'static str {
        "mycrds.commercetools.com/finalizers"
    }

    // The core of the [Context] trait consists of the two hook functions [handle_apply]
    // and [handle_cleanup].Keep in mind that both functions must be idempotent.

    // The [handle_apply] function is triggered whenever a custom resource object is
    // created or updated.
    async fn handle_apply(&self, object: Arc<MyCrd>) -> KubeResult<Action> {
        // do whatever you want with your custom resource object
        println!("My property is: {}", object.spec.my_property);
        Ok(Action::await_change())
    }

    // The [handle_cleanup] function is triggered when a custom resource object is deleted.
    async fn handle_cleanup(&self, object: Arc<MyCrd>) -> KubeResult<Action> {
        // do whatever you want with your custom resource object
        println!("My property is: {}", object.spec.my_property);
        Ok(Action::await_change())
    }
}

// The final step is to implement the [Reconcile] trait on a struct that holds the context.
// The Reconciler is responsible for starting the controller runtime and managing the
// reconciliation loop.

// The [destruct] function is used to retrieve the Api, Config, and context.
// And that’s basically it!
struct MyReconciler {
    context: Arc<MyContext>,
    crd_api: Api<MyCrd>,
}

#[async_trait]
impl Reconcile<MyCrd, MyContext, MyK8sRepo> for MyReconciler {
    fn destruct(self) -> (Api<MyCrd>, Config, Arc<MyContext>) {
        (self.crd_api, Config::default(), self.context)
    }
}

// Now we can wire everything together in the main function and start the reconciler.
// It will continuously watch for custom resource objects and invoke the [handle_apply] and
// [handle_cleanup] functions as part of the reconciliation loop.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::try_default().await?;

    let k8s_repo = MyK8sRepo {
        client: client.clone(),
    };
    let context = MyContext {
        repo: Arc::new(k8s_repo),
    };

    let reconciler = MyReconciler {
        context: Arc::new(context),
        crd_api: Api::namespaced(client, "default"),
    };

    // Start the reconciler, which will handle the reconciliation loop synchronously.
    reconciler.start().await;

    // If you want to run the reconciler asynchronously, you can use the `start_concurrent` method.
    // reconciler.start_concurrent(Some(10)).await;

    Ok(())
}
```

## Error Handling

Kuberator provides a dedicated error type. When implementing [Reconcile::handle_apply]
and [Reconcile::handle_cleanup], you must return this error in your Result, or use
[kuberator::error::Result] directly.

To convert your custom error into a Kuberator error, implement `From` for your error
type and wrap it using `Error::Anyhow`.

Your `error.rs` file could look something like this:

```rust
use std::fmt::Debug;

use kuberator::error::Error as KubeError;
use thiserror::Error as ThisError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("Kube error: {0}")]
    Kube(#[from] kube::Error),
}

impl From<Error> for KubeError {
    fn from(error: Error) -> KubeError {
        KubeError::Anyhow(anyhow::anyhow!(error))
    }
}
```

With this approach, you can conveniently handle your custom errors using the `?` operator
and return them as Kuberator errors.

## Status Object Handling

Kuberator provides helper methods to facilitate the **Observed Generation Pattern.** To use 
this pattern, you need to implement the ObserveGeneration trait for your status object.

Let's say this is your `status.rs` file:

```rust
use kuberator::ObserveGeneration;

pub struct MyStatus {
    pub status: State,
    pub observed_generation: Option<i64>,
}

pub enum State {
    Created,
    Updated,
    Deleted,
}

impl ObserveGeneration for MyStatus {
    fn add(&mut self, observed_generation: i64) {
        self.observed_generation = Some(observed_generation);
    }
}
```

With this implementation, you can utilize the `update_status()` method provided by the 
[Finalize] trait.

This allows you to:
(a) Keep your resource status up to date.
(b) Compare it against the current generation of the resource (`object.meta().generation`) 
to determine whether you have already processed this version or if it is a new show in 
the reconciliation cycle.

This pattern is particularly useful for ensuring idempotency in your reconciliation logic.

## License

[MIT](./LICENSE-MIT)
