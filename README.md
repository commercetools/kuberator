# Kuberator - Kubernetes Operator Framework

`Kuberator` is a Kubernetes Operator Framework that should help you build Kubernetes Operators. And it
is a heavy work in progress.

## Usage

It's best to follow an example to understand how to use the `kuberator`.

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
        // do whatever you wnat with your custom resource object
        println!("My property is: {}", object.spec.my_property);
        Ok(Action::await_change())
    }

    // The [handle_cleanup] function is triggered when a custom resource object is deleted.
    async fn handle_cleanup(&self, object: Arc<MyCrd>) -> KubeResult<Action> {
        // do whatever you wnat with your custom resource object
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

    reconciler.start().await;

    Ok(())
}
```

## License

[MIT](./LICENSE-MIT)
