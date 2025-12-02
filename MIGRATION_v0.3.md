# Migration Guide: v0.2 → v0.3

## Breaking Change: Switched from `log` to `tracing`

Kuberator v0.3 replaces the `log` crate with `tracing` for comprehensive observability in async contexts. This change provides framework-level instrumentation and better integration with modern Rust async ecosystems.

## Why This Change?

**Benefits you gain:**
- **Framework visibility**: See reconciliation loops, finalizer operations, and status updates as spans
- **Better debugging**: Correlate framework and business logic in a single trace
- **Production-ready observability**: Export traces to Jaeger, Tempo, Grafana, etc. via OpenTelemetry
- **Async-aware logging**: Automatic context propagation across await points
- **Ecosystem alignment**: Matches tokio, kube-rs, and modern Rust async patterns

## Migration Paths

### Path 1: You Already Use `tracing` ✅ (RECOMMENDED)

**No changes needed!** Your existing tracing setup will automatically capture kuberator's spans and logs.

You'll now see framework operations in your traces:
```
kuberator.reconcile (250ms)
  ├─ kuberator.handle_reconciliation (245ms)
  │   ├─ kuberator.finalize (5ms)
  │   ├─ reconcile.apply (220ms)  # Your business logic
  │   └─ kuberator.update_status (15ms)
```

### Path 2: You Use `log` Crate

Add the `tracing-log` bridge to maintain compatibility:

#### Cargo.toml
```toml
[dependencies]
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
tracing-log = "0.2"  # Bridge for log compatibility
```

#### main.rs
```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Bridge log calls to tracing (if you still use log::* macros)
    tracing_log::LogTracer::init()?;

    // Now your existing log::info!() calls work automatically

    // ... rest of your code
}
```

#### Migrate Your Logging (Optional but Recommended)

Replace `log::*` macros with `tracing::*` equivalents:

**Before:**
```rust
log::info!("Processing resource {}", name);
log::error!("Failed to update: {:?}", error);
```

**After:**
```rust
tracing::info!(resource_name = %name, "Processing resource");
tracing::error!(error = ?error, "Failed to update");
```

### Path 3: You Use Neither

Add tracing initialization to your operator:

#### Cargo.toml
```toml
[dependencies]
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

#### main.rs
```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // ... your operator code
}
```

## What's Instrumented in Kuberator

Kuberator v0.3 adds comprehensive instrumentation:

| Span Name | What It Tracks | Fields |
|-----------|---------------|---------|
| `kuberator.reconcile` | Top-level reconciliation entry point | resource_name, resource_namespace, resource_generation |
| `kuberator.handle_reconciliation` | Finalizer management and routing | resource_name, resource_namespace, finalizer, has_deletion_timestamp |
| `kuberator.finalize` | Finalizer add/remove operations | resource_name, resource_namespace, finalizer |
| `kuberator.update_status` | Status updates | resource_name, resource_namespace, resource_generation |
| `kuberator.update_status_with_error` | Status updates with errors | resource_name, resource_namespace, resource_generation, has_error |
| `kuberator.error_policy` | Error handling and requeue decisions | resource_name, resource_namespace |

## Example: Full Trace Visibility

With kuberator v0.3, you get complete observability:

```
[AtlasCluster default/my-cluster gen:5]
├─ kuberator.reconcile (2.5s)
│  └─ kuberator.handle_reconciliation (2.5s)
│     ├─ kuberator.finalize (10ms) [finalizer=atlasclusters.mongodb.com/finalizer]
│     ├─ reconcile.apply (2.3s)
│     │  ├─ changes.detect (50ms)
│     │  ├─ atlas.get_cluster (800ms)
│     │  ├─ atlas.update_cluster (1.2s)
│     │  └─ atlas.get_advanced_configuration (200ms)
│     └─ kuberator.update_status (150ms)
```

This shows exactly where time is spent: framework operations vs. your business logic vs. external APIs.

## Advanced: OpenTelemetry Integration

For production observability, export traces to OpenTelemetry collectors:

```rust
use tracing_subscriber::prelude::*;
use tracing_opentelemetry::OpenTelemetryLayer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup OpenTelemetry
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    // Setup tracing subscriber with OpenTelemetry layer
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .with(OpenTelemetryLayer::new(tracer))
        .init();

    // Your operator code...

    // Graceful shutdown
    opentelemetry::global::shutdown_tracer_provider();
    Ok(())
}
```

Now all kuberator framework spans appear in Jaeger, Grafana Tempo, or any OTLP-compatible backend.

## Environment Variables

Control log/trace levels via environment variables:

```bash
# Set overall level
export RUST_LOG=info

# Fine-tune specific modules
export RUST_LOG=kuberator=debug,my_operator=info,atlas=trace

# Filter by span name
export RUST_LOG=kuberator::reconcile=trace
```

## Testing

If you have tests that check log output, update them:

**Before:**
```rust
// Testing with env_logger
env_logger::init();
```

**After:**
```rust
// Testing with tracing
tracing_subscriber::fmt()
    .with_test_writer()  // For test output
    .init();
```

## Troubleshooting

### "No logs appearing"

Make sure you initialize tracing **before** starting your reconciler:

```rust
tracing_subscriber::fmt().init();  // ← Must come first
reconciler.start(None).await;
```

### "Still seeing env_logger references"

Update your Cargo.toml:
```toml
[dev-dependencies]
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
# Remove: env_logger = "0.11"
```

### "My log::* calls don't work"

Add the bridge:
```rust
tracing_log::LogTracer::init()?;
```

## Need Help?

- Check the updated examples in `examples/`
- Review the [tracing documentation](https://docs.rs/tracing)
- See [kube-rs observability guide](https://kube.rs/controllers/observability/)
- Open an issue at https://github.com/commercetools/kuberator/issues

## Summary

**Minimal migration**: Add `tracing-log` bridge, initialize tracing subscriber
**Recommended migration**: Replace `log::*` with `tracing::*` for structured logging
**Full migration**: Add OpenTelemetry for production observability

The investment is small, the observability gains are **huge**.
