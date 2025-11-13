# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2025-11-13

### Added
- Comprehensive test suite with 93.55% code coverage
- 14 tests for error.rs module (improved from 0% to 93.99% coverage)
- Examples directory with three runnable examples:
  - `basic_operator.rs` - Minimal working operator
  - `with_status.rs` - Using ObserveGeneration pattern
  - `error_handling.rs` - Using WithStatusError pattern
- Caching strategy documentation in README.md
- Comprehensive documentation for `destruct()` method with Config customization examples
- CHANGELOG.md following Keep a Changelog format

### Changed
- Updated Cargo.toml description to "A Kubernetes operator framework in Rust"
- Clarified caching strategy performance characteristics in documentation
- Updated Adhoc strategy documentation to reflect that it pre-caches provided namespaces

### Fixed
- Error message typo in error.rs (cleanup error said "apply" instead of "clean up")

## [0.2.0] - 2025-03-01

### Added
- API provider abstraction with `ProvideApi` trait ([#13](https://github.com/commercetools/kuberator/pull/13))
- `StaticApiProvider` for pre-cached API instances
- `CachedApiProvider` for dynamic namespace discovery
- Flexible caching strategies: Strict, Adhoc, and Extendable ([#14](https://github.com/commercetools/kuberator/pull/14))
- Generic `K8sRepository` to eliminate boilerplate
- Comprehensive test suite with Given/When/Then pattern ([#15](https://github.com/commercetools/kuberator/pull/15))

### Changed
- **BREAKING**: Upgraded `kube` to v2.0 ([#12](https://github.com/commercetools/kuberator/pull/12))
- **BREAKING**: Upgraded `schemars` to v1.1
- **BREAKING**: `StaticApiProvider::new()` now requires `CachingStrategy` parameter
- Updated all examples and documentation for new API

### Migration Guide (v0.1.x → v0.2.0)

#### Kube v2.0 Upgrade
Update your dependencies:
```toml
kube = { version = "2.0", features = ["runtime", "derive"] }
k8s-openapi = { version = "0.26", features = ["latest"] }
schemars = "1.1"
```

#### StaticApiProvider Changes
Add a caching strategy to all `StaticApiProvider::new()` calls:
```rust
// Before
StaticApiProvider::new(client, vec!["default"])

// After - choose the appropriate strategy
StaticApiProvider::new(client, vec!["default"], CachingStrategy::Strict)
```

**Choosing a strategy:**
- `CachingStrategy::Strict` - Production (lock-free, errors on uncached namespaces)
- `CachingStrategy::Adhoc` - Dynamic environments (lock-free, no caching)
- `CachingStrategy::Extendable` - Runtime discovery (RwLock, lazy loading)

## [0.1.8] - 2025-02-15

### Added
- Graceful shutdown support for controllers ([#10](https://github.com/commercetools/kuberator/pull/10))
- `start()` and `start_concurrent()` now accept optional shutdown signal

## [0.1.7] - 2025-02-01

### Changed
- Various internal improvements and bug fixes

---

## Earlier Versions

Earlier versions were not documented in this changelog. For historical changes,
see the [commit history](https://github.com/commercetools/kuberator/commits/main).

---

[Unreleased]: https://github.com/commercetools/kuberator/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/commercetools/kuberator/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/commercetools/kuberator/compare/v0.1.8...v0.2.0
[0.1.8]: https://github.com/commercetools/kuberator/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/commercetools/kuberator/releases/tag/v0.1.7
