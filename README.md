<h1 align="center">
deltactl
</h1>

## Use-case

`deltactl` is just a CLI wrapped around common Deltalake operations exposed by
the Rust [`deltalake`] library. The goal is to provide a single binary capable
of inspecting and maintaining Deltalake tables.

* Create simple maintenance jobs for self-managed tables in object store;
  no need for a Spark runtime or Python environment.
* Observe and debug table states in an interrogative fashion.
* Develop and test table operations and patterns on the local filesystem.

## Installation

Compile the binary from source with `cargo`.

- From the Git repository:
  ```sh
  cargo install --locked --git https://github.com/lsjoeberg/deltactl
  ```
- From local source:
  ```sh
  git clone https://github.com/lsjoeberg/deltactl && cd deltactl
  cargo install --path .
  ```

### Object store support

The [`deltalake`] library uses the [`object_store`] crate to interact with
tables stored in remote object storage services. The `deltactl` CLI supports
Amazon S3 and Azure Blob Storage via features flags; these are _not_ enabled
by default.

To enable support for e.g. Azure storage, install with:

```sh
cargo install -F azure --locked --git https://github.com/lsjoeberg/deltactl
```

The `object_store` crate exposes a wide configuration surface via environment
variables. These parameters can also be specified as `--storage-options` via
the CLI. See the supported options for each object storage service in the
`object_store` crate documentation:

* [Azure](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html)
* [S3](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html)

Note that other storage providers are also supported if they emulate e.g.
AWS S3, such as [Scaleway]. See examples below for how to configure emulated
object store services.

## Example usage

### Local filesystem

Deltalake tables can live entirely on a regular filesystem.

```sh
# Inspect the last five entries in the commit log.
deltactl history -n5 --oneline --uri './data/my_table'
```

### Azure Blob Storage

> [!NOTE]
> Requires feature flag `azure`.

Authenticate with Azure CLI:

```sh
deltactl details \
  -o'use_azure_cli=true' \
  --uri 'abfss://<container>@<account>.dfs.core.windows.net/<table_name>'
```

### Amazon S3

> [!NOTE]
> Requires feature flag `s3`.

Connect to a Scaleway S3 bucket:

1. Set environment variables:
   ```dotenv
   # .env
   export AWS_ACCESS_KEY_ID="***"
   export AWS_SECRET_ACCESS_KEY="***"
   export AWS_REGION="nl-ams"
   export AWS_ENDPOINT_URL="https://s3.nl-ams.scw.cloud"
   ```

2. Load storage options and interact with a table:
   ```sh
   source ./.env
   deltactl details --uri 's3://<bucket>/<table_name>'
   ```

<!-- References -->

[`deltalake`]: https://crates.io/crates/deltalake
[`object_store`]: https://crates.io/crates/object_store
[Scaleway]: https://www.scaleway.com/en/docs/object-storage/api-cli/using-api-call-list/
