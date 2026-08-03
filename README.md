# lance-hdfs-provider

HDFS store provider for Lance built on top of OpenDAL's `services::Hdfs` backend. It lets Lance and LanceDB read and write datasets directly to Hadoop HDFS through `hdfs://` URIs.

## Installation

Add the crate in your `Cargo.toml`:

```toml
[dependencies]
lance-hdfs-provider = "0.2.0"
```

## Quickstart: Lance dataset

Register the provider, then read or write using HDFS URIs:

```rust,no_run
use std::sync::Arc;
use lance::{io::ObjectStoreRegistry, session::Session,
    dataset::{DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE}
};
use lance::dataset::builder::DatasetBuilder;
use lance_hdfs_provider::HdfsStoreProvider;

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = ObjectStoreRegistry::default();
    registry.insert("hdfs", Arc::new(HdfsStoreProvider));

    let session = Arc::new(Session::new(
        DEFAULT_INDEX_CACHE_SIZE,
        DEFAULT_METADATA_CACHE_SIZE,
        Arc::new(registry),
    ));

    let uri = "hdfs://127.0.0.1:9000/sample-dataset";

    // Load an existing dataset
    let _dataset = DatasetBuilder::from_uri(uri)
        .with_session(session.clone())
        .load()
        .await?;

    // Or write a new dataset (see examples)
    Ok(())
# }
```

## Quickstart: LanceDB

Use the same registry when creating the LanceDB session:

```rust,no_run
use std::sync::Arc;
use lance::{io::ObjectStoreRegistry, session::Session,
    dataset::{DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE}
};
use lance_hdfs_provider::HdfsStoreProvider;

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = ObjectStoreRegistry::default();
    registry.insert("hdfs", Arc::new(HdfsStoreProvider));

    let session = Arc::new(Session::new(
        DEFAULT_INDEX_CACHE_SIZE,
        DEFAULT_METADATA_CACHE_SIZE,
        Arc::new(registry),
    ));

    let db = lancedb::connect("hdfs://127.0.0.1:9000/test-db")
        .session(session.clone())
        .execute()
        .await?;

    let table = db.open_table("table1").execute().await?;
    Ok(())
# }
```

## Notes

- Ensure your HDFS URI includes the NameNode. It can be a server with host and port (e.g. `hdfs://127.0.0.1:9000/path`), or a named cluster.
- Explicit Lance `StorageOptions` take priority over environment variables. If neither specifies a NameNode, the URI authority is used.
- This crate uses OpenDAL's Java/libhdfs-backed HDFS service. Building and running it requires a Java and Hadoop native environment.

## Configuration

Supported Lance `StorageOptions`:

| Key | Environment variable | Description |
| --- | --- | --- |
| `hdfs_name_node` | `HDFS_NAME_NODE` | NameNode URI or HA nameservice. Defaults to the `hdfs://` URI authority. |
| `hdfs_user` | `HADOOP_USER_NAME`, then `HDFS_USER` | HDFS user name. |
| `hdfs_kerberos_ticket_cache_path` | None | Kerberos ticket cache path, typically from `klist` after `kinit`. |
| `hdfs_atomic_write_dir` | None | HDFS directory OpenDAL can use for atomic writes. |

Example:

```rust,no_run
use std::collections::HashMap;
use std::sync::Arc;

use lance::io::{ObjectStoreParams, StorageOptionsAccessor};

let storage_options = HashMap::from([
    ("hdfs_name_node".to_string(), "hdfs://namenode:8020".to_string()),
    ("hdfs_user".to_string(), "lance".to_string()),
    (
        "hdfs_kerberos_ticket_cache_path".to_string(),
        "/tmp/krb5cc_lance".to_string(),
    ),
]);

let params = ObjectStoreParams {
    storage_options_accessor: Some(Arc::new(
        StorageOptionsAccessor::with_static_options(storage_options),
    )),
    ..Default::default()
};
```

## Runtime Environment

OpenDAL's HDFS service depends on `hdrs` and libhdfs. In most environments you should set:

```bash
export JAVA_HOME=/path/to/java
export HADOOP_HOME=/path/to/hadoop
export CLASSPATH="$(${HADOOP_HOME}/bin/hadoop classpath --glob)"
export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${HADOOP_HOME}/lib/native:${LD_LIBRARY_PATH}"
```

For HA clusters, make the Hadoop configuration directory available:

```bash
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export CLASSPATH="${HADOOP_CONF_DIR}:${CLASSPATH}"
```

## Licenses

Licensed under either of

- Apache License, Version 2.0
  ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license
  ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
