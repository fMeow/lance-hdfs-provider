# lance-hdfs-provider

HDFS store provider for Lance built on top of the OpenDAL `hdfs` service. It lets Lance and LanceDB read and write datasets directly to Hadoop HDFS.

## Installation

Add the crate in your `Cargo.toml`:

```toml
[dependencies]
lance-hdfs-provider = "0.1.0"
```

## Quickstart: Lance dataset

Register the provider, then read or write using HDFS URIs:

```rust
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

```rust
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

- Ensure your HDFS URI includes the NameNode host and port (e.g. `hdfs://127.0.0.1:9000/path`).
- Authentication and additional options can be passed via Lance `StorageOptions`; any key supported by OpenDAL's HDFS service can be provided.

## License

MIT
