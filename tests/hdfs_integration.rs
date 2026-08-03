// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Integration tests for HDFS object store provider
//!
//! These tests need an existing HDFS cluster and are ignored by default.
//!
//! Set `HDFS_NAME_NODE` to the name node or nameservice address
//! (e.g., `hdfs://namenode:9000` or `hdfs://mycluster`) when it should
//! override the authority in the test URI.
//!
//! Run:
//!   HDFS_NAME_NODE=hdfs://localhost:9000 cargo test --test hdfs_integration -- --ignored

use std::collections::HashMap;
use std::sync::Arc;

use lance_hdfs_provider::HdfsStoreProvider;
use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor,
};
use object_store::path::Path;

fn name_node() -> String {
    std::env::var("HDFS_NAME_NODE").unwrap_or_else(|_| "hdfs://localhost:9000".to_string())
}

fn uri(path: &str) -> String {
    format!(
        "{}/{}",
        name_node().trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

#[ignore = "Requires HDFS cluster"]
#[tokio::test]
async fn test_hdfs_store_creation() {
    let registry = Arc::new(ObjectStoreRegistry::default());
    registry.insert("hdfs", Arc::new(HdfsStoreProvider));
    let params = ObjectStoreParams::default();

    let (store, path) = ObjectStore::from_uri_and_params(registry, &uri("test/path"), &params)
        .await
        .unwrap();

    assert_eq!(store.scheme(), "hdfs");
    assert_eq!(path, Path::from("test/path"));
}

#[ignore = "Requires HDFS cluster"]
#[tokio::test]
async fn test_hdfs_store_with_custom_config() {
    let mut storage_options = HashMap::new();
    storage_options.insert("hdfs_user".to_string(), "testuser".to_string());
    storage_options.insert("hdfs_name_node".to_string(), name_node());

    let params = ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
            storage_options,
        ))),
        ..Default::default()
    };

    let registry = Arc::new(ObjectStoreRegistry::default());
    registry.insert("hdfs", Arc::new(HdfsStoreProvider));
    let (store, path) =
        ObjectStore::from_uri_and_params(registry, "hdfs://placeholder/user/test", &params)
            .await
            .unwrap();

    assert_eq!(store.scheme(), "hdfs");
    assert_eq!(path, Path::from("user/test"));
}

#[ignore = "Requires HDFS cluster"]
#[tokio::test]
async fn test_hdfs_basic_operations() {
    let params = ObjectStoreParams::default();
    let registry = Arc::new(ObjectStoreRegistry::default());
    registry.insert("hdfs", Arc::new(HdfsStoreProvider));
    let (store, _) = ObjectStore::from_uri_and_params(registry, &uri("basic-operations"), &params)
        .await
        .unwrap();

    let path = Path::from("test_file.txt");
    if store.inner.head(&path).await.is_ok() {
        store.inner.delete(&path).await.unwrap();
    }

    let test_data = bytes::Bytes::from("Hello, HDFS!");
    store
        .inner
        .put(&path, test_data.clone().into())
        .await
        .unwrap();
    let read_data = store.inner.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(read_data, test_data);
    store.inner.delete(&path).await.unwrap();
}

#[ignore = "Requires HDFS HA cluster configuration"]
#[tokio::test]
async fn test_hdfs_ha_configuration() {
    let registry = Arc::new(ObjectStoreRegistry::default());
    registry.insert("hdfs", Arc::new(HdfsStoreProvider));
    let params = ObjectStoreParams::default();

    let (store, path) =
        ObjectStore::from_uri_and_params(registry, "hdfs://mycluster/user/test", &params)
            .await
            .unwrap();

    assert_eq!(store.scheme(), "hdfs");
    assert_eq!(path, Path::from("user/test"));
}
