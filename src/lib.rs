// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! # lance-hdfs-provider
//!
//! HDFS object store provider for [Lance](https://lancedb.github.io/lance/),
//! backed by [OpenDAL](https://opendal.apache.org/).
//!
//! Dataset URIs use the form `hdfs://<name-node-or-nameservice>/<path>`:
//!
//! | `storage_options` key | Environment variable | OpenDAL option |
//! | --- | --- | --- |
//! | `hdfs_name_node` | `HDFS_NAME_NODE` | `name_node` |
//! | `hdfs_user` | `HADOOP_USER_NAME`, then `HDFS_USER` | `user` |
//! | `hdfs_kerberos_ticket_cache_path` | None | `kerberos_ticket_cache_path` |
//! | `hdfs_atomic_write_dir` | None | `atomic_write_dir` |

mod commit;

use std::collections::HashMap;
use std::sync::Arc;

use object_store_opendal::OpendalStore;
use opendal::{Operator, services::Hdfs};
use url::Url;

use lance_core::error::{Error, Result};
use lance_io::object_store::{
    DEFAULT_CLOUD_IO_PARALLELISM, ObjectStore,
    ObjectStoreParams, ObjectStoreProvider, StorageOptions,
};

pub use commit::RenameCommitHandler;

/// Block size used for cloud object stores (64KB).
const DEFAULT_CLOUD_BLOCK_SIZE: usize = 64 * 1024;

/// HDFS object store provider backed by OpenDAL.
///
/// When users register this provider, `hdfs://` URIs are routed to an HDFS-backed
/// object store built on OpenDAL. The constructed store delegates all object-store
/// operations to OpenDAL's HDFS service via [`OpendalStore`].
#[derive(Default, Debug, Clone)]
pub struct HdfsStoreProvider;

impl HdfsStoreProvider {
    fn operator_error(error: impl std::fmt::Display, name_node: &str, has_user: bool) -> Error {
        Error::io(format!(
            "Failed to create HDFS operator: {error}. name_node={name_node}, has_user={has_user}"
        ), snafu::location!())
    }

    fn build_config<I, K, V>(
        base_path: &Url,
        storage_options: &StorageOptions,
        env_vars: I,
    ) -> Result<HashMap<String, String>>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        base_path
            .host_str()
            .ok_or_else(|| Error::invalid_input("HDFS URI must contain namenode host", snafu::location!()))?;

        let env_vars = env_vars
            .into_iter()
            .filter_map(|(key, value)| {
                let value = value.into();
                if value.is_empty() {
                    None
                } else {
                    Some((key.as_ref().to_string(), value))
                }
            })
            .collect::<HashMap<_, _>>();

        let name_node = storage_options
            .0
            .get("hdfs_name_node")
            .filter(|v| !v.is_empty())
            .cloned()
            .or_else(|| env_vars.get("HDFS_NAME_NODE").cloned())
            .unwrap_or_else(|| format!("hdfs://{}", base_path.authority()));

        let mut config = HashMap::from([
            ("name_node".to_string(), name_node),
            ("root".to_string(), "/".to_string()),
            ("rename_overwrite".to_string(), "false".to_string()),
        ]);

        let user = storage_options
            .0
            .get("hdfs_user")
            .filter(|v| !v.is_empty())
            .cloned()
            .or_else(|| env_vars.get("HADOOP_USER_NAME").cloned())
            .or_else(|| env_vars.get("HDFS_USER").cloned());
        if let Some(user) = user {
            config.insert("user".to_string(), user);
        }

        for (storage_key, config_key) in [
            (
                "hdfs_kerberos_ticket_cache_path",
                "kerberos_ticket_cache_path",
            ),
            ("hdfs_atomic_write_dir", "atomic_write_dir"),
        ] {
            if let Some(value) = storage_options.0.get(storage_key).filter(|v| !v.is_empty()) {
                config.insert(config_key.to_string(), value.clone());
            }
        }

        Ok(config)
    }

    fn calculate_store_prefix(
        url: &Url,
        storage_options: Option<&HashMap<String, String>>,
        env_vars: &HashMap<String, String>,
    ) -> String {
        let authority = storage_options
            .and_then(|opts| opts.get("hdfs_name_node"))
            .filter(|v| !v.is_empty())
            .cloned()
            .or_else(|| env_vars.get("HDFS_NAME_NODE").cloned())
            .unwrap_or_else(|| url.authority().to_string());

        format!("{}${}", url.scheme(), authority)
    }
}

#[async_trait::async_trait]
impl ObjectStoreProvider for HdfsStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let block_size = params.block_size.unwrap_or(DEFAULT_CLOUD_BLOCK_SIZE);
        let storage_options = StorageOptions(params.storage_options().cloned().unwrap_or_default());
        let config = Self::build_config(&base_path, &storage_options, std::env::vars())?;

        let name_node = config
            .get("name_node")
            .cloned()
            .unwrap_or_else(|| "<missing>".to_string());
        let has_user = config.contains_key("user");
        let operator = Operator::from_iter::<Hdfs>(config)
            .map_err(|error| Self::operator_error(error, &name_node, has_user))?
            .finish();

        // Build the OpendalStore from the operator. The store prefixes are
        // handled by ObjectStore::new internally.
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(OpendalStore::new(operator));

        Ok(ObjectStore::new(
            store,
            base_path,
            Some(block_size),
            params.object_store_wrapper.clone(),
            params.use_constant_size_upload_parts,
            params.list_is_lexically_ordered.unwrap_or(false),
            DEFAULT_CLOUD_IO_PARALLELISM,
            storage_options.download_retry_count(),
            params.storage_options(),
        ))
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        let env_vars = std::env::vars().collect::<HashMap<String, String>>();
        Ok(Self::calculate_store_prefix(url, storage_options, &env_vars))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    

    #[test]
    fn test_hdfs_config_from_url() {
        let url = Url::parse("hdfs://namenode:9000/test").unwrap();
        let config = HdfsStoreProvider::build_config(
            &url,
            &StorageOptions::default(),
            Vec::<(&str, &str)>::new(),
        )
        .unwrap();

        assert_eq!(config.get("name_node").unwrap(), "hdfs://namenode:9000");
        assert_eq!(config.get("root").unwrap(), "/");
        assert_eq!(config.get("rename_overwrite").unwrap(), "false");
    }

    #[test]
    fn test_hdfs_storage_options_override_environment_and_url() {
        let url = Url::parse("hdfs://url-namenode:9000/test").unwrap();
        let storage_options = StorageOptions(HashMap::from([
            (
                "hdfs_name_node".to_string(),
                "hdfs://option-namenode:8020".to_string(),
            ),
            ("hdfs_user".to_string(), "option-user".to_string()),
            (
                "hdfs_kerberos_ticket_cache_path".to_string(),
                "/tmp/krb5cc".to_string(),
            ),
            (
                "hdfs_atomic_write_dir".to_string(),
                "/tmp/atomic".to_string(),
            ),
        ]));
        let env_vars = [
            ("HDFS_NAME_NODE", "hdfs://env-namenode:9000"),
            ("HADOOP_USER_NAME", "env-user"),
        ];

        let config = HdfsStoreProvider::build_config(&url, &storage_options, env_vars).unwrap();

        assert_eq!(
            config.get("name_node").unwrap(),
            "hdfs://option-namenode:8020"
        );
        assert_eq!(config.get("user").unwrap(), "option-user");
        assert_eq!(
            config.get("kerberos_ticket_cache_path").unwrap(),
            "/tmp/krb5cc"
        );
        assert_eq!(config.get("atomic_write_dir").unwrap(), "/tmp/atomic");
        assert_eq!(config.get("rename_overwrite").unwrap(), "false");
    }

    #[test]
    fn test_hdfs_config_from_environment() {
        let url = Url::parse("hdfs://url-namenode:9000/test").unwrap();
        let env_vars = [
            ("HDFS_NAME_NODE", "hdfs://env-namenode:9000"),
            ("HADOOP_USER_NAME", "env-user"),
        ];

        let config =
            HdfsStoreProvider::build_config(&url, &StorageOptions::default(), env_vars).unwrap();

        assert_eq!(config.get("name_node").unwrap(), "hdfs://env-namenode:9000");
        assert_eq!(config.get("user").unwrap(), "env-user");
    }

    #[test]
    fn test_hdfs_config_rejects_url_without_host() {
        let url = Url::parse("hdfs:///test").unwrap();
        let error = HdfsStoreProvider::build_config(
            &url,
            &StorageOptions::default(),
            Vec::<(&str, &str)>::new(),
        )
        .unwrap_err();

        assert!(matches!(error, Error::InvalidInput { .. }));
        assert!(error.to_string().contains("namenode host"));
    }

    #[test]
    fn test_hdfs_operator_error_includes_connection_context() {
        let error = HdfsStoreProvider::operator_error(
            std::io::Error::other("native client unavailable"),
            "hdfs://namenode:9000",
            true,
        );
        let message = error.to_string();

        assert!(matches!(error, Error::IO { .. }));
        assert!(message.contains("native client unavailable"));
        assert!(message.contains("name_node=hdfs://namenode:9000"));
        assert!(message.contains("has_user=true"));
    }
}
