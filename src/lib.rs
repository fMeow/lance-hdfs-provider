#[doc = include_str!("../README.md")]
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::future::IntoFuture;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use lance_core::Error;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore as OSObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use object_store_opendal::OpendalStore;
use opendal::raw::percent_decode_path;
use opendal::{Operator, services::Hdfs};
use send_wrapper::SendWrapper;
use snafu::location;
use url::Url;

use lance_io::object_store::{
    DEFAULT_CLOUD_IO_PARALLELISM, ObjectStore, ObjectStoreParams, ObjectStoreProvider,
    StorageOptions,
};

/// HDFS Object Store Provider for Lance.
///
/// # Example
///
/// ## With lance
/// ```rust,no_run
/// # use std::sync::Arc;
/// # use lance::{io::ObjectStoreRegistry, session::Session,
/// #     dataset::{DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE}}
/// # ;
/// # use lance_hdfs_provider::HdfsStoreProvider;
/// # use lance::dataset::builder::DatasetBuilder;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut registry = ObjectStoreRegistry::default();
/// registry.insert("hdfs", Arc::new(HdfsStoreProvider));
///
/// let session = Arc::new(Session::new(
///     DEFAULT_INDEX_CACHE_SIZE,
///     DEFAULT_METADATA_CACHE_SIZE,
///     Arc::new(registry),
/// ));
///
/// let uri = "hdfs://127.0.0.1:9000/sample-dataset";
/// let _ds = DatasetBuilder::from_uri(uri).with_session(session).load().await?;
/// # Ok(())
/// # }
/// ```
/// ## With lancedb
/// ```rust,no_run
/// # use std::sync::Arc;
/// # use lance::{io::ObjectStoreRegistry, session::Session,
/// #     dataset::{DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE}
/// # };
/// # use lance_hdfs_provider::HdfsStoreProvider;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let mut registry = ObjectStoreRegistry::default();
///     registry.insert("hdfs", Arc::new(HdfsStoreProvider));
///
///     let session = Arc::new(Session::new(
///         DEFAULT_INDEX_CACHE_SIZE,
///         DEFAULT_METADATA_CACHE_SIZE,
///         Arc::new(registry),
///     ));
///
///     let db = lancedb::connect("hdfs://127.0.0.1:9000/test-db")
///         .session(session.clone())
///         .execute()
///         .await?;
///
///     let table = db.open_table("table1").execute().await?;
///     Ok(())
/// # }
/// ```
#[derive(Debug, Default, Clone)]
pub struct HdfsStoreProvider;

impl HdfsStoreProvider {
    fn operator_error(error: impl std::fmt::Display, name_node: &str, has_user: bool) -> Error {
        Error::io(
            format!(
                "Failed to create HDFS operator: {error}. name_node={name_node}, has_user={has_user}"
            ),
            location!(),
        )
    }

    fn build_config<I, K, V>(
        base_path: &Url,
        storage_options: &StorageOptions,
        env_vars: I,
    ) -> Result<HashMap<String, String>, Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        base_path.host_str().ok_or_else(|| {
            Error::invalid_input("HDFS URI must contain namenode host", location!())
        })?;

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
            .filter(|value| !value.is_empty())
            .cloned()
            .or_else(|| env_vars.get("HDFS_NAME_NODE").cloned())
            .unwrap_or_else(|| format!("hdfs://{}", base_path.authority()));

        let mut config = HashMap::from([
            ("name_node".to_string(), name_node),
            ("root".to_string(), "/".to_string()),
        ]);

        let user = storage_options
            .0
            .get("hdfs_user")
            .filter(|value| !value.is_empty())
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
            if let Some(value) = storage_options
                .0
                .get(storage_key)
                .filter(|value| !value.is_empty())
            {
                config.insert(config_key.to_string(), value.clone());
            }
        }

        Ok(config)
    }

    fn calculate_object_store_prefix_with_env(
        url: &Url,
        storage_options: Option<&HashMap<String, String>>,
        env_vars: &HashMap<String, String>,
    ) -> Result<String, Error> {
        let authority = storage_options
            .and_then(|options| options.get("hdfs_name_node"))
            .filter(|value| !value.is_empty())
            .cloned()
            .or_else(|| env_vars.get("HDFS_NAME_NODE").cloned())
            .unwrap_or_else(|| url.authority().to_string());

        Ok(format!("{}${}", url.scheme(), authority))
    }
}

struct HdfsObjectStore {
    inner: OpendalStore,
    operator: Operator,
}

impl HdfsObjectStore {
    fn new(operator: Operator) -> Self {
        Self {
            inner: OpendalStore::new(operator.clone()),
            operator,
        }
    }

    fn format_opendal_error(err: opendal::Error, path: &Path) -> object_store::Error {
        match err.kind() {
            opendal::ErrorKind::NotFound => object_store::Error::NotFound {
                path: path.to_string(),
                source: Box::new(err),
            },
            opendal::ErrorKind::AlreadyExists => object_store::Error::AlreadyExists {
                path: path.to_string(),
                source: Box::new(err),
            },
            opendal::ErrorKind::Unsupported => object_store::Error::NotSupported {
                source: Box::new(err),
            },
            opendal::ErrorKind::ConditionNotMatch => object_store::Error::Precondition {
                path: path.to_string(),
                source: Box::new(err),
            },
            kind => object_store::Error::Generic {
                store: kind.into_static(),
                source: Box::new(err),
            },
        }
    }
}

impl Debug for HdfsObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsObjectStore")
            .field("inner", &self.inner)
            .finish()
    }
}

impl Display for HdfsObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

#[async_trait::async_trait]
impl OSObjectStore for HdfsObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, bytes, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        SendWrapper::new(
            self.operator
                .rename(
                    &percent_decode_path(from.as_ref()),
                    &percent_decode_path(to.as_ref()),
                )
                .into_future(),
        )
        .await
        .map_err(|err| Self::format_opendal_error(err, to))
    }
}

#[async_trait::async_trait]
impl ObjectStoreProvider for HdfsStoreProvider {
    async fn new_store(
        &self,
        base_path: Url,
        params: &ObjectStoreParams,
    ) -> Result<ObjectStore, lance_core::Error> {
        let storage_options = StorageOptions(params.storage_options().cloned().unwrap_or_default());
        let config = Self::build_config(&base_path, &storage_options, std::env::vars())?;

        let name_node = config
            .get("name_node")
            .cloned()
            .unwrap_or_else(|| "<missing>".to_string());
        let has_user = config.contains_key("user");
        let download_retry_count = storage_options.download_retry_count();

        let operator = Operator::from_iter::<Hdfs>(config)
            .map_err(|error| Self::operator_error(error, &name_node, has_user))?
            .finish();

        let opendal_store = Arc::new(HdfsObjectStore::new(operator));
        Ok(ObjectStore::new(
            opendal_store,
            base_path,
            params.block_size,
            params.object_store_wrapper.clone(),
            params.use_constant_size_upload_parts,
            params.list_is_lexically_ordered.unwrap_or(false),
            DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count,
            params.storage_options(),
        ))
    }

    fn extract_path(&self, url: &Url) -> Result<Path, Error> {
        Path::parse(url.path()).map_err(|e| {
            Error::invalid_input(
                format!("Failed to parse path '{}': {}", url.path(), e),
                location!(),
            )
        })
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String, Error> {
        let env_vars = std::env::vars().collect::<HashMap<String, String>>();
        Self::calculate_object_store_prefix_with_env(url, storage_options, &env_vars)
    }
}
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use url::Url;

    use super::*;

    #[test]
    fn test_hdfs_store_paths() {
        let provider = HdfsStoreProvider;
        let cases = [
            ("hdfs://namenode:9000/path/to/file", "path/to/file"),
            ("hdfs://namenode/path/to/file", "path/to/file"),
            ("hdfs://namenode:9000/", ""),
            (
                "hdfs://namenode:9000/user/data/dataset/file.parquet",
                "user/data/dataset/file.parquet",
            ),
            ("hdfs://ht-hdfsqa/user/data/file.txt", "user/data/file.txt"),
        ];

        for (url, expected_path) in cases {
            let path = provider.extract_path(&Url::parse(url).unwrap()).unwrap();
            assert_eq!(path, Path::from(expected_path));
        }
    }

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
    fn test_hdfs_config_falls_back_to_hdfs_user_environment() {
        let url = Url::parse("hdfs://url-namenode:9000/test").unwrap();
        let env_vars = [("HDFS_USER", "fallback-user")];

        let config =
            HdfsStoreProvider::build_config(&url, &StorageOptions::default(), env_vars).unwrap();

        assert_eq!(config.get("user").unwrap(), "fallback-user");
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

    #[test]
    fn test_hdfs_object_store_prefix_uses_effective_name_node() {
        let url = Url::parse("hdfs://url-namenode:9000/test").unwrap();
        let storage_options = HashMap::from([(
            "hdfs_name_node".to_string(),
            "hdfs://option-namenode:8020".to_string(),
        )]);
        let env_vars = HashMap::from([(
            "HDFS_NAME_NODE".to_string(),
            "hdfs://env-namenode:9000".to_string(),
        )]);

        let prefix = HdfsStoreProvider::calculate_object_store_prefix_with_env(
            &url,
            Some(&storage_options),
            &env_vars,
        )
        .unwrap();

        assert_eq!(prefix, "hdfs$hdfs://option-namenode:8020");
    }

    #[test]
    fn test_hdfs_object_store_prefix_uses_environment_before_url() {
        let url = Url::parse("hdfs://url-namenode:9000/test").unwrap();
        let env_vars = HashMap::from([(
            "HDFS_NAME_NODE".to_string(),
            "hdfs://env-namenode:9000".to_string(),
        )]);

        let prefix =
            HdfsStoreProvider::calculate_object_store_prefix_with_env(&url, None, &env_vars)
                .unwrap();

        assert_eq!(prefix, "hdfs$hdfs://env-namenode:9000");
    }
}
