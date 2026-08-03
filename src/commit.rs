// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Safe commit handler for HDFS-backed Lance datasets.
//!
//! HDFS does not support conditional put operations natively. The
//! [`RenameCommitHandler`] provided here uses a staging file plus atomic
//! `rename_if_not_exists` to safely commit manifests without risking
//! conflicting concurrent writes.
//!
//! # Usage
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use lance_hdfs_provider::RenameCommitHandler;
//! use lance::dataset::CommitBuilder;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let commit_handler = Arc::new(RenameCommitHandler);
//! let dataset = /* ... */;
//! CommitBuilder::new(dataset)
//!     .with_commit_handler(commit_handler)
//!     .execute(transaction)
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::fmt::Debug;

use lance_core::{Error, Result};
use lance_io::object_store::ObjectStore;
use lance_table::format::{IndexMetadata, Manifest, Transaction};
use lance_table::io::commit::{CommitError, CommitHandler, ManifestLocation, ManifestNamingScheme};
use object_store::Error as ObjectStoreError;
use object_store::path::Path;

/// A commit implementation that uses a temporary path and renames the object.
///
/// This only works for object stores that support atomic rename if not exists,
/// which includes HDFS (via OpenDAL's `rename` with `rename_overwrite=false`).
pub struct RenameCommitHandler;

impl Debug for RenameCommitHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RenameCommitHandler").finish()
    }
}

/// Create a staging path for a manifest by appending a UUID to the target path.
fn make_staging_manifest_path(base: &Path) -> Result<Path> {
    let id = uuid::Uuid::new_v4().to_string();
    Path::parse(format!("{base}-{id}")).map_err(|e| Error::io(format!("{e}"), snafu::location!()))
}

#[async_trait::async_trait]
impl CommitHandler for RenameCommitHandler {
    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: lance_table::io::commit::ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        // Create a temporary object, then use `rename_if_not_exists` to commit.
        // If failed, clean up the temporary object.

        let path = naming_scheme.manifest_path(base_path, manifest.version);
        let tmp_path = make_staging_manifest_path(&path)?;

        let res = manifest_writer(object_store, manifest, indices, &tmp_path, transaction).await?;

        match object_store
            .inner
            .rename_if_not_exists(&tmp_path, &path)
            .await
        {
            Ok(_) => {
                // Successfully committed
                Ok(ManifestLocation {
                    version: manifest.version,
                    path,
                    size: Some(res.size as u64),
                    naming_scheme,
                    e_tag: None, // Rename can change e-tag.
                })
            }
            Err(ObjectStoreError::AlreadyExists { .. }) => {
                // Another transaction has already been committed
                // Attempt to clean up temporary object, but ignore errors if we can't
                let _ = object_store.delete(&tmp_path).await;

                Err(CommitError::CommitConflict)
            }
            Err(e) => Err(CommitError::OtherError(e.into())),
        }
    }
}
