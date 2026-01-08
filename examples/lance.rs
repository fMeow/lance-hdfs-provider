use std::sync::Arc;

use arrow::{
    array::UInt32Array,
    datatypes::{DataType, Field, Schema},
    record_batch::{RecordBatch, RecordBatchIterator},
};
use futures::StreamExt;
use lance::{
    Dataset,
    dataset::{
        AutoCleanupParams, DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE, WriteMode,
        WriteParams, builder::DatasetBuilder,
    },
    io::ObjectStoreRegistry,
    session::Session,
};
use lance_hdfs_provider::HdfsStoreProvider;
use lancedb::connection::LanceFileVersion;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;
    let registry = ObjectStoreRegistry::default();
    registry.insert("hdfs", Arc::new(HdfsStoreProvider));

    let session = Arc::new(Session::new(
        DEFAULT_INDEX_CACHE_SIZE,
        DEFAULT_METADATA_CACHE_SIZE,
        Arc::new(registry),
    ));
    write_dataset(
        session.clone(),
        "hdfs://hdfs://127.0.0.1:9000/sample-dataset",
    )
    .await?;

    let _record_batchs = read_dataset(
        session.clone(),
        "hdfs://hdfs://127.0.0.1:9000/sample-dataset",
    )
    .await?;

    Ok(())
}

// Writes sample dataset to the given path
async fn write_dataset(
    session: Arc<Session>,
    data_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Define new schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt32, false),
        Field::new("value", DataType::UInt32, false),
    ]));

    // Create new record batches
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(UInt32Array::from(vec![6, 7, 8, 9, 10, 11])),
        ],
    )?;

    let batches = RecordBatchIterator::new([Ok(batch)], schema.clone());

    // Define write parameters (e.g. overwrite dataset)
    let write_params = WriteParams {
        mode: WriteMode::Overwrite,
        session: Some(session),
        data_storage_version: Some(LanceFileVersion::V2_1),
        enable_v2_manifest_paths: true,
        auto_cleanup: Some(AutoCleanupParams::default()),
        ..Default::default()
    };

    Dataset::write(batches, data_path, Some(write_params)).await?;
    Ok(())
}

// Reads dataset from the given path and prints batch size, schema for all record batches. Also extracts and prints a slice from the first batch
async fn read_dataset(
    session: Arc<Session>,
    data_path: &str,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let dataset = DatasetBuilder::from_uri(data_path)
        .with_session(session)
        .load()
        .await?;
    let scanner = dataset.scan();

    let batches = scanner
        .try_into_stream()
        .await?
        .map(|b| b.unwrap())
        .collect::<Vec<_>>()
        .await;
    Ok(batches)
}
