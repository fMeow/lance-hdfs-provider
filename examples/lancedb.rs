use arrow::{
    array::{RecordBatch, UInt32Array},
    datatypes::{DataType, Field, Schema},
};
use futures::StreamExt;
use lance::{
    dataset::{DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE},
    io::ObjectStoreRegistry,
    session::Session,
};
use lance_hdfs_provider::HdfsStoreProvider;
use lancedb::query::{ExecutableQuery, QueryBase};

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

    let db = lancedb::connect("hdfs://127.0.0.1:9000/test-db")
        .session(session.clone())
        .execute()
        .await?;
    let table = db.open_table("table1").execute().await?;

    {
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

        let batches = arrow::array::RecordBatchIterator::new([Ok(batch)], schema.clone());
        table.add(batches);
    }
    let batches = table
        .query()
        .limit(100)
        .execute()
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(batches.len(), 1);
    let batch = batches.first().unwrap();
    assert_eq!(batch.num_rows(), 6);

    Ok(())
}
