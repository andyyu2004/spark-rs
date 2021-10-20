use spark::rdd::TypedRddExt;
use spark::{SparkContext, SparkResult, SparkSession};
use std::sync::Arc;

fn new() -> (SparkSession, Arc<SparkContext>) {
    let _ = tracing_subscriber::fmt::try_init();
    let session = SparkSession::builder().create();
    let scx = session.spark_context();
    (session, scx)
}

#[tokio::test]
async fn it_works() -> SparkResult<()> {
    let (_spark, scx) = new();
    let rdd = scx.parallelize(&[1, 2, 3, 4]);
    assert_eq!(rdd.collect().await?, vec![1, 2, 3, 4]);
    Ok(())
}

#[tokio::test]
async fn distributed_it_works() -> SparkResult<()> {
    let (_spark, scx) = new();
    let rdd = scx.parallelize(&[1, 2, 3, 4]);
    assert_eq!(rdd.collect().await?, vec![1, 2, 3, 4]);
    Ok(())
}
