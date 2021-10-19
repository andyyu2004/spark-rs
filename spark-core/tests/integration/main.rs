use spark_core::rdd::TypedRddExt;
use spark_core::{SparkContext, SparkResult, SparkSession};
use std::sync::Arc;

fn new() -> (SparkSession, Arc<SparkContext>) {
    tracing_subscriber::fmt::init();
    let session = SparkSession::builder().create();
    let scx = session.spark_context();
    (session, scx)
}

#[tokio::test]
async fn it_works() -> SparkResult<()> {
    let (_spark, scx) = new();
    let rdd = scx.parallelize(&[3]);
    rdd.collect().await?;
    Ok(())
}
