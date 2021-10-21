#![feature(once_cell)]

use spark::config::MasterUrl;
use spark::rdd::TypedRddExt;
use spark::{SparkContext, SparkResult, SparkSession};
use std::lazy::SyncLazy;
use std::sync::Arc;

fn new(master_url: MasterUrl) -> (SparkSession, Arc<SparkContext>) {
    let _ = tracing_subscriber::fmt::try_init();
    let session = SparkSession::builder().master_url(master_url).create();
    let scx = session.spark_context();
    (session, scx)
}

fn new_local() -> (SparkSession, Arc<SparkContext>) {
    new(MasterUrl::default())
}

fn new_distributed() -> (SparkSession, Arc<SparkContext>) {
    new(MasterUrl::Distributed {
        url: spark::config::DistributedUrl::Local { num_threads: num_cpus::get() },
    })
}

static DATA: SyncLazy<Vec<u32>> = SyncLazy::new(|| (0..1024000).collect());

#[tokio::test]
async fn it_works_local() -> SparkResult<()> {
    let (_spark, scx) = new_local();
    let rdd = scx.parallelize(&DATA);
    assert_eq!(rdd.collect().await?, *DATA);
    Ok(())
}

#[tokio::test]
async fn distributed_it_works() -> SparkResult<()> {
    let (_spark, scx) = new_distributed();
    let rdd = scx.parallelize(&DATA);
    assert_eq!(rdd.collect().await?, *DATA);
    Ok(())
}
