#![feature(once_cell)]

mod random;

use indexed_vec::Idx;
use serde_closure::Fn;
use spark::config::{ClusterUrl, MasterUrl};
use spark::rdd::{TypedRdd, TypedRddExt, TypedRddRef};
use spark::scheduler::{JobId, ResultTask, StageId, TaskId, TaskMeta};
use spark::{PartitionIdx, SparkContext, SparkIteratorRef, SparkResult, SparkSession, TaskContext};
use std::lazy::SyncLazy;
use std::sync::Arc;

// static DATA: SyncLazy<Vec<u32>> = SyncLazy::new(|| (0..30_240_000).collect());
static DATA: SyncLazy<Vec<u32>> = SyncLazy::new(|| (0..30).collect());

use tracing_flame::FlameLayer;
use tracing_subscriber::{fmt, prelude::*};

#[allow(unused)]
fn setup_global_subscriber() {
    let fmt_layer = fmt::Layer::default();
    let (flame_layer, _guard) = FlameLayer::with_file("./tracing.folded").unwrap();
    let _ = tracing_subscriber::registry().with(fmt_layer).with(flame_layer).try_init();
}

async fn new(master_url: MasterUrl) -> SparkResult<(SparkSession, Arc<SparkContext>)> {
    let _ = tracing_subscriber::fmt::try_init();
    // setup_global_subscriber();
    let session = SparkSession::builder().master_url(master_url).create().await?;
    let scx = session.scx();
    Ok((session, scx))
}

async fn new_local() -> SparkResult<(SparkSession, Arc<SparkContext>)> {
    new(MasterUrl::default()).await
}

async fn new_distributed() -> SparkResult<(SparkSession, Arc<SparkContext>)> {
    new(MasterUrl::Cluster { url: ClusterUrl::Standalone { num_threads: num_cpus::get() } }).await
}

#[tokio::test]
async fn it_works_local() -> SparkResult<()> {
    let (_spark, scx) = new_local().await?;
    let rdd = scx.parallelize(&DATA);
    assert_eq!(rdd.collect().await?, *DATA);
    Ok(())
}

#[tokio::test]
async fn test_map_local() -> SparkResult<()> {
    let (_spark, scx) = new_local().await?;
    let rdd = scx.parallelize(&DATA);
    assert_eq!(
        rdd.map(Fn!(|x| x * 2)).collect().await?,
        *DATA.iter().map(|&x| x * 2).collect::<Vec<u32>>()
    );
    Ok(())
}

#[tokio::test]
async fn test_filter_local() -> SparkResult<()> {
    let (_spark, scx) = new_local().await?;
    let rdd = scx.parallelize(&DATA);
    assert_eq!(
        rdd.filter(Fn!(|&x| x % 2 == 0)).collect().await?,
        *DATA.iter().copied().filter(|x| x % 2 == 0).collect::<Vec<u32>>()
    );
    Ok(())
}

#[tokio::test]
async fn distributed_it_works() -> SparkResult<()> {
    let (_spark, scx) = new_distributed().await?;
    let rdd = scx.parallelize(&DATA);
    assert_eq!(rdd.collect().await?, *DATA);
    Ok(())
}
