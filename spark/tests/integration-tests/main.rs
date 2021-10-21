#![feature(once_cell)]

use spark::config::MasterUrl;
use spark::rdd::TypedRddExt;
use spark::{SparkContext, SparkResult, SparkSession};
use std::lazy::SyncLazy;
use std::sync::Arc;

static DATA: SyncLazy<Vec<u32>> = SyncLazy::new(|| (0..1024000).collect());

use tracing_flame::FlameLayer;
use tracing_subscriber::{fmt, prelude::*};

#[allow(unused)]
fn setup_global_subscriber() {
    let fmt_layer = fmt::Layer::default();
    let (flame_layer, _guard) = FlameLayer::with_file("./tracing.folded").unwrap();
    let _ = tracing_subscriber::registry().with(fmt_layer).with(flame_layer).try_init();
}

fn new(master_url: MasterUrl) -> (SparkSession, Arc<SparkContext>) {
    let _ = tracing_subscriber::fmt::try_init();
    // setup_global_subscriber();
    let session = SparkSession::builder().master_url(master_url).create();
    let scx = session.scx();
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
