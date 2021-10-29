#![feature(once_cell)]

use criterion::{criterion_group, criterion_main, Criterion};
use futures::executor::block_on;
use indexed_vec::Idx;
use serde_closure::Fn;
use spark::config::MasterUrl;
use spark::rdd::{TypedRdd, TypedRddExt};
use spark::scheduler::{JobId, ResultTask, StageId, TaskId, TaskMeta};
use spark::{PartitionIdx, SparkIteratorRef, SparkSession, TaskContext};
use std::lazy::SyncLazy;
use std::sync::Arc;

static DATA: SyncLazy<Vec<u32>> = SyncLazy::new(|| (0..10240).collect());

criterion_main!(serialization, computation);

criterion_group!(computation, bench_identity_computation);
criterion_group!(serialization, bench_serialization);

pub fn bench_serialization(c: &mut Criterion) {
    c.bench_function("serialize task", move |b| {
        let spark =
            block_on(SparkSession::builder().master_url(MasterUrl::default()).create()).unwrap();

        let rdd = spark.scx().parallelize(&DATA);

        let f = Fn!(|_cx: &mut TaskContext, iter: SparkIteratorRef<_>| iter.collect::<Vec<_>>());
        let task = ResultTask::new(
            TaskMeta::new(TaskId::new(0), StageId::new(0), PartitionIdx::new(0), JobId::new(0)),
            rdd.as_typed_ref(),
            Arc::new(f),
        );
        b.iter(move || bincode::serialize(&task));
    });
}

pub fn bench_identity_computation(c: &mut Criterion) {
    c.bench_function("distributed identity 1M", move |b| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let spark = block_on(
            SparkSession::builder()
                .master_url(MasterUrl::Cluster {
                    url: spark::config::ClusterUrl::Standalone { num_threads: num_cpus::get() },
                })
                .create(),
        )
        .unwrap();
        b.to_async(&runtime).iter(move || spark.scx().parallelize(&DATA).collect())
    });

    c.bench_function("local identity 1M", move |b| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let spark =
            block_on(SparkSession::builder().master_url(MasterUrl::default()).create()).unwrap();
        b.to_async(&runtime).iter(move || spark.scx().parallelize(&DATA).collect())
    });
}
