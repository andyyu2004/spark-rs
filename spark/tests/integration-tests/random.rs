//! scratch area for random things

use super::*;

// #[tokio::test]
// async fn serialize_vec() -> SparkResult<()> {
//     let (_spark, _scx) = new_local();
//     bincode::serialize(&*DATA).unwrap();
//     Ok(())
// }

// #[tokio::test]
// async fn serialize_rdd() -> SparkResult<()> {
//     let (_spark, scx) = new_local();
//     let rdd = TypedRddRef::from_inner(scx.parallelize(&DATA));
//     bincode::serialize(&rdd).unwrap();
//     Ok(())
// }

#[tokio::test]
async fn serialize_task() -> SparkResult<()> {
    let (_spark, scx) = new_local().await?;
    let rdd = TypedRddRef::from_inner(scx.parallelize(&DATA));
    let meta = TaskMeta::new(TaskId::new(0), StageId::new(0), PartitionIdx::new(0), JobId::new(0));
    let mapper = Arc::new(Fn!(
        |_cx: &mut TaskContext, iter: SparkIteratorRef<u32>| iter.collect::<Vec<_>>()
    ));
    let result_task = ResultTask::new(meta, rdd, mapper);
    let mut buf = vec![];
    bincode::serialize_into(&mut buf, &result_task).unwrap();
    Ok(())
}
