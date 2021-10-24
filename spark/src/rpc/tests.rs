use super::*;
use crate::executor::ExecutorId;
use indexed_vec::Idx;

#[tokio::test]
async fn test_simple_rpc_call() -> SparkResult<()> {
    let spark = SparkSession::builder().create().await?;
    let scx = spark.scx();
    let env = scx.env();

    let client = create_client(scx.bind_addr()).await.unwrap();
    for i in 1..1000 {
        assert_eq!(client.alloc_executor_id(tarpc::context::current()).await?, ExecutorId::new(i));
    }
    Ok(())
}
