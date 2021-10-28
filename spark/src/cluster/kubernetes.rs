use super::*;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::serde_json::json;
use kube::api::PostParams;
use kube::Api;

pub struct KubeClusterScheduler {
    job_api: Api<Job>,
}

impl KubeClusterScheduler {
    pub async fn new() -> SparkResult<Self> {
        let client = kube::Client::try_default().await?;
        let job_api = Api::<Job>::default_namespaced(client);
        Ok(Self { job_api })
    }

    pub fn create_executor(&self) -> SparkResult<()> {
        // self.job_api.create(&PostParams::default(), &json! {});
        Ok(())
    }
}

impl ClusterSchedulerBackend for KubeClusterScheduler {
}
