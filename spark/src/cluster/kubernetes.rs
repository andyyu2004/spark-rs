use super::*;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::serde_json::json;
use kube::api::{Api, Patch, PatchParams};
use url::Url;

pub struct KubeClusterScheduler {
    job_api: Api<Job>,
}

impl KubeClusterScheduler {
    pub async fn new(url: &Url) -> SparkResult<Arc<Self>> {
        let kube_config = kube::Config::new(url.to_string().parse().unwrap());
        let client = kube::Client::try_from(kube_config)?;
        let job_api = Api::<Job>::default_namespaced(client);
        let patch = json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": "spark-master"
            },
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                        {
                            "name":"test",
                            "image":"arch-linux"
                        }
                        ],
                        "restartPolicy": "Never"
                    }
                },
                "backoffLimit": 4
            }
        });
        let master_job =
            job_api.patch("name", &PatchParams::default(), &Patch::Apply(patch)).await?;
        Ok(Arc::new(Self { job_api }))
    }

    pub async fn create_executor(&self) -> SparkResult<()> {
        let patch = json!({
            "apiVersion":"batch/v1",
            "kind":"Job",
            "metadata":{
                "name":"spark-executor"
            },
            "spec":{
                "template":{
                    "spec":{
                        "containers":[
                        {
                            "name":"test",
                            "image":"arch-linux"
                        }
                        ],
                        "restartPolicy":"Never"
                    }
                },
                "backoffLimit":4
            }
        });
        let job = self.job_api.patch("name", &PatchParams::default(), &Patch::Apply(patch)).await?;
        dbg!(job);
        // self.job_api.create(&PostParams::default(), &json! {});
        Ok(())
    }
}

impl ClusterSchedulerBackend for KubeClusterScheduler {
}
