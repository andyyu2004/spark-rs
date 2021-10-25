use crate::*;
use dashmap::mapref::entry::Entry;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use indexed_vec::Idx;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::OnceCell;

newtype_index!(BroadcastId);

#[derive(Serialize, Deserialize)]
pub struct Broadcast<T> {
    #[serde(skip)]
    scx: Weak<SparkContext>,
    #[serde(skip)]
    datum: OnceCell<T>,
    broadcast_id: BroadcastId,
}

#[derive(Default)]
pub struct BroadcastContext {
    broadcast_idx: AtomicUsize,
    cached: DashMap<BroadcastId, Vec<u8>>,
}

impl BroadcastContext {
    pub fn new() -> Self {
        Self { broadcast_idx: Default::default(), cached: Default::default() }
    }

    pub fn new_broadcast<T: Datum>(&self, scx: &Arc<SparkContext>, datum: T) -> Broadcast<T> {
        let broadcast_id = self.next_broadcast_id();
        let serialized = bincode::serialize(&datum).unwrap();
        self.cached.insert(broadcast_id, serialized);
        Broadcast { broadcast_id, scx: Arc::downgrade(scx), datum: OnceCell::from(datum) }
    }
}

impl BroadcastContext {
    /// Return a slice of bytes representing the serialized form of the broadcasted item
    pub async fn get_broadcasted_bytes(
        &self,
        id: BroadcastId,
    ) -> SparkResult<Ref<'_, BroadcastId, Vec<u8>>> {
        match self.cached.entry(id) {
            Entry::Occupied(entry) => Ok(entry.into_ref().downgrade()),
            Entry::Vacant(entry) => {
                let env = SparkEnv::get();
                assert!(!env.is_driver(), "driver should have all broadcasts in cache");
                let client = SparkEnv::get_driver_rpc_client().await?;
                let item = client.get_broadcasted_item(tarpc::context::current(), id).await??;
                Ok(entry.insert(item).downgrade())
            }
        }
    }

    fn next_broadcast_id(&self) -> BroadcastId {
        BroadcastId::new(self.broadcast_idx.fetch_add(1, Ordering::SeqCst))
    }
}

impl<T: Datum> Broadcast<T> {
    pub fn new(scx: &Arc<SparkContext>, datum: T) -> Self {
        scx.broadcast_context().new_broadcast(scx, datum)
    }

    #[instrument(skip(self))]
    pub async fn get(&self) -> SparkResult<&T> {
        trace!("broadcast get");
        self.datum
            .get_or_try_init(|| async move {
                trace!("broadcast data not present");
                assert!(
                    self.scx.upgrade().is_none(),
                    "datum should not be None if we are still on the driver"
                );
                let bcx = SparkEnv::get_broadcast_context();
                let bytes = bcx.get_broadcasted_bytes(self.broadcast_id).await?;
                let value = bincode::deserialize(&bytes).expect("failed to deserialize broadcast");
                Ok(value)
            })
            .await
    }
}
