use api::OrderId;
use netidx_derive::Pack;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Pack)]
pub struct OrderIdAllocator {
    pub seqid: Uuid,
    pub seqno: u64,
}

impl OrderIdAllocator {
    pub fn new() -> Self {
        Self { seqid: Uuid::new_v4(), seqno: 0 }
    }

    pub fn next_order_id(&mut self) -> OrderId {
        let seqno = self.seqno;
        self.seqno += 1;
        OrderId { seqid: self.seqid, seqno }
    }
}

impl Default for OrderIdAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AtomicOrderIdAllocator {
    pub seqid: Uuid,
    pub seqno: AtomicU64,
}

impl AtomicOrderIdAllocator {
    pub fn new() -> Self {
        Self { seqid: Uuid::new_v4(), seqno: AtomicU64::new(0) }
    }

    pub fn new_with_seqid(seqid: Uuid) -> Self {
        Self { seqid, seqno: AtomicU64::new(0) }
    }

    pub fn next_order_id(&self) -> OrderId {
        let seqno = self.seqno.fetch_add(1, Ordering::Relaxed);
        OrderId { seqid: self.seqid, seqno }
    }
}
