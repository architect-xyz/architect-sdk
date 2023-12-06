//! A global allocator for static data.

use portable_atomic::Ordering;
use smallvec::SmallVec;
use std::{mem::MaybeUninit, sync::atomic::AtomicUsize};

pub struct StaticBumpAllocator<T: Clone + 'static, const SZ: usize> {
    data: &'static mut [MaybeUninit<T>],
    pos: usize,
    total: &'static AtomicUsize, // CR alee: does this work
    prev: Option<&'static Self>,
}

unsafe impl<T: Clone + 'static, const SZ: usize> Send for StaticBumpAllocator<T, SZ> {}
unsafe impl<T: Clone + 'static, const SZ: usize> Sync for StaticBumpAllocator<T, SZ> {}

impl<T: Clone + 'static, const SZ: usize> StaticBumpAllocator<T, SZ> {
    pub fn new_data() -> &'static mut [MaybeUninit<T>] {
        let mut data = Vec::with_capacity(SZ);
        data.extend((0..SZ).into_iter().map(|_| MaybeUninit::uninit()));
        data.leak()
    }

    pub fn new(total: &'static AtomicUsize) -> Self {
        Self { data: Self::new_data(), pos: 0, total, prev: None }
    }

    pub fn insert(&mut self, t: T) -> &'static T {
        if self.pos >= self.data.len() {
            let prev = std::mem::replace(
                self,
                Self { data: Vec::new().leak(), pos: 0, total: self.total, prev: None },
            );
            *self = Self {
                data: Self::new_data(),
                pos: 0,
                total: prev.total,
                prev: Some(Box::leak(Box::new(prev))),
            };
        }
        self.data[self.pos] = MaybeUninit::new(t);
        let t = unsafe { &*self.data[self.pos].as_ptr() };
        self.pos += 1;
        t
    }

    /// call on_new for every T added since previous snapshot, and
    /// return a new snapshot
    pub fn snapshot<F: FnMut(&'static T)>(
        &self,
        previous: Option<AllocatorSnapshot<T>>,
        mut on_new: F,
    ) -> AllocatorSnapshot<T> {
        let mut cur = self;
        let mut snaps = SmallVec::<[&Self; 128]>::new();
        let mut pos = previous.map_or(0, |p| p.pos);
        // populate snaps with all roots from current to [previous]
        loop {
            snaps.push(cur);
            if let Some(p) = previous {
                if cur.data as *const _ == p.data {
                    break;
                }
            }
            match cur.prev {
                None => break,
                Some(p) => {
                    cur = p;
                }
            }
        }
        for snap in snaps.iter().rev() {
            for t in &snap.data[pos..snap.pos] {
                on_new(unsafe { &*t.as_ptr() })
            }
            pos = 0;
        }
        AllocatorSnapshot {
            data: self.data as *const _,
            pos: self.pos,
            total: self.total.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot cursor into the allocator, used for tracking new objects incrementally.
#[derive(Debug, Clone)]
pub struct AllocatorSnapshot<T: 'static> {
    pub data: *const [MaybeUninit<T>],
    pub pos: usize,
    pub total: usize,
}

impl<T: Clone> Copy for AllocatorSnapshot<T> {}
unsafe impl<T> Send for AllocatorSnapshot<T> {}
unsafe impl<T> Sync for AllocatorSnapshot<T> {}
