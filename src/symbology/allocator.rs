//! A global allocator for static data.  Intended to emulate a global symbol table
//! in Rust.  Reading is free, writing should be done through [Txn] which provides
//! a mutex, as StaticBumpAllocator::insert isn't thread-safe.

use std::{
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
};

pub struct StaticBumpAllocator<T: Clone + 'static, const SZ: usize> {
    data: &'static mut [MaybeUninit<T>],
    pos: usize,
    total: &'static AtomicUsize,
    #[allow(unused)]
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
        self.total.fetch_add(1, Ordering::Relaxed);
        t
    }
}
