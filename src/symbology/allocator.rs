//! A global allocator for static data.

use std::mem::MaybeUninit;

pub struct StaticBumpAllocator<T: 'static, const SZ: usize> {
    data: &'static mut [MaybeUninit<T>],
    pos: usize,
}

unsafe impl<T: 'static, const SZ: usize> Send for StaticBumpAllocator<T, SZ> {}
unsafe impl<T: 'static, const SZ: usize> Sync for StaticBumpAllocator<T, SZ> {}

impl<T: 'static, const SZ: usize> StaticBumpAllocator<T, SZ> {
    pub fn new_data() -> &'static mut [MaybeUninit<T>] {
        let mut data = Vec::with_capacity(SZ);
        data.extend((0..SZ).into_iter().map(|_| MaybeUninit::uninit()));
        data.leak()
    }

    pub fn new() -> Self {
        Self { data: Self::new_data(), pos: 0 }
    }

    pub fn insert(&mut self, t: T) -> &'static T {
        if self.pos >= self.data.len() {
            self.pos = 0;
            self.data = Self::new_data();
        }
        self.data[self.pos] = MaybeUninit::new(t);
        let t = unsafe { &*self.data[self.pos].as_ptr() };
        self.pos += 1;
        t
    }
}
