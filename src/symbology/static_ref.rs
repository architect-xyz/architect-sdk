//! Static references to symbology types, wrapped as newtypes.
//!
//! These are used to wrap symbology types from the symbology API, in order to achieve
//! zero-copy performance for users of this reference client.  Instead of passing around
//! the full API types, which can be large, users of this client can pass around these
//! static reference wrappers.
//!
//! Newtypes created by `static_ref!` shadow the names of their wrapped/inner types, and
//! the provided Deref impl should make their usage in either context transparent to the
//! programmer.
//!
//! Newtypes created by `static_ref!` implement the StaticRef trait, which provides useful
//! functionality for working with symbology.  The macro will also create the necessary
//! process-global memory pools.

use super::allocator::{AllocatorSnapshot, StaticBumpAllocator};
use anyhow::Result;
use api::{symbology::Symbolic, Str};
use arc_swap::ArcSwap;
use immutable_chunkmap::map::MapL as Map;
use parking_lot::Mutex;
use portable_atomic::Ordering;
use std::{
    ops::Deref,
    str::FromStr,
    sync::{atomic::AtomicUsize, Arc},
};

pub trait StaticRef<T: Symbolic, const SLAB_SIZE: usize>:
    Clone + Copy + Deref<Target = T> + 'static
{
    /// Returns a reference to the global pool of T's
    fn allocator() -> &'static Mutex<StaticBumpAllocator<T, SLAB_SIZE>>;

    fn allocator_counter() -> &'static AtomicUsize;

    /// Returns a snapshot of the current global pool of T's
    fn allocator_snapshot<F: FnMut(Self)>(
        prev: Option<AllocatorSnapshot<T>>,
        mut new: F,
    ) -> AllocatorSnapshot<T> {
        match prev {
            None => {
                Self::allocator().lock().snapshot(prev, |i| new(Self::from_pointee(i)))
            }
            Some(p) => {
                if p.total < Self::allocator_counter().load(Ordering::Relaxed) {
                    Self::allocator()
                        .lock()
                        .snapshot(Some(p), |i| new(Self::from_pointee(i)))
                } else {
                    p
                }
            }
        }
    }

    /// Returns a reference to the global map of T's by-name
    fn by_name() -> &'static ArcSwap<Map<Str, Self>>;

    /// Returns a reference to the global map of T's by-id
    fn by_id() -> &'static ArcSwap<Map<T::Id, Self>>;

    /// Direct constructor
    fn from_pointee(pointee: &'static T) -> Self;

    fn insert(
        by_name: &mut Map<Str, Self>,
        by_id: &mut Map<T::Id, Self>,
        inner: T,
        validate: bool,
    ) -> Result<Self> {
        if validate {
            inner.validate()?;
        }
        let inner = Self::allocator().lock().insert(inner);
        let t = Self::from_pointee(inner);
        by_name.insert_cow(inner.name(), t);
        by_id.insert_cow(inner.id(), t);
        Ok(t)
    }

    fn remove(self, by_name: &mut Map<Str, Self>, by_id: &mut Map<T::Id, Self>) {
        by_name.remove_cow(&self.name());
        by_id.remove_cow(&self.id());
    }

    /// Look up a symbol by name.
    /// This is O(log(N)) where N is the total number of symbols in the set.
    fn get(s: &str) -> Option<Self> {
        Self::by_name().load().get(s).copied()
    }

    /// Look up a symbol by id.
    /// This is O(log(N)) where N is the total number of symbols in the set.
    fn get_by_id(id: &T::Id) -> Option<Self> {
        Self::by_id().load().get(id).copied()
    }

    /// Look up a symbol by name or id.
    fn get_by_name_or_id(s: &str) -> Option<Self> {
        Self::get(s).or_else(|| {
            let id = T::Id::from_str(s).ok()?;
            Self::get_by_id(&id)
        })
    }

    /// Get a map of all symbols indexed by name. This is O(1)
    fn all() -> Arc<Map<Str, Self>> {
        Self::by_name().load_full()
    }

    /// Get a map of all symbols indexed by id. This is O(1)
    fn all_by_id() -> Arc<Map<T::Id, Self>> {
        Self::by_id().load_full()
    }
}

#[macro_export]
macro_rules! static_ref {
    ($name:ident, $inner:ty, $slab_size:literal) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name(&'static $inner);

        paste::paste! {
            pub(crate) static [<$name:snake:upper _BY_NAME>]: Lazy<ArcSwap<Map<Str, $name>>> =
                Lazy::new(|| ArcSwap::new(Arc::new(Map::new())));
            pub(crate) static [<$name:snake:upper _BY_ID>]: Lazy<ArcSwap<Map<<$inner as Symbolic>::Id, $name>>> =
                Lazy::new(|| ArcSwap::new(Arc::new(Map::new())));
            pub(crate) static [<$name:snake:upper _COUNT>]: Lazy<AtomicUsize> =
                Lazy::new(|| AtomicUsize::new(0));
            pub(crate) static [<$name:snake:upper _POOL>]: Lazy<Mutex<StaticBumpAllocator<$inner, $slab_size>>> =
                Lazy::new(|| Mutex::new(StaticBumpAllocator::new(&[<$name:snake:upper _COUNT>])));

            impl StaticRef<$inner, $slab_size> for $name {
                fn allocator() -> &'static Mutex<StaticBumpAllocator<$inner, $slab_size>> {
                    &[<$name:snake:upper _POOL>]
                }

                fn allocator_counter() -> &'static AtomicUsize {
                    &[<$name:snake:upper _COUNT>]
                }

                fn by_name() -> &'static ArcSwap<Map<Str, Self>> {
                    &[<$name:snake:upper _BY_NAME>]
                }

                fn by_id() -> &'static ArcSwap<Map<<$inner as Symbolic>::Id, Self>> {
                    &[<$name:snake:upper _BY_ID>]
                }

                fn from_pointee(pointee: &'static $inner) -> Self {
                    Self(pointee)
                }
            }
        }

        impl std::ops::Deref for $name {
            type Target = $inner;

            fn deref(&self) -> &Self::Target {
                self.0
            }
        }

        impl std::str::FromStr for $name {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
                match Self::get(s) {
                    Some(t) => Ok(t),
                    None => bail!("no such {} {}", stringify!($name), s),
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(
                &self,
                f: &mut std::fmt::Formatter<'_>,
            ) -> std::result::Result<(), std::fmt::Error> {
                write!(f, "{}", self.name)
            }
        }

        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                self.0.name == other.0.name
            }
        }

        impl Eq for $name {}

        impl std::hash::Hash for $name {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.0.name.hash(state)
            }
        }

        impl PartialOrd for $name {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                self.0.name.partial_cmp(&other.0.name)
            }
        }

        impl Ord for $name {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.name.cmp(&other.0.name)
            }
        }
    };
}
