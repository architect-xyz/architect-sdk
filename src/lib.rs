#[cfg(feature = "netidx")]
pub mod account_manager;
#[cfg(feature = "netidx")]
pub mod admin_stats;
#[cfg(feature = "netidx")]
pub mod channel_driver;
pub mod client;
#[cfg(feature = "netidx")]
pub mod common;
pub mod external_driver;
pub mod marketdata;
#[cfg(feature = "netidx")]
pub mod orderflow;
#[cfg(feature = "netidx")]
pub mod paths;
pub mod symbology;
pub mod synced;
#[cfg(feature = "netidx")]
pub mod tls;

#[cfg(feature = "grpc")]
pub use client::ArchitectClient;
#[cfg(feature = "netidx")]
pub use {
    channel_driver::{ChannelDriver, ChannelDriverBuilder},
    common::Common,
    marketdata::managed_marketdata::ManagedMarketdata,
    orderflow::order_id_allocator::{AtomicOrderIdAllocator, OrderIdAllocator},
    paths::Paths,
};
