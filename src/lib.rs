// pub mod client;
#[cfg(feature = "graphql")]
pub mod graphql;
#[cfg(feature = "grpc")]
pub mod grpc;
pub mod marketdata;
#[cfg(feature = "grpc")]
pub mod symbology;
pub mod synced;

// #[cfg(feature = "grpc")]
// pub use client::ArchitectClient;
