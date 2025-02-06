use api::symbology::MarketdataVenue;
use std::collections::BTreeMap;

#[cfg(feature = "grpc")]
pub mod l2_client;
pub mod level_book;
#[cfg(feature = "grpc")]
pub mod managed_l1_streams;
// #[cfg(feature = "grpc")]
// pub mod managed_l2_clients;

pub use level_book::{LevelBook, LevelLike};

#[derive(Debug)]
pub enum MarketdataSource<T> {
    Gateway(T),
    PerVenue(BTreeMap<MarketdataVenue, T>),
}

impl<T> MarketdataSource<T> {
    pub fn get(&self, venue: &MarketdataVenue) -> Option<&T> {
        match self {
            MarketdataSource::Gateway(t) => Some(t),
            MarketdataSource::PerVenue(map) => map.get(venue),
        }
    }
}
