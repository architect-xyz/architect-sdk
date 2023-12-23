use super::{allocator::StaticBumpAllocator, static_ref::StaticRef, Venue};
use crate::static_ref;
use anyhow::{bail, Result};
use api::{
    symbology::{
        product::{ProductId, TokenInfo},
        Symbolic,
    },
    Str,
};
use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use immutable_chunkmap::map::MapL as Map;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use rust_decimal::Decimal;
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicUsize, Arc},
};

static_ref!(Product, ProductInner, 512);

impl Product {
    /// forward the inner constructor as a convenience
    pub fn new(name: &str, kind: ProductKind) -> Result<api::symbology::Product> {
        api::symbology::Product::new(name, (&kind).into())
    }
}

impl Serialize for Product {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.name.serialize(serializer)
    }
}

/// Derivation of `api::symbology::Product` where ids are replaced with StaticRef's.
#[derive(Debug, Clone)]
pub struct ProductInner {
    pub id: ProductId,
    pub name: Str,
    pub kind: ProductKind,
}

impl Symbolic for ProductInner {
    type Id = ProductId;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn name(&self) -> Str {
        self.name
    }
}

impl From<&ProductInner> for api::symbology::Product {
    fn from(p: &ProductInner) -> api::symbology::Product {
        api::symbology::Product { id: p.id, name: p.name, kind: (&p.kind).into() }
    }
}

/// Derivation of `api::symbology::ProductKind` where ids are replaced with StaticRef's.
#[derive(Debug, Clone)]
pub enum ProductKind {
    Coin {
        token_info: BTreeMap<Venue, TokenInfo>,
    },
    Fiat,
    Equity,
    Perpetual,
    Future {
        underlying: Option<Product>,
        multiplier: Option<Decimal>,
        expiration: Option<DateTime<Utc>>,
    },
    FutureSpread {
        same_side_leg: Option<Product>,
        opp_side_leg: Option<Product>,
    },
    Option {
        underlying: Option<Product>,
        multiplier: Option<Decimal>,
        expiration: Option<DateTime<Utc>>,
    },
    Index,
    Commodity,
    Unknown,
}

// CR alee: sad reimpl, just use strum or something
impl ProductKind {
    pub fn name(&self) -> &'static str {
        match self {
            ProductKind::Coin { .. } => "Coin",
            ProductKind::Fiat => "Fiat",
            ProductKind::Equity => "Equity",
            ProductKind::Perpetual => "Perpetual",
            ProductKind::Future { .. } => "Future",
            ProductKind::FutureSpread { .. } => "FutureSpread",
            ProductKind::Option { .. } => "Option",
            ProductKind::Index => "Index",
            ProductKind::Commodity => "Commodity",
            ProductKind::Unknown => "Unknown",
        }
    }

    // /// return the option direction, or None if the product isn't an
    // /// option.
    // pub fn option_dir(&self) -> Option<OptionDir> {
    //     let class = self.0.class.load();
    //     match &**class {
    //         ProductClassInner::Option { .. } => {
    //             let name = self.0.name.as_str();
    //             name.split_once(" ")
    //                 .and_then(|(_, strike)| strike.chars().next())
    //                 .and_then(|c| match c {
    //                     'P' => Some(OptionDir::Put),
    //                     'C' => Some(OptionDir::Call),
    //                     _ => None,
    //                 })
    //         }
    //         ProductClassInner::Future { .. }
    //         | ProductClassInner::Coin { token_info: _ }
    //         | ProductClassInner::Commodity
    //         | ProductClassInner::Energy
    //         | ProductClassInner::Equity
    //         | ProductClassInner::Fiat
    //         | ProductClassInner::Metal
    //         | ProductClassInner::Index
    //         | ProductClassInner::Unknown => None,
    //     }
    // }

    // /// return the strike price of the option, or none if the product
    // /// isn't an option
    // pub fn option_strike(&self) -> Option<Decimal> {
    //     let class = self.0.class.load();
    //     match &**class {
    //         ProductClassInner::Option { .. } => {
    //             let name = self.0.name.as_str();
    //             name.split_once(" ").and_then(|(_, strike)| {
    //                 strike.trim_start_matches(&['P', 'C']).parse::<Decimal>().ok()
    //             })
    //         }
    //         ProductClassInner::Future { .. }
    //         | ProductClassInner::Coin { token_info: _ }
    //         | ProductClassInner::Commodity
    //         | ProductClassInner::Energy
    //         | ProductClassInner::Equity
    //         | ProductClassInner::Fiat
    //         | ProductClassInner::Metal
    //         | ProductClassInner::Index
    //         | ProductClassInner::Unknown => None,
    //     }
    // }

    // /// return the underlying product if any
    // pub fn underlying(&self) -> Option<Product> {
    //     let class = self.0.class.load();
    //     match &**class {
    //         ProductClassInner::Option { underlying, .. }
    //         | ProductClassInner::Future { underlying, .. } => Some(*underlying),
    //         ProductClassInner::Coin { token_info: _ }
    //         | ProductClassInner::Commodity
    //         | ProductClassInner::Energy
    //         | ProductClassInner::Equity
    //         | ProductClassInner::Fiat
    //         | ProductClassInner::Metal
    //         | ProductClassInner::Index
    //         | ProductClassInner::Unknown => None,
    //     }
    // }
}

impl From<&ProductKind> for api::symbology::ProductKind {
    fn from(kind: &ProductKind) -> api::symbology::ProductKind {
        match kind {
            ProductKind::Coin { token_info } => {
                let mut ti = BTreeMap::new();
                for (k, v) in token_info {
                    ti.insert(k.id, v.clone());
                }
                api::symbology::ProductKind::Coin { token_info: ti }
            }
            ProductKind::Fiat => api::symbology::ProductKind::Fiat,
            ProductKind::Equity => api::symbology::ProductKind::Equity,
            ProductKind::Perpetual => api::symbology::ProductKind::Perpetual,
            ProductKind::Future { underlying, multiplier, expiration } => {
                api::symbology::ProductKind::Future {
                    underlying: underlying.map(|u| u.id),
                    multiplier: *multiplier,
                    expiration: *expiration,
                }
            }
            ProductKind::FutureSpread { same_side_leg, opp_side_leg } => {
                api::symbology::ProductKind::FutureSpread {
                    same_side_leg: same_side_leg.map(|p| p.id),
                    opp_side_leg: opp_side_leg.map(|p| p.id),
                }
            }
            ProductKind::Option { underlying, multiplier, expiration } => {
                api::symbology::ProductKind::Option {
                    underlying: underlying.map(|u| u.id),
                    multiplier: *multiplier,
                    expiration: *expiration,
                }
            }
            ProductKind::Index => api::symbology::ProductKind::Index,
            ProductKind::Commodity => api::symbology::ProductKind::Commodity,
            ProductKind::Unknown => api::symbology::ProductKind::Unknown,
        }
    }
}
