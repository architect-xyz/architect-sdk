use super::{allocator::StaticBumpAllocator, hcstatic::Hcstatic, Venue};
use crate::hcstatic;
use anyhow::{anyhow, bail, Result};
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
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicUsize, Arc},
};

hcstatic!(Product, ProductInner, 512);

impl Product {
    /// Forward the new impl of the inner type as a convenience
    pub fn new(
        name: &str,
        kind: api::symbology::ProductKind,
    ) -> Result<api::symbology::Product> {
        api::symbology::Product::new(name, kind)
    }
}

impl From<Product> for api::symbology::Product {
    fn from(p: Product) -> api::symbology::Product {
        p.clone().into()
    }
}

/// Derivation of `api::symbology::Product` where ids are replaced with hcstatics.
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

impl TryFrom<api::symbology::Product> for ProductInner {
    type Error = anyhow::Error;

    fn try_from(p: api::symbology::Product) -> Result<ProductInner> {
        Ok(ProductInner { id: p.id, name: p.name, kind: ProductKind::try_from(p.kind)? })
    }
}

impl From<ProductInner> for api::symbology::Product {
    fn from(p: ProductInner) -> api::symbology::Product {
        api::symbology::Product { id: p.id, name: p.name, kind: p.kind.into() }
    }
}

/// Derivation of `api::symbology::ProductKind` where ids are replaced with hcstatics.
#[derive(Debug, Clone)]
pub enum ProductKind {
    Coin { token_info: BTreeMap<Venue, TokenInfo> },
    Fiat,
    Equity,
    Perpetual,
    Future { underlying: Product, multiplier: Decimal, expiration: DateTime<Utc> },
    Option { underlying: Product, multiplier: Decimal, expiration: DateTime<Utc> },
    Commodity,
    Energy,
    Metal,
    Index,
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
            ProductKind::Option { .. } => "Option",
            ProductKind::Commodity => "Commodity",
            ProductKind::Energy => "Energy",
            ProductKind::Metal => "Metal",
            ProductKind::Index => "Index",
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

impl TryFrom<api::symbology::ProductKind> for ProductKind {
    type Error = anyhow::Error;

    fn try_from(kind: api::symbology::ProductKind) -> Result<ProductKind> {
        Ok(match kind {
            api::symbology::ProductKind::Coin { token_info: ti } => {
                let mut token_info = BTreeMap::new();
                for (k, v) in ti {
                    let k =
                        Venue::get_by_id(&k).ok_or_else(|| anyhow!("no such venue"))?;
                    token_info.insert(k, v);
                }
                ProductKind::Coin { token_info }
            }
            api::symbology::ProductKind::Fiat => ProductKind::Fiat,
            api::symbology::ProductKind::Equity => ProductKind::Equity,
            api::symbology::ProductKind::Perpetual => ProductKind::Perpetual,
            api::symbology::ProductKind::Future {
                underlying,
                multiplier,
                expiration,
            } => ProductKind::Future {
                underlying: Product::get_by_id(&underlying)
                    .ok_or_else(|| anyhow!("no such underlying"))?,
                multiplier,
                expiration,
            },
            api::symbology::ProductKind::Option {
                underlying,
                multiplier,
                expiration,
            } => ProductKind::Option {
                underlying: Product::get_by_id(&underlying)
                    .ok_or_else(|| anyhow!("no such underlying"))?,
                multiplier,
                expiration,
            },
            api::symbology::ProductKind::Commodity => ProductKind::Commodity,
            api::symbology::ProductKind::Energy => ProductKind::Energy,
            api::symbology::ProductKind::Metal => ProductKind::Metal,
            api::symbology::ProductKind::Index => ProductKind::Index,
            api::symbology::ProductKind::Unknown => ProductKind::Unknown,
        })
    }
}

impl From<ProductKind> for api::symbology::ProductKind {
    fn from(kind: ProductKind) -> api::symbology::ProductKind {
        match kind {
            ProductKind::Coin { token_info } => {
                let mut ti = BTreeMap::new();
                for (k, v) in token_info {
                    ti.insert(k.id, v);
                }
                api::symbology::ProductKind::Coin { token_info: ti }
            }
            ProductKind::Fiat => api::symbology::ProductKind::Fiat,
            ProductKind::Equity => api::symbology::ProductKind::Equity,
            ProductKind::Perpetual => api::symbology::ProductKind::Perpetual,
            ProductKind::Future { underlying, multiplier, expiration } => {
                api::symbology::ProductKind::Future {
                    underlying: underlying.id,
                    multiplier,
                    expiration,
                }
            }
            ProductKind::Option { underlying, multiplier, expiration } => {
                api::symbology::ProductKind::Option {
                    underlying: underlying.id,
                    multiplier,
                    expiration,
                }
            }
            ProductKind::Commodity => api::symbology::ProductKind::Commodity,
            ProductKind::Energy => api::symbology::ProductKind::Energy,
            ProductKind::Metal => api::symbology::ProductKind::Metal,
            ProductKind::Index => api::symbology::ProductKind::Index,
            ProductKind::Unknown => api::symbology::ProductKind::Unknown,
        }
    }
}
