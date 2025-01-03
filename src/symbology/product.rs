use super::{allocator::StaticBumpAllocator, static_ref::StaticRef, VenueRef};
use crate::static_ref;
use anyhow::{bail, Result};
use api::{
    symbology::{
        product::{InstrumentType, ProductId, TokenInfo},
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
use rust_decimal_macros::dec;
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicUsize, Arc},
};

static_ref!(ProductRef, ProductInner, 512);

impl ProductRef {
    /// forward the inner constructor as a convenience
    pub fn new(name: &str, kind: ProductKind) -> Result<api::symbology::Product> {
        api::symbology::Product::new(name, (&kind).into())
    }
}

impl From<&ProductRef> for api::symbology::Product {
    fn from(p: &ProductRef) -> api::symbology::Product {
        api::symbology::Product { id: p.id, name: p.name, kind: (&p.kind).into() }
    }
}

/// Derivation of `api::symbology::Product` where ids are replaced with StaticRef's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductInner {
    pub id: ProductId,
    pub name: Str,
    pub kind: ProductKind,
}

impl ProductInner {
    pub fn iter_references(&self, mut f: impl FnMut(ProductRef)) {
        match &self.kind {
            ProductKind::Coin { .. }
            | ProductKind::Fiat
            | ProductKind::Equity
            | ProductKind::Index
            | ProductKind::Commodity
            | ProductKind::Unknown => {}
            ProductKind::Perpetual { underlying, .. }
            | ProductKind::Future { underlying, .. }
            | ProductKind::Option { underlying, .. } => {
                underlying.map(|p| f(p));
            }
            ProductKind::FutureSpread { same_side_leg, opp_side_leg } => {
                same_side_leg.map(|p| f(p));
                opp_side_leg.map(|p| f(p));
            }
            ProductKind::EventSeries { .. } => {}
            ProductKind::Event { series, outcomes, .. } => {
                series.map(|p| f(p));
                for p in outcomes.iter() {
                    f(*p);
                }
            }
            ProductKind::EventOutcome { contracts, .. } => match contracts {
                EventContracts::Single { yes, .. } => f(*yes),
                EventContracts::Dual { yes, no, .. } => {
                    f(*yes);
                    f(*no);
                }
            },
            ProductKind::EventContract { underlying, .. } => {
                underlying.map(|p| f(p));
            }
        }
    }
}

impl Symbolic for ProductInner {
    type Id = ProductId;

    fn type_name() -> &'static str {
        "product"
    }

    fn id(&self) -> Self::Id {
        self.id
    }

    fn name(&self) -> Str {
        self.name
    }
}

/// Derivation of `api::symbology::ProductKind` where ids are replaced with StaticRef's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ProductKind {
    Coin {
        token_info: BTreeMap<VenueRef, TokenInfo>,
    },
    Fiat,
    Equity,
    Perpetual {
        underlying: Option<ProductRef>,
        multiplier: Option<Decimal>,
        instrument_type: Option<InstrumentType>,
    },
    Future {
        underlying: Option<ProductRef>,
        multiplier: Option<Decimal>,
        expiration: Option<DateTime<Utc>>,
        instrument_type: Option<InstrumentType>,
    },
    FutureSpread {
        same_side_leg: Option<ProductRef>,
        opp_side_leg: Option<ProductRef>,
    },
    Option {
        underlying: Option<ProductRef>,
        multiplier: Option<Decimal>,
        expiration: Option<DateTime<Utc>>,
        instrument_type: Option<InstrumentType>,
    },
    Index,
    Commodity,
    EventSeries {
        display_name: Option<String>,
    },
    Event {
        series: Option<ProductRef>,
        outcomes: Vec<ProductRef>,
        mutually_exclusive: Option<bool>,
        expiration: Option<DateTime<Utc>>,
        display_category: Option<String>,
        display_name: Option<String>,
    },
    EventOutcome {
        contracts: EventContracts,
        display_order: Option<u32>,
        display_name: Option<String>,
    },
    EventContract {
        underlying: Option<ProductRef>,
        expiration: Option<DateTime<Utc>>,
    },
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum EventContracts {
    Single {
        yes: ProductRef,
        yes_alias: Option<Str>,
    },
    Dual {
        yes: ProductRef,
        yes_alias: Option<Str>,
        no: ProductRef,
        no_alias: Option<Str>,
    },
}

impl EventContracts {
    pub fn iter_contracts(&self, mut f: impl FnMut(ProductRef)) {
        match self {
            EventContracts::Single { yes, .. } => f(*yes),
            EventContracts::Dual { yes, no, .. } => {
                f(*yes);
                f(*no);
            }
        }
    }

    pub fn yes(&self) -> ProductRef {
        match self {
            EventContracts::Single { yes, .. } => *yes,
            EventContracts::Dual { yes, .. } => *yes,
        }
    }

    pub fn no(&self) -> Option<ProductRef> {
        match self {
            EventContracts::Single { .. } => None,
            EventContracts::Dual { no, .. } => Some(*no),
        }
    }
}

impl From<EventContracts> for api::symbology::EventContracts {
    fn from(value: EventContracts) -> api::symbology::EventContracts {
        match value {
            EventContracts::Single { yes, yes_alias } => {
                api::symbology::EventContracts::Single { yes: yes.id, yes_alias }
            }
            EventContracts::Dual { yes, yes_alias, no, no_alias } => {
                api::symbology::EventContracts::Dual {
                    yes: yes.id,
                    yes_alias,
                    no: no.id,
                    no_alias,
                }
            }
        }
    }
}

// CR alee: sad reimpl, just use strum or something
impl ProductKind {
    pub fn name(&self) -> &'static str {
        match self {
            ProductKind::Coin { .. } => "Coin",
            ProductKind::Fiat => "Fiat",
            ProductKind::Equity => "Equity",
            ProductKind::Perpetual { .. } => "Perpetual",
            ProductKind::Future { .. } => "Future",
            ProductKind::FutureSpread { .. } => "FutureSpread",
            ProductKind::Option { .. } => "Option",
            ProductKind::Index => "Index",
            ProductKind::Commodity => "Commodity",
            ProductKind::EventSeries { .. } => "EventSeries",
            ProductKind::Event { .. } => "Event",
            ProductKind::EventOutcome { .. } => "EventOutcome",
            ProductKind::EventContract { .. } => "EventContract",
            ProductKind::Unknown => "Unknown",
        }
    }

    pub fn is_option(&self) -> bool {
        matches!(self, ProductKind::Option { .. })
    }

    pub fn is_option_like(&self) -> bool {
        match self {
            ProductKind::Option { .. } => true,
            ProductKind::EventContract { underlying, expiration } => {
                underlying.is_some() && expiration.is_some()
            }
            _ => false,
        }
    }

    // TODO: support equity options
    pub fn is_security(&self) -> bool {
        matches!(self, ProductKind::Equity)
    }

    pub fn is_event(&self) -> bool {
        matches!(self, ProductKind::Event { .. })
    }

    pub fn is_event_outcome(&self) -> bool {
        matches!(self, ProductKind::EventOutcome { .. })
    }

    pub fn is_event_contract(&self) -> bool {
        matches!(self, ProductKind::EventContract { .. })
    }

    /// Get all descendant event contract pairs.
    pub fn event_contracts(&self) -> Option<Vec<EventContracts>> {
        match self {
            ProductKind::Event { outcomes, .. } => {
                let mut res = vec![];
                for p in outcomes.iter() {
                    if let Some(cs) = p.kind.event_contracts() {
                        res.extend(cs);
                    }
                }
                Some(res)
            }
            ProductKind::EventOutcome { contracts, .. } => Some(vec![contracts.clone()]),
            _ => None,
        }
    }

    pub fn underlying(&self) -> Option<ProductRef> {
        match self {
            ProductKind::Perpetual { underlying, .. }
            | ProductKind::Future { underlying, .. }
            | ProductKind::Option { underlying, .. }
            | ProductKind::EventContract { underlying, .. } => *underlying,
            _ => None,
        }
    }

    pub fn multiplier(&self) -> Decimal {
        let multiplier = match self {
            ProductKind::Future { multiplier, .. }
            | ProductKind::Perpetual { multiplier, .. }
            | ProductKind::Option { multiplier, .. } => *multiplier,
            ProductKind::FutureSpread { same_side_leg, .. } => {
                same_side_leg.map(|p| p.kind.multiplier())
            }
            ProductKind::Coin { .. }
            | ProductKind::Fiat
            | ProductKind::Equity
            | ProductKind::Index
            | ProductKind::Commodity
            | ProductKind::EventSeries { .. }
            | ProductKind::Event { .. }
            | ProductKind::EventOutcome { .. }
            | ProductKind::EventContract { .. }
            | ProductKind::Unknown => None,
        };
        multiplier.unwrap_or(dec!(1))
    }

    pub fn expiration(&self) -> Option<DateTime<Utc>> {
        match self {
            ProductKind::Future { expiration, .. }
            | ProductKind::Option { expiration, .. } => *expiration,
            ProductKind::FutureSpread { same_side_leg, opp_side_leg } => {
                // We need to return the expiration which occurs first
                // CR mholm for alee: could recurse infinitely
                std::cmp::min(
                    same_side_leg.and_then(|p| p.kind.expiration()),
                    opp_side_leg.and_then(|p| p.kind.expiration()),
                )
            }
            ProductKind::Event { expiration, .. } => *expiration,
            ProductKind::EventContract { expiration, .. } => *expiration,
            _ => None,
        }
    }

    pub fn instrument_type(&self) -> Option<InstrumentType> {
        let instrument_type = match self {
            ProductKind::Future { instrument_type, .. }
            | ProductKind::Perpetual { instrument_type, .. }
            | ProductKind::Option { instrument_type, .. } => *instrument_type,
            ProductKind::FutureSpread { same_side_leg, .. } => same_side_leg
                .map(|p| p.kind.instrument_type().unwrap_or(InstrumentType::Linear)),
            ProductKind::Coin { .. }
            | ProductKind::Fiat
            | ProductKind::Equity
            | ProductKind::Index
            | ProductKind::Commodity
            | ProductKind::EventSeries { .. }
            | ProductKind::Event { .. }
            | ProductKind::EventOutcome { .. }
            | ProductKind::EventContract { .. }
            | ProductKind::Unknown => None,
        };
        instrument_type
    }

    pub fn is_expired(&self, now: &DateTime<Utc>) -> bool {
        match self.expiration() {
            Some(exp) => exp.date_naive() < now.date_naive(),
            None => false,
        }
    }

    pub fn is_linear(&self) -> bool {
        self.instrument_type().is_some_and(|t| t == InstrumentType::Linear)
    }

    pub fn is_inverse(&self) -> bool {
        self.instrument_type().is_some_and(|t| t == InstrumentType::Inverse)
    }

    pub fn is_quanto(&self) -> bool {
        self.instrument_type().is_some_and(|t| t == InstrumentType::Quanto)
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
            ProductKind::Perpetual { underlying, multiplier, instrument_type } => {
                api::symbology::ProductKind::Perpetual {
                    underlying: underlying.map(|u| u.id),
                    multiplier: *multiplier,
                    instrument_type: *instrument_type,
                }
            }
            ProductKind::Future {
                underlying,
                multiplier,
                expiration,
                instrument_type,
            } => api::symbology::ProductKind::Future {
                underlying: underlying.map(|u| u.id),
                multiplier: *multiplier,
                expiration: *expiration,
                instrument_type: *instrument_type,
            },
            ProductKind::FutureSpread { same_side_leg, opp_side_leg } => {
                api::symbology::ProductKind::FutureSpread {
                    same_side_leg: same_side_leg.map(|p| p.id),
                    opp_side_leg: opp_side_leg.map(|p| p.id),
                }
            }
            ProductKind::Option {
                underlying,
                multiplier,
                expiration,
                instrument_type,
            } => api::symbology::ProductKind::Option {
                underlying: underlying.map(|u| u.id),
                multiplier: *multiplier,
                expiration: *expiration,
                instrument_type: *instrument_type,
            },
            ProductKind::Index => api::symbology::ProductKind::Index,
            ProductKind::Commodity => api::symbology::ProductKind::Commodity,
            ProductKind::EventSeries { display_name } => {
                api::symbology::ProductKind::EventSeries {
                    display_name: display_name.clone(),
                }
            }
            ProductKind::Event {
                series,
                outcomes,
                mutually_exclusive,
                expiration,
                display_category,
                display_name,
            } => api::symbology::ProductKind::Event {
                series: series.map(|s| s.id),
                outcomes: outcomes.iter().map(|p| p.id).collect(),
                mutually_exclusive: *mutually_exclusive,
                expiration: *expiration,
                display_category: display_category.clone(),
                display_name: display_name.clone(),
            },
            ProductKind::EventOutcome { display_order, contracts, display_name } => {
                api::symbology::ProductKind::EventOutcome {
                    contracts: match contracts {
                        EventContracts::Single { yes, yes_alias } => {
                            api::symbology::EventContracts::Single {
                                yes: yes.id,
                                yes_alias: yes_alias.clone(),
                            }
                        }
                        EventContracts::Dual { yes, yes_alias, no, no_alias } => {
                            api::symbology::EventContracts::Dual {
                                yes: yes.id,
                                yes_alias: yes_alias.clone(),
                                no: no.id,
                                no_alias: no_alias.clone(),
                            }
                        }
                    },
                    display_order: *display_order,
                    display_name: display_name.clone(),
                }
            }
            ProductKind::EventContract { underlying, expiration } => {
                api::symbology::ProductKind::EventContract {
                    underlying: underlying.map(|p| p.id),
                    expiration: *expiration,
                }
            }
            ProductKind::Unknown => api::symbology::ProductKind::Unknown,
        }
    }
}
