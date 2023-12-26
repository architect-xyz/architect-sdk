use crate::symbology::Cpty;
use api::{config::Location, symbology::CptyId};
use fxhash::FxHashMap;
use netidx::path::Path;

// CR alee: this begs the existence of a ComponentKind type
/// Keeps track of where each component is publishing in netidx
#[derive(Debug, Clone)]
pub struct Paths {
    pub hosted_base: Path,
    pub local_base: Path,
    pub marketdata_location_override: FxHashMap<CptyId, Location>,
}

impl Paths {
    /// given a location, return the base path in netidx under which
    /// that location's components may be found
    pub fn base(&self, location: &Location) -> &Path {
        match location {
            Location::Hosted => &self.hosted_base,
            Location::Local => &self.local_base,
        }
    }

    /// Symbology server
    pub fn sym(&self) -> Path {
        self.local_base.append("symbology")
    }

    /// Marketdata feeds
    pub fn marketdata(&self, cpty: Cpty) -> Path {
        let base = self.base(
            self.marketdata_location_override
                .get(&CptyId { venue: cpty.venue.id, route: cpty.route.id })
                .unwrap_or(&Location::Local),
        );
        base.append("qf")
    }

    /// Realtime marketdata feeds
    pub fn marketdata_rt(&self, cpty: Cpty) -> Path {
        self.marketdata(cpty)
            .append("rt")
            .append(&cpty.venue.name)
            .append(&cpty.route.name)
    }

    /// Realtime marketdata candles
    pub fn marketdata_ohlc(&self, cpty: Cpty) -> Path {
        self.marketdata(cpty)
            .append("ohlc")
            .append(&cpty.venue.name)
            .append(&cpty.route.name)
    }

    /// Marketdata APIs (RPCs)
    pub fn marketdata_api(&self, cpty: Cpty) -> Path {
        self.marketdata(cpty)
            .append("api")
            .append(&cpty.venue.name)
            .append(&cpty.route.name)
    }

    /// RFQ feeds
    pub fn marketdata_rfq(&self, cpty: Cpty) -> Path {
        self.marketdata(cpty)
            .append("rfq")
            .append(&cpty.venue.name)
            .append(&cpty.route.name)
    }

    /// Core netidx interface
    pub fn core(&self) -> Path {
        self.local_base.append("core")
    }
}
