use crate::symbology::Cpty;
use api::{config::Location, symbology::CptyId, ComponentId};
use fxhash::{FxHashMap, FxHashSet};
use netidx::path::Path;

// CR alee: this begs the existence of a ComponentKind type
/// Keeps track of where each component is publishing in netidx
#[derive(Debug, Clone)]
pub struct Paths {
    pub hosted_base: Path,
    pub local_base: Path,
    pub core_base: Path,
    pub local_components: FxHashSet<ComponentId>,
    pub remote_components: FxHashMap<ComponentId, Path>,
    pub use_local_symbology: bool,
    pub use_local_userdb: bool,
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
        let base =
            if self.use_local_symbology { &self.local_base } else { &self.hosted_base };
        base.append("symbology")
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

    /// Hist marketdata candles
    pub fn marketdata_hist_ohlc(&self, cpty: Cpty) -> Path {
        self.marketdata(cpty)
            .append("hist")
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

    pub fn marketdata_marks(&self) -> Path {
        self.local_base.append("qf").append("marks")
    }

    /// Core RPCs base path
    pub fn core(&self) -> Path {
        self.core_base.clone()
    }

    /// Channel for the core started by this config file
    pub fn channel(&self) -> Path {
        self.core_base.append("channel")
    }

    /// One-way write-only channel for core-to-core communication
    pub fn in_channel(&self) -> Path {
        self.core_base.append("in-channel")
    }

    /// Find the most direct channel for a given component
    pub fn component(&self, com: ComponentId) -> Option<Path> {
        let channel_base = if self.local_components.contains(&com) {
            Some(&self.core_base)
        } else if let Some(base) = self.remote_components.get(&com) {
            Some(base)
        } else {
            None
        };
        channel_base.map(|base| base.append("channel"))
    }

    /// UserDB (licensing, registration, etc.)
    pub fn userdb(&self) -> Path {
        let base =
            if self.use_local_userdb { &self.local_base } else { &self.hosted_base };
        base.append("userdb")
    }
}
