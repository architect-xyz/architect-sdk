use crate::symbology::Cpty;
use api::ComponentId;
use netidx::path::Path;
use serde::{Deserialize, Serialize};

/// Component location--local to the installation, or hosted by Architect
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Location {
    Hosted,
    Local,
}

// TODO: think a bit where this belongs. does it matter its not in api/sdk?
// also this begs the existence of a ComponentKind type
/// Keeps track of where each component is publishing in netidx
#[derive(Debug, Clone)]
pub struct Paths {
    pub hosted_base: Path,
    pub local_base: Path,
    // pub default_component_location: Location,
    // pub component_location_override: FxHashMap<Component, Location>,
    // CR-soon alee: remove after deprecation
    // /// Use legacy by-id marketdata paths instead of newer by-name paths
    // pub legacy_marketdata_paths: bool,
}

// TODO: actual paths
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
    pub fn marketdata(&self) -> Path {
        self.local_base.append("qf")
    }

    /// Realtime marketdata feeds
    pub fn marketdata_rt(&self, cpty: Cpty) -> Path {
        self.marketdata().append("rt").append(&cpty.venue.name).append(&cpty.route.name)
    }

    /// Components
    pub fn components(&self) -> Path {
        self.local_base.append("components")
    }

    /// Component
    pub fn component(&self, id: ComponentId) -> Path {
        self.components().append(&Path::from(&format!("{id}")))
    }
}
