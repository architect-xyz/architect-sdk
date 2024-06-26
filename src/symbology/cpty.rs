use super::*;
use anyhow::{anyhow, Result};
use api::symbology::CptyId;
use std::fmt;

/// Commonly used compound type
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct Cpty {
    pub venue: VenueRef,
    pub route: RouteRef,
}

impl Cpty {
    pub fn get(s: &str) -> Option<Self> {
        if let Some((vstr, rstr)) = s.split_once('/') {
            let venue = VenueRef::get(vstr)?;
            let route = RouteRef::get(rstr)?;
            Some(Self { venue, route })
        } else {
            None
        }
    }

    pub fn find(s: &str) -> Result<Self> {
        Self::get(s).ok_or_else(|| anyhow!("missing cpty: {}", s))
    }

    pub fn get_by_id(id: &CptyId) -> Option<Self> {
        let venue = VenueRef::get_by_id(&id.venue)?;
        let route = RouteRef::get_by_id(&id.route)?;
        Some(Self { venue, route })
    }

    pub fn find_by_id(id: &CptyId) -> Result<Self> {
        let venue = VenueRef::find_by_id(&id.venue)?;
        let route = RouteRef::find_by_id(&id.route)?;
        Ok(Self { venue, route })
    }

    pub fn id(&self) -> CptyId {
        CptyId { venue: self.venue.id, route: self.route.id }
    }

    pub fn name(&self) -> String {
        format!("{}/{}", self.venue.name, self.route.name)
    }
}

impl fmt::Display for Cpty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.venue.name, self.route.name)
    }
}
