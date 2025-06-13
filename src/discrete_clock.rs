//! A clock timer that ticks exactly on the second, producing times
//! that are clean/aligned to whole numbers of seconds.

use anyhow::{bail, Result};
use chrono::{DateTime, DurationRound, Utc};
use std::future::Future;
use thiserror::Error;
use tokio::time::{Instant, Interval, MissedTickBehavior};

pub trait Clock {
    type Error;

    fn tick(&mut self)
        -> impl Future<Output = Result<DateTime<Utc>, Self::Error>> + Send;
}

pub struct DiscreteClock {
    interval: Interval,
    interval_duration: chrono::Duration,
    tolerance: chrono::Duration,
}

impl DiscreteClock {
    pub fn from_period(period: chrono::Duration) -> Result<Self> {
        if period.subsec_nanos() != 0 {
            bail!("period must be a whole number of seconds");
        }
        let interval_duration = period;
        let dur = interval_duration.to_std()?;
        let now = Utc::now();
        let now_aliased = (now + interval_duration).duration_trunc(interval_duration)?;
        let mut start_at = Instant::now();
        if now_aliased > now {
            start_at = Instant::now() + (now_aliased - now).to_std()?;
        }
        let mut interval = tokio::time::interval_at(start_at, dur);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Ok(Self { interval, interval_duration, tolerance: interval_duration / 100 })
    }

    pub fn from_secs(interval_secs: u64) -> Result<Self> {
        let interval_duration = chrono::Duration::seconds(interval_secs as i64);
        Self::from_period(interval_duration)
    }
}

impl Clock for DiscreteClock {
    type Error = DiscreteClockError;

    /// On the next tick, return the gridded timestamp if within the
    /// desired tolerance (default to 1% of the interval time).
    ///
    /// Otherwise, complain.
    async fn tick(&mut self) -> Result<DateTime<Utc>, DiscreteClockError> {
        self.interval.tick().await;
        let now = Utc::now();
        let now_aliased = now
            .duration_trunc(self.interval_duration)
            .map_err(DiscreteClockError::TickRoundingError)?;
        if (now - now_aliased).abs() > self.tolerance {
            *self = Self::from_period(self.interval_duration)
                .map_err(DiscreteClockError::ResetError)?;
            return Err(DiscreteClockError::TickSkew {
                actual: now,
                expected: now_aliased,
            })?;
        }
        Ok(now_aliased)
    }
}

#[derive(Error, Debug)]
pub enum DiscreteClockError {
    #[error("tick rounding error: {0}")]
    TickRoundingError(#[from] chrono::RoundingError),
    #[error("{actual} is too far off the grid time {expected}")]
    TickSkew { actual: DateTime<Utc>, expected: DateTime<Utc> },
    #[error("failed to reset clock to recover from tick skew: {0}")]
    ResetError(anyhow::Error),
}

impl Clock for Option<DiscreteClock> {
    type Error = DiscreteClockError;

    async fn tick(&mut self) -> Result<DateTime<Utc>, DiscreteClockError> {
        if let Some(clock) = self.as_mut() {
            clock.tick().await
        } else {
            std::future::pending().await
        }
    }
}
