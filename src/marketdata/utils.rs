use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use netidx::{resolver_client::GlobSet, subscriber::Event};
use rust_decimal::Decimal;

pub fn decimal_or_error(e: &netidx::subscriber::Event) -> Result<Decimal> {
    use netidx::subscriber::Value;
    match e {
        Event::Update(Value::Decimal(v)) => Ok(*v),
        Event::Update(other) => bail!("unexpected value: {}", other.to_string()),
        Event::Unsubscribed => bail!("unsubscribed"),
    }
}

// CR alee: move to utils or better yet netidx itself
pub async fn apply_oneshot<F: FnMut(netidx_archive::logfile::Id, Event) -> ()>(
    client: netidx_archive::recorder_client::Client,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
    filter: &GlobSet,
    mut f: F,
) -> Result<()> {
    use netidx_archive::logfile::BatchItem;
    let mut res = client.oneshot(&start, &end, filter).await?;
    for mut shard in res.0.drain(..) {
        for (id, up) in shard.image.drain() {
            f(id, up);
        }
        for (_, mut batch) in shard.deltas.drain(..) {
            for BatchItem(id, up) in batch.drain(..) {
                f(id, up);
            }
        }
    }
    Ok(())
}
