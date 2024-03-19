//! Netidx-based stats/metrics publishing library, for admin monitoring

use crate::Common;
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use chrono::Utc;
use futures::{
    channel::mpsc::{self, UnboundedReceiver},
    select_biased, FutureExt, StreamExt,
};
use fxhash::FxHashMap;
use log::{debug, error};
use netidx::{
    path::Path,
    pool::Pooled,
    publisher::{Publisher, UpdateBatch, Val, Value},
};
use once_cell::sync::OnceCell;
use std::{sync::Arc, time::Duration};
use sysinfo::{PidExt, ProcessExt, System, SystemExt};
use tokio::time::interval;

pub enum StatCmd {
    Set(Value),
    AddAcc(Value),
    SubAcc(Value),
    MulAcc(Value),
    DivAcc(Value),
}

pub const SYSINFO_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) fn start_listener_task(
    base_path: Path,
    service: &str,
    mut log_rx: futures::channel::mpsc::Receiver<
        Pooled<Vec<netidx::publisher::WriteRequest>>,
    >,
    mut stats_rx: UnboundedReceiver<(Path, StatCmd)>,
    publisher: Publisher,
    log_level_val: Val,
) -> tokio::task::JoinHandle<()> {
    let timeout = Some(Duration::from_secs(30));
    let mut sysinfo_ticks = 0;
    let mut sysinfo_interval = interval(SYSINFO_INTERVAL);
    let sysinfo_cpu_refresh_num_ticks = 20;
    let service = service.to_string();

    tokio::spawn(async move {
        let mut sysinfo_paths = SysinfoPaths::default();
        let mut sys = System::new_all();
        sys.refresh_all();

        let mut stat_values: FxHashMap<Path, (Val, Value)> = FxHashMap::default();

        // let version_val = publish_version(&publisher, &paths, &component);
        // if let Err(e) = version_val {
        //     error!("failed to publish version: {}", e)
        // }
        // debug!("admin-stats publish_version done");

        loop {
            select_biased! {
                // handle a subscriber attempt to set the log-level
                mut r = log_rx.select_next_some()  => {
                    if let Some(wr) = r.drain(..).last() {
                        debug!("subscriber submitted log-level: {}", wr.value);
                        if let Ok(value) = wr.value.cast_to::<String>() {
                            if let Some(new_level) = log_level_of_string(value) {
                                log::set_max_level(new_level);
                                debug!("new max log level: {}", new_level);
                                let mut batch = publisher.start_batch();
                                log_level_val.update(&mut batch, new_level.to_string());
                                batch.commit(timeout).await
                            }
                        }
                    }
                },
                // handle a common.stat() value publish
                (path, stat) = stats_rx.select_next_some().fuse() => {
                    let mut batch = publisher.start_batch();
                    process_stat(
                        &publisher,
                        base_path.clone(),
                        &service,
                        path,
                        stat,
                        &mut stat_values,
                        &mut batch);
                    // drain and process any other immediately available messages
                    while let Ok(Some((path, stat))) = stats_rx.try_next() {
                        process_stat(
                            &publisher,
                            base_path.clone(),
                            &service,
                            path,
                            stat,
                            &mut stat_values,
                            &mut batch);
                    }
                    batch.commit(timeout).await
                },
                _ = sysinfo_interval.tick().fuse() => {
                    // we get the pid each time just in case it changes, like if we daemonize
                    let my_pid = match sysinfo::get_current_pid() {
                        Ok(pid) => pid,
                        Err(e) => {
                            error!("failed to get current pid: {}", e);
                            return;
                        }
                    };
                    if sysinfo_ticks % sysinfo_cpu_refresh_num_ticks == 0 {
                        // this needs to be done periodically because of cpu scaling, or something
                        sys.refresh_cpu();
                    }
                    sysinfo_ticks += 1;
                    if sys.refresh_process(my_pid) {
                        if let Some(process) = sys.process(my_pid) {
                            publish_sysinfo(
                                base_path.clone(),
                                &service,
                                &publisher,
                                process,
                                &mut sysinfo_paths
                            ).await;
                        } else {
                            debug!("couldn't find info about my own process, pid: {}", my_pid);
                        }
                    }
                },
                complete => break,
            }
        }
    })
}

fn log_level_of_string(s: String) -> Option<log::LevelFilter> {
    match s.to_ascii_lowercase().as_str() {
        "error" => Some(log::LevelFilter::Error),
        "warn" => Some(log::LevelFilter::Warn),
        "info" => Some(log::LevelFilter::Info),
        "debug" => Some(log::LevelFilter::Debug),
        "trace" => Some(log::LevelFilter::Trace),
        "off" => Some(log::LevelFilter::Off),
        _ => None,
    }
}

/// Given a base_path (usually / or /local), append admin to become /admin or /local/admin
fn stat_admin_base(base_path: Path) -> Path {
    base_path.append("admin")
}

pub(crate) fn base_and_aliases(base_path: Path, service: &str) -> Result<PathAndAliases> {
    let admin_base = stat_admin_base(base_path);
    let hostname = my_hostname()?;
    let ord = admin_base.append("by-service").append(&service).append(&hostname);
    let alias1 = admin_base.append("by-host").append(&hostname).append(&service);
    let alias2 =
        admin_base.append("by-service-host").append(&format!("{}-{}", service, hostname));
    Ok((ord, alias1, alias2))
}

pub(crate) fn full_and_alias_paths(
    base_path: Path,
    service: &str,
    path: Path,
) -> Result<PathAndAliases> {
    let (full_base, alias1_base, alias2_base) = base_and_aliases(base_path, service)?;
    Ok((full_base.append(&path), alias1_base.append(&path), alias2_base.append(&path)))
}

fn compute_value(cur: Option<&Value>, cmd: StatCmd) -> Value {
    match cmd {
        StatCmd::Set(v) => v,
        StatCmd::AddAcc(v) => match cur {
            None => v,
            Some(cur) => cur.clone() + v,
        },
        StatCmd::SubAcc(v) => match cur {
            None => Value::I64(0) - v,
            Some(cur) => cur.clone() - v,
        },
        StatCmd::MulAcc(v) => match cur {
            None => Value::I64(0),
            Some(cur) => cur.clone() * v,
        },
        StatCmd::DivAcc(v) => match cur {
            None => Value::I64(0),
            Some(cur) => cur.clone() / v,
        },
    }
}

fn process_stat(
    publisher: &Publisher,
    base_path: Path,
    service: &str,
    relpath: Path,
    cmd: StatCmd,
    stat_values: &mut FxHashMap<Path, (Val, Value)>,
    batch: &mut UpdateBatch,
) -> () {
    // path exists, simply add the updated value to the batch
    if let Some((val, value)) = stat_values.get_mut(&relpath) {
        *value = compute_value(Some(value), cmd);
        val.update(batch, value.clone());
        return;
    }
    // path doesn't exist, create published value
    let value = compute_value(None, cmd);
    let (path, alias1, alias2) =
        match full_and_alias_paths(base_path, service, relpath.clone()) {
            Ok(paths) => paths,
            Err(e) => {
                debug!(
                    "failed to set value '{}' for path: {}: {}",
                    value.clone(),
                    relpath,
                    e.to_string()
                );
                return;
            }
        };
    let val = match publisher.publish(path, value.clone()) {
        Ok(val) => val,
        Err(e) => {
            debug!(
                "failed to publish value '{}' for path: {}: {}",
                value.clone(),
                relpath,
                e.to_string()
            );
            return;
        }
    };
    if let Err(e) = publisher.alias(val.id(), alias1) {
        debug!(
            "failed to alias value '{}' for path: {}: {}",
            value.clone(),
            relpath,
            e.to_string()
        )
    }
    if let Err(e) = publisher.alias(val.id(), alias2) {
        debug!(
            "failed to alias value '{}' for path: {}: {}",
            value.clone(),
            relpath,
            e.to_string()
        )
    }
    if let Some(_) = stat_values.insert(relpath.clone(), (val, value)) {
        debug!("unexpected existing stat at path {}", relpath)
    }
}

type PathAndAliases = (Path, Path, Path);

#[derive(Default)]
struct SysinfoPaths {
    heartbeat: Option<Val>,
    pid: Option<Val>,
    cpu: Option<Val>,
    rss: Option<Val>,
    vsize: Option<Val>,
    disk_write: Option<Val>,
    disk_read: Option<Val>,
}

fn publish_sysinfo_one(
    sysinfo_path: &mut Option<Val>,
    publisher: &Publisher,
    relpath: Path,
    base_path: Path,
    service: &str,
    value: Value,
    batch: &mut UpdateBatch,
) -> Result<()> {
    match sysinfo_path {
        Some(ref val) => val.update(batch, value),
        None => {
            let stem = Path::from("sysinfo");
            let (path, alias1, alias2) =
                full_and_alias_paths(base_path, service, stem.append(&relpath))?;
            let val = publisher.publish(path, value)?;
            if let Err(e) = publisher.alias(val.id(), alias1) {
                debug!("failed to alias '{}': {}", relpath, e.to_string())
            }
            if let Err(e) = publisher.alias(val.id(), alias2) {
                debug!("failed to alias '{}': {}", relpath, e.to_string())
            }
            *sysinfo_path = Some(val)
        }
    }
    Ok(())
}

async fn publish_sysinfo_inner(
    base_path: Path,
    service: &str,
    publisher: &Publisher,
    process: &sysinfo::Process,
    sysinfo_paths: &mut SysinfoPaths,
) -> Result<()> {
    let now = Utc::now();
    let disk_usage = process.disk_usage();
    let mut batch = publisher.start_batch();
    publish_sysinfo_one(
        &mut sysinfo_paths.heartbeat,
        &publisher,
        Path::from("heartbeat"),
        base_path.clone(),
        service,
        now.into(),
        &mut batch,
    )?;
    publish_sysinfo_one(
        &mut sysinfo_paths.pid,
        &publisher,
        Path::from("pid"),
        base_path.clone(),
        service,
        process.pid().as_u32().into(),
        &mut batch,
    )?;
    publish_sysinfo_one(
        &mut sysinfo_paths.cpu,
        &publisher,
        Path::from("cpu"),
        base_path.clone(),
        service,
        process.cpu_usage().into(),
        &mut batch,
    )?;
    publish_sysinfo_one(
        &mut sysinfo_paths.rss,
        &publisher,
        Path::from("rss"),
        base_path.clone(),
        service,
        process.memory().into(),
        &mut batch,
    )?;
    publish_sysinfo_one(
        &mut sysinfo_paths.vsize,
        &publisher,
        Path::from("vsize"),
        base_path.clone(),
        service,
        process.virtual_memory().into(),
        &mut batch,
    )?;
    publish_sysinfo_one(
        &mut sysinfo_paths.disk_write,
        &publisher,
        Path::from("disk_write"),
        base_path.clone(),
        service,
        disk_usage.written_bytes.into(),
        &mut batch,
    )?;
    publish_sysinfo_one(
        &mut sysinfo_paths.disk_read,
        &publisher,
        Path::from("disk_read"),
        base_path.clone(),
        service,
        disk_usage.read_bytes.into(),
        &mut batch,
    )?;
    let timeout = Some(Duration::from_secs(30));
    batch.commit(timeout).await;
    Ok(())
}

async fn publish_sysinfo(
    base_path: Path,
    service: &str,
    publisher: &Publisher,
    process: &sysinfo::Process,
    sysinfo_paths: &mut SysinfoPaths,
) -> () {
    match publish_sysinfo_inner(base_path, service, publisher, process, sysinfo_paths)
        .await
    {
        Ok(()) => (),
        Err(e) => debug!("failed to publish sysinfo: {}", e.to_string()),
    }
}

// fn publish_version(
//     publisher: &Publisher,
//     paths: &Paths,
//     component: &Component,
// ) -> Result<Val> {
//     let p = Path::from("rev");
//     let (path, alias1, alias2) = full_and_alias_paths(&p, paths, *component)?;
//     let val = publisher.publish::<String>(path, crate::GIT_VERSION.into())?;
//     publisher.alias(val.id(), alias1)?;
//     publisher.alias(val.id(), alias2)?;
//     Ok(val)
// }

static MY_HOSTNAME: OnceCell<Result<ArcStr>> = OnceCell::new();

pub fn my_hostname() -> Result<ArcStr> {
    let h = MY_HOSTNAME.get_or_init(|| {
        let sys = System::new();
        match sys.host_name() {
            None => Err(anyhow!("could not determine hostname")),
            Some(h) => Ok(ArcStr::from(h)),
        }
    });
    match h {
        Ok(h) => Ok(h.clone()),
        Err(e) => Err(anyhow!(e.to_string())),
    }
}

/// Standalone version of admin stats
#[derive(Clone, Debug)]
pub struct AdminStats {
    stats_tx: Arc<mpsc::UnboundedSender<(Path, StatCmd)>>,
}

impl AdminStats {
    pub fn start(publisher: &Publisher, base_path: Path, service: &str) -> Result<Self> {
        let (log_tx, log_rx) = mpsc::channel(3);
        let ll_relpath = Path::from("log-level");
        let ll_paths =
            full_and_alias_paths(base_path.clone(), service, ll_relpath.clone())?;
        let (ll_path, ll_alias1, ll_alias2) = ll_paths;
        let ll_val = publisher.publish(ll_path, log::max_level().to_string())?;
        let () = publisher.alias(ll_val.id(), ll_alias1)?;
        let () = publisher.alias(ll_val.id(), ll_alias2)?;
        publisher.writes(ll_val.id(), log_tx);
        let (stats_tx, stats_rx) = mpsc::unbounded();
        start_listener_task(
            base_path.clone(),
            service,
            log_rx,
            stats_rx,
            publisher.clone(),
            ll_val,
        );
        Ok(Self { stats_tx: Arc::new(stats_tx) })
    }

    fn stat_cmd(&self, path: impl Into<Path>, cmd: StatCmd) {
        match self.stats_tx.unbounded_send((path.into(), cmd)) {
            Ok(()) => (),
            Err(e) => debug!("couldn't send stat: {}", e.to_string()),
        }
    }

    pub fn set(&self, path: impl Into<Path>, stat: impl Into<Value>) {
        self.stat_cmd(path, StatCmd::Set(stat.into()))
    }

    pub fn add_acc(&self, path: impl Into<Path>, stat: impl Into<Value>) {
        self.stat_cmd(path, StatCmd::AddAcc(stat.into()))
    }

    pub fn sub_acc(&self, path: impl Into<Path>, stat: impl Into<Value>) {
        self.stat_cmd(path, StatCmd::SubAcc(stat.into()))
    }

    pub fn mul_acc(&self, path: impl Into<Path>, stat: impl Into<Value>) {
        self.stat_cmd(path, StatCmd::MulAcc(stat.into()))
    }

    pub fn div_acc(&self, path: impl Into<Path>, stat: impl Into<Value>) {
        self.stat_cmd(path, StatCmd::DivAcc(stat.into()))
    }
}

/// Attach the stats system to [Common]
impl Common {
    /// Initialize the stats system for publishing statistics under
    /// $base/admin/$service/$host and also
    /// $base/admin/$host/$service
    pub fn init_stats(&self, service: &str, is_local: bool) -> Result<()> {
        let base_path = if is_local {
            self.paths.local_base.clone()
        } else {
            self.paths.hosted_base.clone()
        };
        let publisher = self.publisher.clone();
        if let Err(_) = self.stats.set(AdminStats::start(&publisher, base_path, service)?)
        {
            bail!("init_stats: stats was already initialized!");
        }
        Ok(())
    }

    /// Publishes a stat. Stats are published under ${base}/admin by component and by host
    /// A prior call to `init_stats` must have been made otherwise this will be a no-op.
    /// # Arguments
    /// * `path` - a relative path describing this stat
    /// * `stat` - the value of this stat
    pub fn stat_set(&self, path: impl Into<Path>, stat: impl Into<Value>) {
        if let Some(stats) = self.stats.get() {
            stats.set(path, stat)
        }
    }

    /// Add accumulates an existing stat. If the existing stat is not initialized
    /// then it assumed to be zero.
    pub fn stat_add_acc(&self, path: impl Into<Path>, stat: impl Into<Value>) {
        if let Some(stats) = self.stats.get() {
            stats.add_acc(path, stat)
        }
    }

    /// Sutract accumulates an existing stat. If the existing stat is not initialized
    /// then it is assumed to be zero.
    pub fn stat_sub_acc(&self, path: impl Into<Path>, stat: impl Into<Value>) {
        if let Some(stats) = self.stats.get() {
            stats.sub_acc(path, stat)
        }
    }

    /// Multiply accumulates an existing stat. If the existing stat is not initialized
    /// then it assumed to be zero
    pub fn stat_mul_acc(&self, path: impl Into<Path>, stat: impl Into<Value>) {
        if let Some(stats) = self.stats.get() {
            stats.mul_acc(path, stat)
        }
    }

    /// Divide accumulates an existing stat. If the existing stat is not initialized
    /// then it is assumed to be zero
    pub fn stat_div_acc(&self, path: impl Into<Path>, stat: impl Into<Value>) {
        if let Some(stats) = self.stats.get() {
            stats.div_acc(path, stat)
        }
    }
}
