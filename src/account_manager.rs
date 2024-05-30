use crate::{
    synced::{SyncHandle, Synced},
    ChannelDriver, Common,
};
use anyhow::{anyhow, bail, Result};
use api::{
    expect_response,
    orderflow::{AccountMessage, AccountsUpdate},
    Account, AccountId, AccountPermissions, MessageTopic, TypedMessage, UserId,
};
use arc_swap::ArcSwap;
use async_stream::stream;
use chrono::{DateTime, Utc};
use futures::Stream;
use futures_util::StreamExt;
use immutable_chunkmap::map::MapL as Map;
use log::{debug, error, info};
use netidx::subscriber::{Event, UpdatesFlags};
use netidx_protocols::rpc::client::Proc;
use std::{pin::Pin, sync::Arc};
use uuid::Uuid;

// CR alee: server side should enforce the List permission...send filtered AccountUpdates

#[derive(Debug, Clone)]
pub struct AccountManagerClient {
    accounts: Arc<ArcSwap<Accounts>>,
    sync_handle: SyncHandle<bool>,
}

impl AccountManagerClient {
    pub fn new() -> Self {
        Self {
            accounts: Arc::new(ArcSwap::from_pointee(Accounts::default())),
            sync_handle: SyncHandle::new(false),
        }
    }

    pub fn new_with_subscription(common: Common) -> Self {
        Self::new_with_debug_subscription(common, false)
    }

    pub fn new_with_debug_subscription(
        common: Common,
        debug_print_updates: bool,
    ) -> Self {
        let t = Self::new();
        let accounts = t.accounts.clone();
        let sync_handle = t.sync_handle.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::run_subscription(
                common,
                &accounts,
                &sync_handle,
                debug_print_updates,
            )
            .await
            {
                error!("account manager subscription failed: {}", e);
            }
        });
        t
    }

    async fn run_subscription(
        common: Common,
        accounts: &ArcSwap<Accounts>,
        sync_handle: &SyncHandle<bool>,
        debug_print_updates: bool,
    ) -> Result<()> {
        use futures::channel::mpsc;
        let com = common.find_component_of_kind("AccountManager")?;
        let (base, _) = common.paths.component(com)?;
        let (tx, mut rx) = mpsc::channel(3);
        let updates_sub = common.subscriber.subscribe(base.append("updates"));
        updates_sub.updates(UpdatesFlags::STOP_COLLECTING_LAST, tx);
        updates_sub.wait_subscribed().await?;
        let mut epoch;
        let mut seq;
        loop {
            debug!("loading snapshot");
            let get_latest_snapshot =
                Proc::new(&common.subscriber, base.append("get-latest-snapshot"))?;
            let snap: AccountsUpdate =
                get_latest_snapshot.call([] as [(&str, _); 0]).await?.cast_to()?;
            epoch = snap.epoch;
            seq = snap.sequence_number;
            accounts.store(Arc::new(snap.into()));
            sync_handle.set(true);
            'batch: while let Some(mut batch) = rx.next().await {
                'inner: for (_, ev) in batch.drain(..) {
                    match ev {
                        Event::Unsubscribed => {
                            while let Ok(_) = rx.try_next() {}
                            break 'batch;
                        }
                        Event::Update(v) => match v.cast_to::<Option<AccountsUpdate>>() {
                            Ok(Some(u)) => {
                                if debug_print_updates {
                                    println!("update: {:?}", u);
                                }
                                if u.epoch != epoch {
                                    info!("epoch advance: {} -> {}", epoch, u.epoch);
                                    break 'batch;
                                }
                                if u.sequence_number < seq + 1 {
                                    continue 'inner;
                                }
                                if u.sequence_number > seq + 1 {
                                    error!(
                                        "out of order update: {} -> {}",
                                        seq, u.sequence_number
                                    );
                                    break 'batch;
                                }
                                seq = u.sequence_number;
                                let new_version = if u.is_snapshot {
                                    u.into()
                                } else {
                                    accounts.load().update(&u.into())
                                };
                                accounts.store(Arc::new(new_version));
                            }
                            Ok(None) => {}
                            Err(e) => {
                                sync_handle.set(false);
                                bail!("protocol error: {}", e);
                            }
                        },
                    }
                }
            }
            sync_handle.set(false);
        }
    }

    pub fn update(&self, up: AccountsUpdate) {
        let new_version = if up.is_snapshot {
            up.into()
        } else {
            self.accounts.load().update(&up.into())
        };
        self.accounts.store(Arc::new(new_version));
    }

    pub fn snapshot(&self, epoch: DateTime<Utc>, sequence_number: u64) -> AccountsUpdate {
        let snap = self.accounts.load();
        let mut accounts = vec![];
        let mut permissions = vec![];
        for (_, a) in &snap.accounts {
            accounts.push(a.clone());
        }
        for (user, by_account) in &snap.permissions_by_user {
            for (account, perms) in by_account {
                permissions.push((*user, *account, *perms));
            }
        }
        AccountsUpdate {
            epoch,
            sequence_number,
            is_snapshot: true,
            accounts: if accounts.is_empty() { None } else { Some(accounts) },
            permissions: if permissions.is_empty() { None } else { Some(permissions) },
        }
    }

    pub fn synced(&self) -> Synced<bool> {
        self.sync_handle.synced()
    }

    pub async fn wait_synced(&self, timeout: Option<std::time::Duration>) -> Result<()> {
        self.sync_handle.synced().wait_synced(timeout).await
    }

    pub fn get_account(&self, id: &AccountId) -> Option<Account> {
        self.accounts.load().accounts.get(id).cloned()
    }

    pub fn find_account(&self, id: &AccountId) -> Result<Account> {
        self.get_account(id).ok_or_else(|| anyhow!("no such account: {}", id))
    }

    pub fn get_account_permissions(
        &self,
        user: &UserId,
        account: &AccountId,
    ) -> AccountPermissions {
        self.accounts
            .load()
            .permissions_by_user
            .get(user)
            .and_then(|m| m.get(account))
            .copied()
            .unwrap_or(AccountPermissions::empty())
    }

    pub fn list_accounts(&self, user: Option<&UserId>) -> Vec<AccountId> {
        let accounts = self.accounts.load();
        let mut account_ids = vec![];
        if let Some(user) = user {
            if let Some(by_account) = accounts.permissions_by_user.get(user) {
                for (id, _) in by_account {
                    account_ids.push(*id);
                }
            }
        } else {
            for (id, _) in &accounts.accounts {
                account_ids.push(*id);
            }
        }
        account_ids
    }
}

#[derive(Debug, Default, Clone)]
struct Accounts {
    accounts: Map<AccountId, Account>,
    permissions_by_user: Map<UserId, Map<AccountId, AccountPermissions>>,
}

impl Accounts {
    fn update(&self, other: &Self) -> Self {
        Self {
            accounts: self.accounts.union(&other.accounts, |_, _, r| Some(*r)),
            permissions_by_user: self
                .permissions_by_user
                .union(&other.permissions_by_user, |_, l, r| {
                    Some(l.union(r, |_, _, r| Some(*r)))
                }),
        }
    }

    // CR alee: add prune method to remove empty permissions entries
}

impl From<AccountsUpdate> for Accounts {
    fn from(up: AccountsUpdate) -> Self {
        let mut t = Self::default();
        if let Some(accounts) = up.accounts {
            for a in accounts {
                t.accounts.insert_cow(a.id, a);
            }
        }
        if let Some(permissions) = up.permissions {
            for (user, account, perms) in permissions {
                let by_user = t.permissions_by_user.get_or_insert_cow(user, Map::default);
                by_user.insert_cow(account, perms);
            }
        }
        t
    }
}

// CR alee: deprecate
pub async fn get_accounts(
    common: &Common,
    driver: &ChannelDriver,
) -> Result<Arc<Vec<Account>>> {
    let com = common.find_component_of_kind("AccountManager")?;
    let id = Uuid::new_v4();
    let accounts = driver
        .request_and_wait_for(
            com,
            AccountMessage::GetAccounts(id),
            expect_response!(AccountMessage::Accounts(_, accounts) => accounts),
        )
        .await?;
    Ok(accounts)
}

/// Stream of (accounts, is_snapshot)
pub type AccountStream = Pin<Box<dyn Stream<Item = (Arc<Vec<Account>>, bool)> + Send>>;

// CR alee: deprecate
pub async fn subscribe_accounts(
    common: &Common,
    driver: &ChannelDriver,
) -> Result<AccountStream> {
    let common = common.clone();
    let mut rx = driver.subscribe();
    let com = common.find_component_of_kind("AccountManager")?;
    driver.subscribe_channel_to_topics(MessageTopic::Accounts.into())?;
    let id = Uuid::new_v4();
    let initial_accounts = driver
        .request_and_wait_for(
            com,
            AccountMessage::GetAccounts(id),
            expect_response!(AccountMessage::Accounts(_, accounts) => accounts),
        )
        .await?;
    let stream = stream! {
        // CR alee: probably want some way to re-query initial accounts if e.g. the core restarts
        yield (initial_accounts, true);
        while let Ok(batch) = rx.recv().await {
            for env in batch.iter() {
                match &env.msg {
                    TypedMessage::AccountManager(AccountMessage::Accounts(None, accounts)) =>
                    {
                        yield (accounts.clone(), false);
                    }
                    _ => {}
                }
            }
        }
    };
    Ok(Box::pin(stream))
}
