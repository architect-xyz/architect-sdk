use crate::{ChannelDriver, Common};
use anyhow::Result;
use api::{
    expect_response, orderflow::AccountMessage, Account, MessageTopic, TypedMessage,
};
use async_stream::stream;
use futures::Stream;
use std::{pin::Pin, sync::Arc};
use uuid::Uuid;

pub async fn get_accounts(
    common: &Common,
    driver: &ChannelDriver,
) -> Result<Arc<Vec<Account>>> {
    let com = common.find_component_of_kind("AccountMaster")?;
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

pub async fn subscribe_accounts(
    common: &Common,
    driver: &ChannelDriver,
) -> Result<AccountStream> {
    let common = common.clone();
    let mut rx = driver.subscribe();
    let com = common.find_component_of_kind("AccountMaster")?;
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
                    TypedMessage::AccountMaster(AccountMessage::Accounts(None, accounts)) =>
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
