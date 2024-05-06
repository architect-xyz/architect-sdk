use crate::{ChannelDriver, Common};
use anyhow::Result;
use api::{expect_response, orderflow::AccountMessage, Account};
use std::sync::Arc;
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
