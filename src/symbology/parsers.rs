use anyhow::{bail, Result};
use api::symbology::{PutOrCall, TradableProduct};
use chrono::{Datelike, NaiveDate};
use rust_decimal::Decimal;
use std::str::FromStr;

#[derive(Debug)]
pub struct UsEquityOptionsParts {
    // OSI 21-byte format:
    // SSSSSSYYMMDDOPPPPPppp
    // 0-5: symbol, right space padded
    // 6-7: expiration year
    // 8-9: expiration month
    // 10-11: expiration day
    // 12: put or call
    // 13-17: strike price (dollar part), left zero padded
    // 18-20: strike price (decimal part), right zero padded
    pub osi_symbol: String,
    pub underlying_symbol: String,
    pub expiration: NaiveDate,
    pub strike_price: Decimal,
    pub put_or_call: PutOrCall,
}

impl UsEquityOptionsParts {
    // Expceted base product format: AAPL US 20260918 220.50 C Option
    pub fn parse(tradable_product: &TradableProduct) -> Result<Option<Self>> {
        let base = tradable_product.base().to_string();
        if !(base.contains(" US ") && base.contains("Option")) {
            return Ok(None);
        }
        let parts = base.split(" ").collect::<Vec<&str>>();
        if parts.len() != 6 {
            bail!("Expected 6 parts, got {} for symbol {}", parts.len(), base);
        }
        let underlying_symbol = parts[0].replace("-", ".");
        let expiration = NaiveDate::parse_from_str(&parts[2], "%Y%m%d")?;
        let strike_price = Decimal::from_str(&parts[3])?;
        let strike_price_string = strike_price.to_string();
        let (strike_price_dollar_part, strike_price_decimal_part) =
            strike_price_string.split_once(".").ok_or_else(|| {
                anyhow::anyhow!(
                    "Expected strike price to be a decimal, got {}",
                    strike_price
                )
            })?;
        let put_or_call_char = parts[4];
        let put_or_call = match put_or_call_char {
            "C" => PutOrCall::Call,
            "P" => PutOrCall::Put,
            _ => bail!("Expected C or P, got {} for symbol {}", put_or_call_char, base),
        };
        let osi_symbol = format!(
            "{:<6}{:02}{:02}{:02}{}{:0>5}{:0<3}",
            underlying_symbol,
            expiration.year().to_string().split_off(2),
            expiration.month(),
            expiration.day(),
            put_or_call_char,
            strike_price_dollar_part,
            strike_price_decimal_part
        );
        Ok(Some(Self {
            osi_symbol,
            underlying_symbol,
            expiration,
            strike_price,
            put_or_call,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_option() -> Result<()> {
        let tradable_product =
            TradableProduct::from_str("AAPL US 20260918 220.50 C Option/USD")?;
        let symbology = UsEquityOptionsParts::parse(&tradable_product)?.unwrap();
        assert_eq!(symbology.osi_symbol, "AAPL  260918C00220500");
        assert_eq!(symbology.underlying_symbol, "AAPL");
        assert_eq!(symbology.expiration, NaiveDate::from_ymd_opt(2026, 9, 18).unwrap());
        assert_eq!(symbology.strike_price, Decimal::from_str("220.50").unwrap());
        assert_eq!(symbology.put_or_call, PutOrCall::Call);
        Ok(())
    }
}
