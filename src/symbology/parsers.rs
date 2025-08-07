use anyhow::{bail, Result};
use architect_api::symbology::{PutOrCall, TradableProduct};
use chrono::{Datelike, NaiveDate};
use rust_decimal::Decimal;
use std::str::FromStr;

#[derive(Debug)]
pub struct UsEquityOptionsParts {
    // Architect Tradable Prodcut format:
    // e.g. AAPL  260918C00220500 Option/USD
    // {osi_symbol} Option/USD
    pub tradable_product: TradableProduct,
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
    pub wrap: String,
    pub expiration: NaiveDate,
    pub strike_price: Decimal,
    pub put_or_call: PutOrCall,
}

impl UsEquityOptionsParts {
    // Expceted base product format: {osi_symbol} Option
    pub fn parse_tradable_product(
        tradable_product: &TradableProduct,
    ) -> Result<Option<Self>> {
        let base = tradable_product.base().to_string();
        if !(base.ends_with(" Option")) {
            return Ok(None);
        }
        let osi_symbol = base.trim_end_matches(" Option");
        if osi_symbol.len() != 21 {
            bail!(
                "Expected 21 chars for OSI symbol format, got {} for {}",
                osi_symbol.len(),
                osi_symbol
            );
        }
        return Self::parse_osi_symbol(osi_symbol).map(|s| Some(s));
    }

    pub fn parse_osi_symbol(osi_symbol: &str) -> Result<Self> {
        if osi_symbol.len() != 21 {
            bail!("OSI symbol must be 21 characters, got {}", osi_symbol.len());
        }

        let wrap = osi_symbol[0..6].trim_end().to_string();
        let underlying_symbol = wrap.trim_end_matches(char::is_numeric).to_string();
        let year = format!("20{}", &osi_symbol[6..8]);
        let month = &osi_symbol[8..10];
        let day = &osi_symbol[10..12];
        let put_or_call_char = &osi_symbol[12..13];
        let strike_dollar = &osi_symbol[13..18];
        let strike_decimal = &osi_symbol[18..21];

        let expiration =
            NaiveDate::parse_from_str(&format!("{}{}{}", year, month, day), "%Y%m%d")?;

        let put_or_call = match put_or_call_char {
            "C" => PutOrCall::Call,
            "P" => PutOrCall::Put,
            _ => {
                bail!("Expected C or P for put/call indicator, got {}", put_or_call_char)
            }
        };

        let strike_price =
            Decimal::from_str(&format!("{}.{}", strike_dollar, strike_decimal))?;

        let tradable_product = TradableProduct::from_str(
            &us_options_osi_symbol_to_tradable_product(osi_symbol),
        )?;

        Ok(Self {
            tradable_product,
            osi_symbol: osi_symbol.to_string(),
            underlying_symbol,
            wrap,
            expiration,
            strike_price,
            put_or_call,
        })
    }

    pub fn from_parts(
        underlying_symbol: &str,
        wrap: &str,
        expiration: NaiveDate,
        strike_price: Decimal,
        put_or_call: PutOrCall,
    ) -> Result<Self> {
        let osi_symbol =
            Self::form_osi_symbol(wrap, expiration, strike_price, put_or_call)?;
        let tradable_product = TradableProduct::from_str(
            &us_options_osi_symbol_to_tradable_product(&osi_symbol),
        )?;
        Ok(Self {
            tradable_product,
            osi_symbol,
            underlying_symbol: underlying_symbol.to_string(),
            wrap: wrap.to_string(),
            expiration,
            strike_price,
            put_or_call,
        })
    }

    pub fn form_osi_symbol(
        underlying_symbol: &str,
        expiration: NaiveDate,
        strike_price: Decimal,
        put_or_call: PutOrCall,
    ) -> Result<String> {
        let strike_price_string = strike_price.to_string();
        let (strike_price_dollar_part, strike_price_decimal_part) =
            strike_price_string.split_once(".").unwrap_or((&strike_price_string, "000"));
        let put_or_call_char = match put_or_call {
            PutOrCall::Put => "P",
            PutOrCall::Call => "C",
        };
        Ok(format!(
            "{:<6}{:02}{:02}{:02}{}{:0>5}{:0<3}",
            underlying_symbol,
            expiration.year().to_string().split_off(2),
            expiration.month(),
            expiration.day(),
            put_or_call_char,
            strike_price_dollar_part,
            strike_price_decimal_part
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_option() -> Result<()> {
        let tradable_product =
            TradableProduct::from_str("AAPL  260918C00220500 Option/USD")?;
        let symbology =
            UsEquityOptionsParts::parse_tradable_product(&tradable_product)?.unwrap();
        assert_eq!(symbology.osi_symbol, "AAPL  260918C00220500");
        assert_eq!(symbology.underlying_symbol, "AAPL");
        assert_eq!(symbology.expiration, NaiveDate::from_ymd_opt(2026, 9, 18).unwrap());
        assert_eq!(symbology.strike_price, Decimal::from_str("220.50").unwrap());
        assert_eq!(symbology.put_or_call, PutOrCall::Call);
        Ok(())
    }

    #[test]
    fn test_parse_osi_symbol() -> Result<()> {
        let symbology = UsEquityOptionsParts::parse_osi_symbol("AAPL  260918C00220500")?;
        assert_eq!(
            symbology.tradable_product,
            TradableProduct::from_str("AAPL  260918C00220500 Option/USD")?
        );
        assert_eq!(symbology.osi_symbol, "AAPL  260918C00220500");
        assert_eq!(symbology.underlying_symbol, "AAPL");
        assert_eq!(symbology.expiration, NaiveDate::from_ymd_opt(2026, 9, 18).unwrap());
        assert_eq!(symbology.strike_price, Decimal::from_str("220.50").unwrap());
        assert_eq!(symbology.put_or_call, PutOrCall::Call);
        Ok(())
    }
}

pub fn us_options_osi_symbol_to_tradable_product(osi_symbol: &str) -> String {
    return format!("{} Option/USD", osi_symbol);
}
