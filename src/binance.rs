use std::collections::HashMap;

use chrono::{DateTime, Utc};
use log::warn;
use reqwest::Client;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, de::DeserializeOwned};

use crate::error::Result;
use crate::structs::MarginData;

const BORROWINGS_URL: &str = "https://www.binance.com/bapi/margin/v1/public/margin/statistics/24h-borrow-and-repay";
const LEFT_AVAILABLE_URL: &str = "https://www.binance.com/bapi/margin/v1/public/margin/marketStats/available-inventory";
const SPOT_DAILY_VOLUME_URL: &str = "https://api.binance.com/api/v3/ticker/24hr";

const CANDLESTICKS_URL: &str = "https://api.binance.com/api/v3/klines";
const CANDLES_INTERVAL: &str = "5m";
const CANDLES_NUMBER: &str = "50";

const FUTURES_EXCHANGE_INFO: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";
const FUNDING_RATE_URL: &str = "https://fapi.binance.com/fapi/v1/premiumIndex";

const LONG_SHORT_RATIO_URL: &str = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio";
// There is no chance to use less interval, the data is returned for a 5-minute interval,
// where both the start and end timestamps are aligned to 5-minute boundaries (e.g., 00:00, 00:05, 00:10, etc.).
const RATIO_INTERVAL: &str = "5m";
const RATIO_LIMIT: &str = "50";

const OPEN_INTEREST_URL: &str = "https://fapi.binance.com/futures/data/openInterestHist";
// There is no chance to use less interval, the data is returned for a 5-minute interval,
// where both the start and end timestamps are aligned to 5-minute boundaries (e.g., 00:00, 00:05, 00:10, etc.).
const OPEN_INTEREST_INTERVAL: &str = "5m";
const OPEN_INTEREST_LIMIT: &str = "50";

const TO_EXCLUDE: [&str; 20] = [
    "USD1", "USDT", "USDC", "USDP", "FDUSD", "BTC", "WBTC", "WBETH", "ETH", "SOL", "BNSOL",
    "XRP", "BNB", "ADA", "SUI", "LTC", "TRX", "PAXG", "DAI", "BFUSD",
];

fn to_datetime_utc<'de, D>(deserializer: D) -> std::result::Result<DateTime<Utc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let ts = i64::deserialize(deserializer)?;

    DateTime::<Utc>::from_timestamp_millis(ts)
        .ok_or_else(|| serde::de::Error::custom("Invalid timestamp"))
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BinanceResponse<T> {
    Ok(T),
    Err(BinanceError),
}

impl<T> BinanceResponse<T> {
    fn into_result(self) -> Result<T> {
        match self {
            BinanceResponse::Ok(success) => Ok(success),
            BinanceResponse::Err(err) => Err(err.into()),
        }
    }
}

// {"timestamp":1753116119982,"status":404,"error":"Not Found","message":"No message available","path":"/v1/public/margin/marketStats/available-inventory/sk"}
#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct BinanceError {
    pub status: u32,
    pub error: String,
    pub message: String,
}

impl From<BinanceError> for crate::error::ServiceError {
    fn from(value: BinanceError) -> Self {
        Self::Internal(value.message)
    }
}

#[derive(Debug, Deserialize)]
struct MarginDataResponse<T> {
    data: T,
}

impl<T> MarginDataResponse<T> {
    fn into_inner(self) -> T {
        self.data
    }
}

#[derive(Debug, Deserialize)]
struct BorrowingsData {
    coins: Vec<BorrowedAsset>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BinanceOpenInterest {
    pub symbol: String,
    pub sum_open_interest_value: Decimal,
    #[serde(deserialize_with = "to_datetime_utc", rename = "timestamp")]
    pub datetime: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceExchangeInfoSymbol {
    pub symbol: String,
    pub contract_type: String,
    pub status: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceExchangeInfoResponse {
    pub symbols: Vec<BinanceExchangeInfoSymbol>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceFundingRate {
    pub symbol: String,
    pub last_funding_rate: Decimal,
    #[serde(deserialize_with = "to_datetime_utc")]
    pub next_funding_time: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BorrowedAsset {
    asset: String,
    total_borrow: Decimal,
    total_repay: Decimal,
    total_borrow_in_usdt: Decimal,
    total_repay_in_usdt: Decimal,
}

#[derive(Debug, Deserialize)]
struct AvailableInventoryData {
    assets: HashMap<String, Decimal>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceDailyVolume {
    pub symbol: String,
    pub volume: Decimal,
    pub quote_volume: Decimal,
}

type BinanceCandleResponse = Vec<Decimal>;

#[derive(Debug, Deserialize, Clone)]
pub struct BinanceCandleMarketTradeVolume {
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub is_closed: bool,
    pub sell_quote_volume: Decimal,
    pub buy_quote_volume: Decimal,
}

impl From<BinanceCandleResponse> for BinanceCandleMarketTradeVolume {
    fn from(value: BinanceCandleResponse) -> Self {
        let open_time = value[0].to_i64().expect("Failed to parse open time");
        let close_time = value[6].to_i64().expect("Failed to parse close time");
        let total_quote_vol = value[7];
        let taker_buy_quote_vol = value[10];
        let taker_sell_quote_vol = total_quote_vol - taker_buy_quote_vol;

        let ts = Utc::now().timestamp_millis();
        let is_closed = if close_time <= ts { true } else { false };

        let open_time = DateTime::<Utc>::from_timestamp_millis(open_time)
            .expect("Failed to parse open time to UTC");

        let close_time = DateTime::<Utc>::from_timestamp_millis(close_time)
            .expect("Failed to parse close time to UTC");

        BinanceCandleMarketTradeVolume {
            open_time,
            close_time,
            is_closed,
            sell_quote_volume: taker_sell_quote_vol,
            buy_quote_volume: taker_buy_quote_vol,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BinanceLongShortRatioPositions {
    pub symbol: String,
    pub long_account: Decimal,
    pub short_account: Decimal,
    pub long_short_ratio: Decimal,
    #[serde(deserialize_with = "to_datetime_utc", rename = "timestamp")]
    pub datetime: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct Binance {
    client: Client,
}

impl Binance {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    fn deserialize_response<T: DeserializeOwned>(&self, resp: String) -> Result<T> {
        serde_json::from_str::<BinanceResponse<T>>(&resp)?.into_result()
    }

    async fn send_request<T: DeserializeOwned>(&self, url: &str) -> Result<T> {
        let resp = self.client.get(url).send().await?.text().await?;
        self.deserialize_response(resp)
    }

    async fn send_request_with_query_params<T: DeserializeOwned>(
        &self,
        url: &str,
        query: &[(&str, &str)],
    ) -> Result<T> {
        let resp = self
            .client
            .get(url)
            .query(query)
            .send()
            .await?
            .text()
            .await?;

        self.deserialize_response(resp)
    }

    async fn get_borrowings_data(&self) -> Result<BorrowingsData> {
        Ok(self
            .send_request::<MarginDataResponse<BorrowingsData>>(BORROWINGS_URL)
            .await?
            .into_inner())
    }

    async fn get_available_inventory(&self) -> Result<AvailableInventoryData> {
        Ok(self
            .send_request::<MarginDataResponse<AvailableInventoryData>>(LEFT_AVAILABLE_URL)
            .await?
            .into_inner())
    }

    pub async fn get_futures_exchange_info(&self) -> Result<BinanceExchangeInfoResponse> {
        self.send_request(FUTURES_EXCHANGE_INFO).await
    }

    pub async fn get_spot_daily_volume(&self, symbol: &str) -> Result<BinanceDailyVolume> {
        let query = &[("type", "MINI"), ("symbol", symbol)];
        self.send_request_with_query_params(SPOT_DAILY_VOLUME_URL, query).await
    }

    pub async fn get_funding_rate(&self, symbol: &str) -> Result<BinanceFundingRate> {
        let query = &[("symbol", symbol)];
        self.send_request_with_query_params(FUNDING_RATE_URL, query).await
    }

    pub async fn get_open_interest(&self, symbol: &str) -> Result<Vec<BinanceOpenInterest>> {
        let query = &[
            ("symbol", symbol),
            ("period", OPEN_INTEREST_INTERVAL),
            ("limit", OPEN_INTEREST_LIMIT)
        ];
        self.send_request_with_query_params(OPEN_INTEREST_URL, query).await
    }

    pub async fn get_margin_data(&self) -> Result<Vec<MarginData>> {
        let borrowings = self.get_borrowings_data().await?;
        let available = self.get_available_inventory().await?;

        let result = borrowings
            .coins
            .into_iter()
            .map(|data| {
                let available = available
                    .assets
                    .get(&data.asset)
                    .cloned()
                    .unwrap_or_else(|| {
                        warn!("Available assets corrupted. No value for {}", data.asset);
                        Decimal::ZERO
                    });

                MarginData {
                    asset: data.asset,
                    total_borrow: data.total_borrow,
                    total_repay: data.total_repay,
                    total_borrow_in_usdt: data.total_borrow_in_usdt,
                    total_repay_in_usdt: data.total_repay_in_usdt,
                    available,
                }
            })
            .collect();

        Ok(result)
    }

    // Stablecoins and non-scam tokens are excluded
    pub async fn get_margin_data_filtered(&self) -> Result<Vec<MarginData>> {
        let response = self.get_margin_data().await?;

        let filtered = response
            .into_iter()
            .filter(|resp| !TO_EXCLUDE.contains(&resp.asset.as_str()))
            .collect();

        Ok(filtered)
    }

    pub async fn get_candlesticks_market_volume(
        &self,
        symbol: &str,
    ) -> Result<Vec<BinanceCandleMarketTradeVolume>> {
        let query = &[
            ("symbol", symbol),
            ("interval", CANDLES_INTERVAL),
            ("limit", CANDLES_NUMBER),
        ];

        let resp = self
            .send_request_with_query_params::<Vec<BinanceCandleResponse>>(CANDLESTICKS_URL, query)
            .await?
            .into_iter()
            .map(BinanceCandleMarketTradeVolume::from)
            .collect();

        Ok(resp)
    }

    pub async fn get_long_short_ratio(
        &self,
        symbol: &str,
    ) -> Result<Vec<BinanceLongShortRatioPositions>> {
        let query = &[("symbol", symbol), ("period", RATIO_INTERVAL), ("limit", RATIO_LIMIT)];
        self.send_request_with_query_params(LONG_SHORT_RATIO_URL, query).await
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_get_borrowings_data() {
        let binance = Binance::new(Client::new());
        let result = binance.get_borrowings_data().await;
        print!("Result: {:?}", result);
    }

    #[ignore]
    #[tokio::test]
    async fn test_get_available_inventory() {
        let binance = Binance::new(Client::new());
        let result = binance.get_available_inventory().await;
        print!("Result: {:?}", result);
    }

    #[ignore]
    #[tokio::test]
    async fn test_get_open_interest() {
        let binance = Binance::new(Client::new());
        let result = binance.get_open_interest("ETHUSDT").await;
        print!("Result: {:?}", result);
    }

    #[ignore]
    #[tokio::test]
    async fn test_get_exchange_info() {
        let binance = Binance::new(Client::new());
        let result = binance.get_futures_exchange_info().await;
        print!("Result: {:?}", result);
    }

    #[ignore]
    #[tokio::test]
    async fn test_get_funding_rate() {
        let binance = Binance::new(Client::new());
        let result = binance.get_funding_rate("SOLUSDT").await;
        print!("Result: {:?}", result);
    }

    #[ignore]
    #[tokio::test]
    async fn test_get_spot_daily_volume() {
        let binance = Binance::new(Client::new());
        let result = binance.get_spot_daily_volume("SOLUSDT").await;
        print!("Result: {:?}", result);
    }

    #[ignore]
    #[tokio::test]
    async fn test_get_candlesticks_market_volume() {
        let binance = Binance::new(Client::new());
        let result = binance.get_candlesticks_market_volume("SOLUSDT").await.unwrap();
        print!("Result: {:?}", result);
    }

    #[ignore]
    #[tokio::test]
    async fn test_get_long_short_ratio() {
        let binance = Binance::new(Client::new());
        let result = binance.get_long_short_ratio("SOLUSDT").await;
        print!("Result: {:?}", result);
    }
}
