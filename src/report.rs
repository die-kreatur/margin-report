use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use log::{error, info};
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use tokio::time::interval;

use crate::binance::{
    Binance, BinanceCandleMarketTradeVolume, BinanceDailyVolume, BinanceLongShortRatioPositions,
    BinanceOpenInterest,
};
use crate::error::Result;
use crate::structs::{MarginDataUpdated, TimeDifference};
use crate::utils::find_percentage_diff;

const EXCHANGE_INFO_UPDATE_INTERVAL: Duration = Duration::from_secs(750);
const INTERVALS: [Interval; 4] = [Interval::M5, Interval::M15, Interval::H1, Interval::H4];

pub struct Report {
    pub symbol: String,
    pub margin_data: MarginDataReport,
    pub spot: SpotReport,
    pub futures: Option<FuturesReport>,
}

pub struct MarginDataReport {
    pub total_borrow: Decimal,
    pub total_borrow_usdt: Decimal,
    pub total_repay: Decimal,
    pub total_repay_usdt: Decimal,
    pub borrow_change: Decimal,
    pub repay_change: Decimal,
    pub br_ratio: Decimal,
    pub available: Decimal,
}

#[derive(Debug)]
pub struct SpotReport {
    pub volume_change: Vec<VolumeChange>,
    pub daily_volume: Option<BinanceDailyVolume>,
}

#[derive(Debug)]
pub struct FuturesReport {
    pub funding_rate: Option<FundingRateReport>,
    pub long_short_ratio: Vec<LongShortRatioReport>,
    pub open_interest: Vec<OpenInterestChange>,
}

#[derive(Debug)]
pub struct FundingRateReport {
    pub funding_rate: Decimal,
    pub next_funding_time: TimeDifference,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interval {
    Now,
    M5,
    M15,
    H1,
    H4,
}

impl Interval {
    fn index(&self) -> usize {
        match self {
            Interval::Now => 0,
            Interval::M5 => 1,
            Interval::M15 => 3,
            Interval::H1 => 12,
            Interval::H4 => 48,
        }
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val = match self {
            Interval::Now => "now",
            Interval::M5 => "5m",
            Interval::M15 => "15m",
            Interval::H1 => "1h",
            Interval::H4 => "4h",
        };

        write!(f, "{}", val)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct VolumeChange {
    pub interval: Interval,
    pub sell: Decimal,
    pub buy: Decimal,
}

#[derive(Debug, PartialEq, Eq)]
pub struct LongShortRatioReport {
    pub interval: Interval,
    pub ratio: Decimal,
}

#[derive(Debug, PartialEq, Eq)]
pub struct OpenInterestChange {
    pub interval: Interval,
    pub change: Decimal,
}

fn filter_sort_candles_volumes(
    volumes: Vec<BinanceCandleMarketTradeVolume>,
) -> Vec<BinanceCandleMarketTradeVolume> {
    let mut volumes = volumes
        .into_iter()
        .filter(|vol| vol.is_closed)
        .collect::<Vec<_>>();

    volumes.sort_by(|item1, item2| item2.open_time.cmp(&item1.open_time));
    volumes
}

fn calculate_volume_changes(volumes: Vec<BinanceCandleMarketTradeVolume>) -> Vec<VolumeChange> {
    let volumes = filter_sort_candles_volumes(volumes);

    let Some(latest) = volumes.first() else {
        return Vec::new();
    };

    INTERVALS
        .iter()
        .filter_map(|interval| {
            volumes
                .get(interval.index())
                .map(|volume| (interval, volume))
        })
        .map(|(interval, volume)| {
            let sell_diff = find_percentage_diff(latest.sell_quote_volume, volume.sell_quote_volume);
            let buy_diff = find_percentage_diff(latest.buy_quote_volume, volume.buy_quote_volume);

            VolumeChange {
                interval: *interval,
                sell: sell_diff,
                buy: buy_diff,
            }
        })
        .collect()
}

fn calculate_open_interest_changes(
    mut open_interest: Vec<BinanceOpenInterest>,
) -> Vec<OpenInterestChange> {
    // ensure that order is correct and the newest open interest goes first
    open_interest.sort_by(|item1, item2| item2.datetime.cmp(&item1.datetime));

    let Some(recent) = open_interest.first() else {
        return Vec::new();
    };

    INTERVALS
        .iter()
        .filter_map(|interval| open_interest.get(interval.index()).map(|oi| (interval, oi)))
        .map(|(interval, oi)| {
            let change = find_percentage_diff(recent.sum_open_interest_value, oi.sum_open_interest_value);

            OpenInterestChange {
                interval: *interval,
                change,
            }
        })
        .collect()
}

fn get_long_short_ratios(
    mut ratios: Vec<BinanceLongShortRatioPositions>,
) -> Vec<LongShortRatioReport> {
    // ensure that order is correct and the newest ratios go first
    ratios.sort_by(|item1, item2| item2.datetime.cmp(&item1.datetime));

    let Some(recent) = ratios.first() else {
        return Vec::new();
    };

    let mut data = INTERVALS
        .iter()
        .filter_map(|interval| ratios.get(interval.index()).map(|ratio| (interval, ratio)))
        .map(|(interval, ratio)| LongShortRatioReport {
            interval: *interval,
            ratio: ratio.long_short_ratio.trunc_with_scale(2).normalize(),
        })
        .collect::<Vec<_>>();

    data.insert(
        0,
        LongShortRatioReport {
            interval: Interval::Now,
            ratio: recent.long_short_ratio.trunc_with_scale(2).normalize(),
        },
    );
    data
}

pub struct ReportCollector {
    binance: Binance,
    futures_symbols: RwLock<HashSet<String>>,
}

impl ReportCollector {
    pub fn new(binance: Binance) -> Self {
        Self {
            binance,
            futures_symbols: RwLock::new(HashSet::new()),
        }
    }
    async fn get_futures_exchange_info_pairs(&self) -> Result<HashSet<String>> {
        let exch_info = self.binance.get_futures_exchange_info().await?;

        let trading_symbols = exch_info
            .symbols
            .into_iter()
            .filter(|item| &item.status == "TRADING" && &item.contract_type == "PERPETUAL")
            .map(|item| item.symbol)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<HashSet<_>>();

        Ok(trading_symbols)
    }

    async fn is_futures_symbol(&self, symbol: &str) -> bool {
        let lock = self.futures_symbols.read().await;

        if lock.contains(symbol) {
            return true;
        }

        false
    }

    async fn get_market_volumes_statistics(&self, symbol: &str) -> Vec<VolumeChange> {
        self.binance
            .get_candlesticks_market_volume(symbol)
            .await
            .map(|data| calculate_volume_changes(data))
            .unwrap_or_else(|e| {
                error!("Failed to get klines data for {}: {}", symbol, e);
                Vec::new()
            })
    }

    async fn get_spot_daily_volume(&self, symbol: &str) -> Option<BinanceDailyVolume> {
        match self.binance.get_spot_daily_volume(symbol).await {
            Ok(volume) => Some(volume),
            Err(e) => {
                error!("Failed to get spot daily volume for {}: {}", symbol, e);
                None
            }
        }
    }

    async fn get_funding_rate(&self, symbol: &str) -> Option<FundingRateReport> {
        match self.binance.get_funding_rate(symbol).await {
            Ok(rate) => {
                let diff = rate.next_funding_time - Utc::now();
                let diff = TimeDifference::calculate(diff.num_minutes());

                Some(FundingRateReport {
                    funding_rate: rate.last_funding_rate.trunc_with_scale(5).normalize(),
                    next_funding_time: diff,
                })
            }
            Err(e) => {
                error!("Failed to get funding rate for {}: {}", symbol, e);
                None
            }
        }
    }

    async fn get_open_interest_statistics(&self, symbol: &str) -> Vec<OpenInterestChange> {
        self.binance
            .get_open_interest(symbol)
            .await
            .map(|data| calculate_open_interest_changes(data))
            .unwrap_or_else(|e| {
                error!("Failed to get OI for {}: {}", symbol, e);
                Vec::new()
            })
    }

    async fn get_long_short_ratio_statistics(&self, symbol: &str) -> Vec<LongShortRatioReport> {
        self.binance
            .get_long_short_ratio(symbol)
            .await
            .map(|data| get_long_short_ratios(data))
            .unwrap_or_else(|e| {
                error!(
                    "Failed to get long short positions ratio for {}: {}",
                    symbol, e
                );
                Vec::new()
            })
    }

    async fn build_spot_report(&self, symbol: &str) -> SpotReport {
        let daily_volume = self.get_spot_daily_volume(symbol).await;
        let volume_change = self.get_market_volumes_statistics(symbol).await;

        SpotReport {
            daily_volume,
            volume_change,
        }
    }

    async fn build_futures_report(&self, symbol: &str) -> Option<FuturesReport> {
        if !self.is_futures_symbol(symbol).await {
            return None;
        }

        let funding_rate = self.get_funding_rate(symbol).await;
        let long_short_ratio = self.get_long_short_ratio_statistics(symbol).await;
        let open_interest = self.get_open_interest_statistics(symbol).await;

        Some(FuturesReport {
            funding_rate,
            long_short_ratio,
            open_interest,
        })
    }

    fn build_margin_data_report(&self, margin_update: MarginDataUpdated) -> MarginDataReport {
        MarginDataReport {
            total_borrow: margin_update.new.total_borrow,
            total_borrow_usdt: margin_update.new.total_borrow_in_usdt,
            total_repay: margin_update.new.total_repay,
            total_repay_usdt: margin_update.new.total_borrow_in_usdt,
            borrow_change: margin_update.borrow_change(),
            repay_change: margin_update.repay_change(),
            br_ratio: margin_update.borrow_repay_ratio(),
            available: margin_update.new.available,
        }
    }

    pub async fn build_report(&self, margin_update: MarginDataUpdated) -> Report {
        let symbol = margin_update.new.asset.clone();
        let pair = format!("{}USDT", symbol);

        let margin_data = self.build_margin_data_report(margin_update);
        let spot = self.build_spot_report(&pair).await;
        let futures = self.build_futures_report(&pair).await;

        Report {
            symbol,
            margin_data,
            spot,
            futures,
        }
    }
}

pub async fn periodic_futures_pairs_update(collector: Arc<ReportCollector>) {
    let mut interval = interval(EXCHANGE_INFO_UPDATE_INTERVAL);
    info!("Updating exchange info Binance Futures");

    loop {
        interval.tick().await;

        match collector.get_futures_exchange_info_pairs().await {
            Err(e) => error!("Failed to update exchange info Binance Futures: {}", e),
            Ok(data) => {
                let mut lock = collector.futures_symbols.write().await;
                lock.extend(data);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::utils::{candles_fixture, open_interest_fixture, position_ratio_fixture};

    use super::*;

    // Checks that Interval::index returns valid index and
    // expected data can be retrieved from the vector
    #[test]
    fn test_check_index() {
        let oi = open_interest_fixture();

        let now = Interval::Now.index();
        let result = oi.get(now).unwrap().datetime.to_string();
        let expected = "2025-10-17 05:40:00 UTC";
        assert_eq!(result, expected);

        let m5 = Interval::M5.index();
        let result = oi.get(m5).unwrap().datetime.to_string();
        let expected = "2025-10-17 05:45:00 UTC";
        assert_eq!(result, expected);

        let m15 = Interval::M15.index();
        let result = oi.get(m15).unwrap().datetime.to_string();
        let expected = "2025-10-17 05:55:00 UTC";
        assert_eq!(result, expected);

        let h1 = Interval::H1.index();
        let result = oi.get(h1).unwrap().datetime.to_string();
        let expected = "2025-10-17 06:40:00 UTC";
        assert_eq!(result, expected);

        let h4 = Interval::H4.index();
        let result = oi.get(h4).unwrap().datetime.to_string();
        let expected = "2025-10-17 09:40:00 UTC";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_calculate_volume_changes() {
        let candles = candles_fixture();
        let result = calculate_volume_changes(candles);

        let expected = vec![
            VolumeChange {
                interval: Interval::M5,
                sell: Decimal::new(-3404, 2),
                buy: Decimal::new(-651, 2),
            },
            VolumeChange {
                interval: Interval::M15,
                sell: Decimal::new(-803, 1),
                buy: Decimal::new(-7464, 2),
            },
            VolumeChange {
                interval: Interval::H1,
                sell: Decimal::new(18224, 2),
                buy: Decimal::new(7269, 2),
            },
            VolumeChange {
                interval: Interval::H4,
                sell: Decimal::new(6144, 2),
                buy: Decimal::new(-4944, 2),
            },
        ];

        assert_eq!(result, expected);
    }

    #[test]
    fn test_get_long_short_ratios() {
        let ratios = position_ratio_fixture();
        let result = get_long_short_ratios(ratios);

        let expected = vec![
            LongShortRatioReport {
                interval: Interval::Now,
                ratio: Decimal::new(381, 2)
            },
            LongShortRatioReport {
                interval: Interval::M5,
                ratio: Decimal::new(381, 2)
            },
            LongShortRatioReport {
                interval: Interval::M15,
                ratio: Decimal::new(378, 2)
            },
            LongShortRatioReport {
                interval: Interval::H1,
                ratio: Decimal::new(359, 2)
            },
            LongShortRatioReport {
                interval: Interval::H4,
                ratio: Decimal::new(402, 2)
            },
        ];

        assert_eq!(result, expected);
    }

    #[test]
    fn test_calculate_open_interest_changes() {
        let oi = open_interest_fixture();
        let result = calculate_open_interest_changes(oi);

        let expected = vec![
            OpenInterestChange {
                interval: Interval::M5,
                change: Decimal::new(-193, 2)
            },
            OpenInterestChange {
                interval: Interval::M15,
                change: Decimal::new(-3, 2)
            },
            OpenInterestChange {
                interval: Interval::H1,
                change: Decimal::new(122, 2)
            },
            OpenInterestChange {
                interval: Interval::H4,
                change: Decimal::new(145, 2)
            },
        ];

        assert_eq!(result, expected);
    }
}
