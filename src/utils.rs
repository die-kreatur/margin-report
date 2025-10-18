use chrono::{DateTime, Duration, SubsecRound, Timelike, Utc};
use numfmt::Numeric;
use rust_decimal::Decimal;

#[cfg(test)]
use crate::binance::{
    BinanceCandleMarketTradeVolume,
    BinanceLongShortRatioPositions,
    BinanceOpenInterest,
};
#[cfg(test)]
use std::fs;

// For simplicity I assume that if the old value is 0, the change is 100%
pub fn find_percentage_diff(new: Decimal, old: Decimal) -> Decimal {
    let result = (new - old)
        .checked_div(old)
        .map(|val| val * Decimal::ONE_HUNDRED)
        .unwrap_or(Decimal::ONE_HUNDRED);

    result.trunc_with_scale(2).normalize()
}

// Some data (open interest statistics or long short positions ratio) can be
// returned only for 5 minutes intervals aligned to 5 minutes boundaries.
// Thus, if request was sent at 17:47, the most recent data would be for 17:40-17:45
// period. If we want to get the most recent data, we have to take that into account
// and adjust service start time.
pub fn calculate_delay_secs() -> u64 {
    let now = Utc::now().trunc_subsecs(0);
    let next = get_time_slot(now);

    (next - now)
        .num_seconds()
        .try_into()
        .expect("Failed to get number of delay seconds")
}

fn get_time_slot(date: DateTime<Utc>) -> DateTime<Utc> {
    let current_minute = date.minute();
    let left_until_5_min_interval = current_minute % 5;

    let until_next_interval = if left_until_5_min_interval.is_zero() {
        left_until_5_min_interval
    } else {
        5 - left_until_5_min_interval
    };

    // Add one extra minute since the data may still be processing at the
    // end of the 5-minute window and the most recent data cannot be returned
    let delay = until_next_interval + 1;

    // Remove current second to start at 00 seconds
    let delay = delay * 60 - date.second();
    date + Duration::seconds(delay.into())
}

#[cfg(test)]
pub fn candles_fixture() -> Vec<BinanceCandleMarketTradeVolume> {
    let file = fs::read("./test_fixtures/candles.json").unwrap();
    let result = serde_json::from_slice::<Vec<Vec<Decimal>>>(&file).unwrap();

    result
        .into_iter()
        .map(BinanceCandleMarketTradeVolume::from)
        .collect()
}

#[cfg(test)]
pub fn position_ratio_fixture() -> Vec<BinanceLongShortRatioPositions> {
    let file = fs::read("./test_fixtures/long_short_ratio.json").unwrap();
    serde_json::from_slice(&file).unwrap()
}

#[cfg(test)]
pub fn open_interest_fixture() -> Vec<BinanceOpenInterest> {
    let file = fs::read("./test_fixtures/open_interest.json").unwrap();
    serde_json::from_slice(&file).unwrap()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_calculate_delay() {
        let date = "2025-10-16T18:11:06Z".parse::<DateTime<Utc>>().unwrap();
        let result = get_time_slot(date).to_string();
        let expected = "2025-10-16 18:16:00 UTC";
        assert_eq!(result, expected);

        let date = "2025-10-16T18:01:55Z".parse::<DateTime<Utc>>().unwrap();
        let result = get_time_slot(date).to_string();
        let expected = "2025-10-16 18:06:00 UTC";
        assert_eq!(result, expected);

        let date = "2025-10-16T18:23:34Z".parse::<DateTime<Utc>>().unwrap();
        let result = get_time_slot(date).to_string();
        let expected = "2025-10-16 18:26:00 UTC";
        assert_eq!(result, expected);

        let date = "2025-10-16T18:00:01Z".parse::<DateTime<Utc>>().unwrap();
        let result = get_time_slot(date).to_string();
        let expected = "2025-10-16 18:01:00 UTC";
        assert_eq!(result, expected);

        let date = "2025-10-16T18:55:17Z".parse::<DateTime<Utc>>().unwrap();
        let result = get_time_slot(date).to_string();
        let expected = "2025-10-16 18:56:00 UTC";
        assert_eq!(result, expected);

        let date = "2025-10-16T18:59:59Z".parse::<DateTime<Utc>>().unwrap();
        let result = get_time_slot(date).to_string();
        let expected = "2025-10-16 19:01:00 UTC";
        assert_eq!(result, expected);

        let date = "2025-10-16T23:59:20Z".parse::<DateTime<Utc>>().unwrap();
        let result = get_time_slot(date).to_string();
        let expected = "2025-10-17 00:01:00 UTC";
        assert_eq!(result, expected);
    }
}
