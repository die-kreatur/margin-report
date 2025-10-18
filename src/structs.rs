use numfmt::Numeric;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::utils::find_percentage_diff;

pub enum MarginDataMessage {
    Error(String),
    Update(MarginDataUpdated),
    New(MarginData),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimeDifference {
    days: i64,
    hours: i64,
    minutes: i64,
}

impl TimeDifference {
    // Accepts time delta in minutes
    pub fn calculate(min_diff: i64) -> Self {
        let (total_hours, total_minutes) = (min_diff / 60, min_diff % 60);
        let (total_days, total_hours) = (total_hours / 24, total_hours % 24);

        TimeDifference {
            days: total_days,
            hours: total_hours,
            minutes: total_minutes
        }
    }

    pub fn is_none(&self) -> bool {
        self.days.is_zero() && self.hours.is_zero() && self.minutes.is_zero()
    }
}

impl std::fmt::Display for TimeDifference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut time_str = String::new();

        if self.days > 0 {
            let days = format!("{}d ", self.days);
            time_str.push_str(&days);
        }

        if self.hours > 0 {
            let hours_and_mins = format!("{}h ", self.hours);
            time_str.push_str(&hours_and_mins);
        }

        if self.minutes > 0 {
            let minutes = format!("{}min ", self.minutes);
            time_str.push_str(&minutes);
        }

        write!(f, "{}", time_str)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct MarginData {
    pub asset: String,
    pub total_borrow: Decimal,
    pub total_repay: Decimal,
    pub total_borrow_in_usdt: Decimal,
    pub total_repay_in_usdt: Decimal,
    pub available: Decimal,
}

#[cfg(test)]
impl Default for MarginData {
    fn default() -> Self {
        Self {
            asset: "SOL".to_string(),
            total_borrow: Decimal::ONE,
            total_repay: Decimal::TEN,
            total_borrow_in_usdt: Decimal::ONE_THOUSAND,
            total_repay_in_usdt: Decimal::ONE_HUNDRED,
            available: Decimal::TEN,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarginDataUpdated {
    pub old: MarginData,
    pub new: MarginData,
}

impl MarginDataUpdated {
    pub fn is_more_than_1m(&self) -> bool {
        self.new.total_borrow_in_usdt >= Decimal::from(1_000_000)
    }

    pub fn borrow_change(&self) -> Decimal {
        find_percentage_diff(self.new.total_borrow, self.old.total_borrow)
    }

    pub fn repay_change(&self) -> Decimal {
        find_percentage_diff(self.new.total_repay, self.old.total_repay)
    }

    pub fn borrow_repay_ratio(&self) -> Decimal {
        self.new.total_borrow / self.new.total_repay
    }

    pub fn is_percent_changed_enough(&self) -> bool {
        self.borrow_change() >= Decimal::TEN
    }

    pub fn is_borrowing_rapidly_increased(&self) -> bool {
        self.borrow_change() >= Decimal::ONE_THOUSAND
    }

    pub fn is_borrow_big_enough(&self) -> bool {
        self.new.total_borrow / self.new.total_repay > Decimal::from(5)
    }
}
