use log::error;
use numfmt::Formatter;
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use rust_decimal::prelude::ToPrimitive;

use crate::binance::BinanceDailyVolume;
use crate::config::TelegramConfig;
use crate::report::{
    FundingRateReport,
    FuturesReport,
    LongShortRatioReport,
    MarginDataReport,
    OpenInterestChange,
    Report,
    SpotReport,
    AggregatedVolume
};
use crate::structs::{MarginData, TimeDifference};

const TELEGRAM_API: &str = "https://api.telegram.org";
const MARKDOVWN2_ESCAPE_SYMBOLS: &str = r#"\\[]()~>#\+-={}.!""#;
const MARKDOVWN2_SYMBOLS: &str = r#"*_"#;

fn format_number(f: &mut Formatter, num: Decimal) -> String {
    let num = num.trunc_with_scale(2).normalize();
    let num = num.to_f64().unwrap();
    let num = f.fmt2(num);
    num.to_owned()
}

fn format_change(f: &mut Formatter, num: Decimal) -> String {
    let str_num = format_number(f, num);

    if num.is_sign_positive() {
        return format!("+{}", str_num)
    }

    str_num
}

fn set_emoji(num: Decimal) -> String {
    if num.is_sign_positive() {
        "🔺".to_string()
    } else {
        "🔻".to_string()
    }
}

fn dollar_formatter() -> Formatter {
    Formatter::new()
        .prefix("$").unwrap()
        .separator(',').unwrap()
        .scales(numfmt::Scales::short())
        .precision(numfmt::Precision::Decimals(2))
}

fn format_spot_report(data: SpotReport) -> String {
    let mut msg = "💸 *Spot*\n\n".to_string();

    let daily_vol = format_daily_volume_report(data.daily_volume);
    msg.push_str(&daily_vol);
    msg.push_str("\n");

    let volumes = format_spot_volume_report(data.volume_change);
    msg.push_str(&volumes);

    msg
}

fn format_spot_volume_report(data: Vec<AggregatedVolume>) -> String {
    if data.is_empty() {
        return "Trading volumes: no data".to_string()
    };

    let mut sell_msg = "🔴 Sell: ".to_string();
    let mut buy_msg = "🟢 Buy: ".to_string();
    let mut ratio_msg = "⚖️ Buy sell ratios: ".to_string();
    let mut f = Formatter::default();

    for item in data {
        let sell = format!("• _{}_ *{}* ", item.interval, format_number(&mut f, item.sell));
        let buy = format!("• _{}_ *{}* ", item.interval, format_number(&mut f, item.buy));
        let ratio = format!("• _{}_ *{}* ", item.interval, format_number(&mut f, item.buy_sell_ratio));

        sell_msg.push_str(&sell);
        buy_msg.push_str(&buy);
        ratio_msg.push_str(&ratio);
    }

    format!("{}\n{}\n{}", buy_msg, sell_msg, ratio_msg)
}

fn format_daily_volume_report(data: Option<BinanceDailyVolume>) -> String {
    let mut msg = "💰 24h volume: ".to_string();

    let Some(report) = data else {
        return "no data".to_string()
    };

    let mut f = Formatter::default();
    let mut f_doll = dollar_formatter();

    let doll_vol = format_number(&mut f_doll, report.quote_volume);
    let vol = format_number(&mut f, report.volume);

    let symbol = report.symbol.strip_suffix("USDT").unwrap_or(&report.symbol);
    let vol_msg = format!("*{}* ({} {})", doll_vol, vol, symbol);

    msg.push_str(&vol_msg);
    msg
}

fn format_futures_report(data: Option<FuturesReport>) -> String {
    let mut msg = "💸 *Futures*".to_string();

    let Some(report) = data else {
        msg.push_str(" not presented");
        return msg
    };

    let funding = funding_rate_report(report.funding_rate);
    msg.push_str("\n\n");
    msg.push_str(&funding);

    let open_interest = open_interest_report(report.open_interest);
    msg.push_str("\n");
    msg.push_str(&open_interest);

    let long_short_ratio = long_short_ratio_report(report.long_short_ratio);
    msg.push_str("\n");
    msg.push_str(&long_short_ratio);

    msg
}

fn funding_rate_report(data: Option<FundingRateReport>) -> String {
    let Some(report) = data else {
        return "Funding rate: no data".to_string()
    };

    format!(
        "⏳ Funding rate *{}* in *{}*",
        report.funding_rate, report.next_funding_time.to_string()
    )
}

fn long_short_ratio_report(data: Vec<LongShortRatioReport>) -> String {
    let mut msg = "⚖️ Long short ratios: ".to_string();

    if data.is_empty() {
        msg.push_str("no data");
        return msg
    };

    let mut f = Formatter::default();
    for ratio in data {
        let ratio_msg = format!("• _{}_ *{}* ", ratio.interval, format_number(&mut f, ratio.ratio));
        msg.push_str(&ratio_msg);
    }

    msg
}

fn open_interest_report(data: Vec<OpenInterestChange>) -> String {
    let mut msg = "💣 OI: ".to_string();

    if data.is_empty() {
        msg.push_str("no data");
        return msg
    };

    let mut f = Formatter::default();
    for oi in data {
        let ratio_msg = format!("• _{}_ *{}*% ", oi.interval, format_change(&mut f, oi.change));
        msg.push_str(&ratio_msg);
    }

    msg
}

fn format_margin_report_message(symbol: &str, data: MarginDataReport) -> String {
    let mut f = Formatter::default();
    let mut f_dol = dollar_formatter();

    let mut msg = format!("#*{}*", symbol);

    let total_borrow_usdt = format_number(&mut f_dol, data.total_borrow_usdt);
    let total_borrow = format_number(&mut f, data.total_borrow);
    let borrow_change = format_change(&mut f, data.borrow_change);
    let emoji = set_emoji(data.borrow_change);

    let borrow_str = format!(
        "\n\n{} Borrowed *{}* ({} {}) {}%",
        emoji, total_borrow_usdt, total_borrow, symbol, borrow_change
    );
    msg.push_str(&borrow_str);

    let total_repay_usdt = format_number(&mut f_dol, data.total_repay_usdt);
    let total_repay = format_number(&mut f, data.total_repay);
    let repay_change = format_change(&mut f, data.repay_change);
    let emoji = set_emoji(data.repay_change);

    let repay_str = format!(
        "\n{} Repayed *{}* ({} {}) {}%",
        emoji, total_repay_usdt, total_repay, symbol, repay_change
    );
    msg.push_str(&repay_str);

    let ratio_str = format!("\n\n⚖️ B/R ratio *{}*", format_number(&mut f, data.br_ratio));
    msg.push_str(&ratio_str);

    let available = format_number(&mut f, data.available);
    let available_string = format!("\n🏦 Available *{}* {}", available, symbol);
    msg.push_str(&available_string);

    msg
}

pub fn format_new_margin_data_message(data: MarginData) -> String {
    format!("#*{}* 🆕\n\n#new", data.asset)
}

pub fn format_full_report(report: Report, updated: TimeDifference) -> String {
    let margin = format_margin_report_message(&report.symbol, report.margin_data);
    let spot = format_spot_report(report.spot);
    let futures = format_futures_report(report.futures);

    let mut msg = format!("{}\n\n{}\n\n{}\n\nLast signal: ", margin, spot, futures);

    if updated.is_none() {
        msg.push_str("never");
    } else {
        msg.push_str(&format!("{}ago", updated));
    }

    msg
}

pub struct Telegram {
    token: String,
    chat: String,
    error_channel: String,
    client: Client,
}

impl Telegram {
    pub fn new(client: Client, config: TelegramConfig) -> Self {
        Self {
            token: config.token,
            chat: config.chat_id,
            error_channel: config.error_channel,
            client,
        }
    }

    fn url(&self) -> String {
        format!("{}/bot{}/sendMessage", TELEGRAM_API, self.token)
    }

    fn escape_markdown_v2(&self, text: &str) -> String {
        text.chars().fold(String::with_capacity(text.len()), |mut acc, char| {
            if MARKDOVWN2_ESCAPE_SYMBOLS.contains(char) && !MARKDOVWN2_SYMBOLS.contains(char) {
                acc.push('\\');
            }
            acc.push(char);
            acc
        })
    }

    fn message(&self, text: &str) -> Value {
        let text = self.escape_markdown_v2(text);

        json!({
            "chat_id": self.chat,
            "text": text,
            "parse_mode": "MarkdownV2"
        })
    }

    fn error_message(&self, text: String) -> Value {
        json!({
            "chat_id": self.error_channel,
            "text": text,
        })
    }

    pub async fn send_error_message(&self, err: String) {
        let url = self.url();
        let msg = self.error_message(err);

        let result = self.client.post(url).json(&msg).send().await;

        if let Err(e) = result {
            error!("Failed to send message to telegram: {:?}, message text: {}", e, msg);
        }
    }

    pub async fn send_message(&self, event: &str) {
        let url = self.url();
        let msg = self.message(event);

        let result = self.client.post(url).json(&msg).send().await;

        if let Err(e) = result {
            error!("Failed to send message to telegram: {:?}, message text: {}", e, msg);
        }
    }
}
