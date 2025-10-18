use std::sync::Arc;

use chrono::{DateTime, Utc};
use log::{error, info};
use tokio::sync::mpsc::Receiver;

use crate::binance::Binance;
use crate::redis::Redis;
use crate::report::ReportCollector;
use crate::structs::{MarginDataMessage, MarginDataUpdated, TimeDifference};
use crate::telegram::{format_full_report, format_new_margin_data_message, Telegram};

pub struct ReportProcessor {
    report: ReportCollector,
    redis: Arc<Redis>,
    tg: Telegram,
}

impl ReportProcessor {
    pub fn new(binance: Binance, redis: Arc<Redis>, tg: Telegram) -> Self {
        let report = ReportCollector::new(binance);
        Self { report, redis, tg }
    }

    async fn get_last_update_time(&self, symbol: &str) -> DateTime<Utc> {
        self
            .redis
            .get_last_update(symbol)
            .await
            .map_err(|e| error!("Failed to get last update time for {}: {}", symbol, e))
            .ok()
            .flatten()
            .unwrap_or(Utc::now())
    }

    async fn save_last_update_time(&self, symbol: &str, updated_at: DateTime<Utc>) {
        self
            .redis
            .set_last_update(symbol, updated_at)
            .await
            .map_err(|e| error!("Failed to save last update time for {}: {}", symbol, e))
            .ok();
    }

    async fn process_margin_data_update(&self, update: MarginDataUpdated) {
        let condition_1m = update.is_more_than_1m() && update.is_percent_changed_enough();

        if (update.is_borrowing_rapidly_increased() || condition_1m) && update.is_borrow_big_enough() {
            let asset = update.new.asset.clone();
            let now = Utc::now();

            let last_update = self.get_last_update_time(&asset).await;

            let min_diff = (now - last_update).num_minutes();
            let time_diff = TimeDifference::calculate(min_diff);

            info!("Building report for {}", asset);
            let report = self.report.build_report(update).await;
            let report = format_full_report(report, time_diff);

            self.tg.send_message(&report).await;
            self.save_last_update_time(&asset, now).await;
        }
    }
}

pub async fn process_new_reports(
    tg: Telegram,
    binance: Binance,
    mut report_rx: Receiver<MarginDataMessage>,
    redis: Arc<Redis>,
) {
    let processor = ReportProcessor::new(binance, redis, tg);

    while let Some(event) = report_rx.recv().await {
        match event {
            MarginDataMessage::Error(e) => processor.tg.send_error_message(e).await,
            MarginDataMessage::Update(update) => processor.process_margin_data_update(update).await,
            MarginDataMessage::New(data) => {
                let msg = format_new_margin_data_message(data);
                processor.tg.send_message(&msg).await
            }
        }
    }
}
