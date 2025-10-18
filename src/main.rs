use std::sync::Arc;
use std::time::Duration;

use log::{info, warn};
use reqwest::Client;
use tokio::{task, try_join};

use crate::binance::Binance;
use crate::margin_data::margin_data_processor;
use crate::config::read_from_file;
use crate::redis::Redis;
use crate::report::{periodic_futures_pairs_update, ReportCollector};
use crate::report_processor::process_new_reports;
use crate::structs::MarginDataMessage;
use crate::telegram::Telegram;
use crate::utils::calculate_delay_secs;

mod binance;
mod config;
mod error;
mod structs;
mod redis;
mod margin_data;
mod telegram;
mod report;
mod report_processor;
mod utils;

#[tokio::main]
async fn main() {
    env_logger::init();
    info!("Initialized logger");

    let config = read_from_file().expect("Failed to read config");
    info!("Loaded config");
    info!("Waiting for the next time slot...");

    let delay = calculate_delay_secs();
    let delay = Duration::from_secs(delay);

    tokio::time::sleep(delay).await;
    info!("Starting service");

    let (report_tx, report_rx) = tokio::sync::mpsc::channel(1024);

    let client = Client::new();
    let binance = Binance::new(client.clone());
    let tg = Telegram::new(client, config.telegram);
    let redis = Arc::new(Redis::new(config.redis_url));

    let report_collector = Arc::new(ReportCollector::new(binance.clone()));
    let exch_info_task = task::spawn(periodic_futures_pairs_update(report_collector.clone()));
    info!("Started task to update futures exchange info");

    let margin_data_task = task::spawn(margin_data_processor(redis.clone(), binance.clone(), report_tx.clone()));
    info!("Started task to check binance updates and save them to redis");

    let report_task = task::spawn(process_new_reports(tg, binance, report_rx, redis));

    if let Err(e) = try_join!(exch_info_task, margin_data_task, report_task) {
        report_tx.send(MarginDataMessage::Error(e.to_string())).await.unwrap();
        warn!("Something went wrong: {}", e)
    }
}
