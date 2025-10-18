use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use log::{error, info};
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio::time::interval;

use crate::binance::Binance;
use crate::structs::{MarginData, MarginDataUpdated};
use crate::redis::Redis;
use crate::structs::MarginDataMessage;

const REQUEST_INTERVAL: Duration = Duration::from_secs(300);

pub struct MarginDataProcessor {
    margin_data: Mutex<HashMap<String, MarginData>>,
    redis: Arc<Redis>,
    binance: Binance,
    report_tx: Sender<MarginDataMessage>,
}

impl MarginDataProcessor {
    pub fn new(
        redis: Arc<Redis>,
        binance: Binance,
        report_tx: Sender<MarginDataMessage>
    ) -> Self {
        Self {
            margin_data: Mutex::new(HashMap::new()),
            redis,
            binance,
            report_tx,
        }
    }

    pub async fn load(
        redis: Arc<Redis>,
        binance: Binance,
        report_tx: Sender<MarginDataMessage>
    ) -> Self {
        let processor = Self::new(redis, binance, report_tx);
        let redis_data = processor.redis.get_all_margin_data().await.expect("Failed to get margin data from redis");

        let redis_data = if redis_data.is_empty() {
            info!("Very first launch. Requesting data from binance and saving it to redis");
            let margin_data = processor.binance.get_margin_data_filtered().await.expect("Failed to get binance data");
            processor.redis.set_margin_data_bulk(margin_data.clone()).await.expect("Failed to save first data to redis");
            margin_data
        } else {
            redis_data
        };

        {
            let mut margin_data = processor.margin_data.lock().await;
            let redis_data = redis_data.into_iter().map(|item| (item.asset.clone(), item)).collect();
            *margin_data = redis_data
        }

        info!("Loaded data from redis to margin data processor");
        processor
    }
}

pub async fn margin_data_processor(
    redis: Arc<Redis>,
    binance: Binance,
    report_tx: Sender<MarginDataMessage>
) {
    let processor = MarginDataProcessor::load(redis, binance, report_tx).await;
    info!("Starting margin data processor...");

    let mut interval = interval(REQUEST_INTERVAL);

    loop {
        interval.tick().await;

        let Ok(latest_binance_resp) = processor.binance.get_margin_data_filtered().await else {
            let msg = "Error while requesting binance data. Check logs";
            processor.report_tx.send(MarginDataMessage::Error(msg.to_string())).await.unwrap();
            continue;
        };

        let previous_resp_data = {
            let lock = processor.margin_data.lock().await;
            lock.clone()
        };

        let mut next_redis_updates = Vec::new();

        for latest_resp_item in latest_binance_resp {
            match previous_resp_data.get(&latest_resp_item.asset) {
                None => {
                    next_redis_updates.push(latest_resp_item.clone());
                    processor.report_tx.send(MarginDataMessage::New(latest_resp_item)).await.unwrap();
                },
                Some(previous_item) => {
                    if previous_item != &latest_resp_item {
                        next_redis_updates.push(latest_resp_item.clone());

                        let updated = MarginDataUpdated {
                            old: previous_item.clone(),
                            new: latest_resp_item
                        };

                        processor.report_tx.send(MarginDataMessage::Update(updated)).await.unwrap();
                    }
                }
            }
        }

        if !next_redis_updates.is_empty() {
            match processor.redis.set_margin_data_bulk(next_redis_updates.clone()).await {
                Ok(_) => {
                    let updates: HashMap<_, _> = next_redis_updates
                        .into_iter()
                        .map(|item| (item.asset.clone(), item))
                        .collect();

                    let mut redis_data = processor.margin_data.lock().await;
                    redis_data.extend(updates);
                },
                Err(e) => {
                    let msg = format!("Failed to save updates to redis: {}", e);
                    error!("{}", msg);
                    processor.report_tx.send(MarginDataMessage::Error(msg)).await.unwrap();
                }
            }
        }
    }
}
