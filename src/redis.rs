use chrono::{DateTime, Utc};
use log::error;
use redis::{AsyncCommands, Client};

use crate::error::{Result, ServiceError};
use crate::structs::MarginData;

fn deserialize_redis_data(entries: Vec<String>) -> Result<Vec<MarginData>> {
    let mut deserialized_data = Vec::with_capacity(entries.len());

    for entry in entries {
        let deserialized_entry = match serde_json::from_str(&entry) {
            Ok(value) => value,
            Err(err) => return Err(ServiceError::internal(err.to_string())),
        };

        deserialized_data.push(deserialized_entry);
    }

    Ok(deserialized_data)
}

pub struct Redis {
    client: Client,
}

impl Redis {
    pub fn new(url: String) -> Self {
        let client = Client::open(url).expect("Failed to connect to redis");

        Self { client }
    }

    fn margin_data_key(&self, symbol: &str) -> String {
        format!("margin-data-{}", symbol)
    }

    fn last_update_key(&self, symbol: &str) -> String {
        format!("last-update-{}", symbol)
    }

    pub async fn set_margin_data_bulk(&self, data: Vec<MarginData>) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        let data = data
            .into_iter()
            .map(|item| {
                let key = self.margin_data_key(&item.asset);
                let val = serde_json::to_string(&item).unwrap();

                (key, val)
            })
            .collect::<Vec<_>>();

        let _: () = conn.mset(&data).await?;
        Ok(())
    }

    pub async fn get_all_margin_data(&self) -> Result<Vec<MarginData>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = conn.keys("margin-data-*").await?;

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let result = conn.mget(keys).await?;
        deserialize_redis_data(result)
    }

    pub async fn set_last_update(&self, symbol: &str, last_update: DateTime<Utc>) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.last_update_key(symbol);
        let value = last_update.to_string();
        let _: () = conn.set(key, value).await?;
        Ok(())
    }

    pub async fn get_last_update(&self, symbol: &str) -> Result<Option<DateTime<Utc>>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.last_update_key(symbol);
        let result: Option<String> = conn.get(key).await?;

        let result = result.and_then(|dt| {
            dt.parse::<DateTime<Utc>>()
                .map_err(|e| {
                    error!("Failed to parse redis datetime: {}", e);
                }
            )
            .ok()
        });

        Ok(result)
    }
}
