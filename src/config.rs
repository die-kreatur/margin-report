use std::fs;

use serde::Deserialize;

use crate::error::ServiceError;

const CONFIG_PATH: &str = "./config.json";

#[derive(Debug, Deserialize)]
pub struct TelegramConfig {
    pub token: String,
    pub chat_id: String,
    pub error_channel: String,
}

#[derive(Debug, Deserialize)]
pub struct ServiceConfig {
    pub telegram: TelegramConfig,
    pub redis_url: String,
}

pub fn read_from_file() -> Result<ServiceConfig, ServiceError> {
    let file = fs::read(CONFIG_PATH)?;
    serde_json::from_slice::<ServiceConfig>(&file).map_err(ServiceError::from)
}
