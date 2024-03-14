use chrono::{DateTime, Utc};
use eyre::Result;
use serde::{Deserialize, Serialize};

pub const KRAKEN_MAX_DATAPOINTS: u16 = 720;

#[derive(Deserialize, Copy, Clone, Debug)]
pub struct KrakenDatapoint {
	pub timestamp: i64,
	pub open: f32,
	pub high: f32,
	pub low: f32,
	pub close: f32,
	pub vwap: f32,
	pub volume: f32,
	pub count: u16,
}

#[derive(Deserialize, Copy, Clone, Debug)]
pub enum KrakenInterval {
	Minute = 1,
	FiveMinutes = 5,
	FifteenMinutes = 15,
	HalfHour = 30,
	Hour = 60,
	FourHours = 240,
	Day = 1440,
	Week = 10080,
	HalfMonth = 21600,
}

impl Default for KrakenInterval {
	fn default() -> Self {
		KrakenInterval::FiveMinutes
	}
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Datapoint {
	pub price: f64,
	pub timestamp: i64,
}

#[derive(Clone, Debug, Copy)]
pub enum TimeType {
	DateTime(DateTime<Utc>),
	Timestamp(i64),
}

impl Datapoint {
	pub fn new(price: f64, time: TimeType) -> Result<Self> {
		let timestamp = match time {
			TimeType::DateTime(datetime) => datetime.timestamp(),
			TimeType::Timestamp(timestamp) => timestamp,
		};

		Ok(Self { price, timestamp })
	}
}
