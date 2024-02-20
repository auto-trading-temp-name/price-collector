use chrono::{DateTime, NaiveDateTime, Utc};
use eyre::{OptionExt, Result};

use serde::Deserialize;
use shared::coin::Pair;

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

#[derive(Clone, Debug)]
pub struct Datapoint {
	pub price: Option<f64>,
	pub datetime: DateTime<Utc>,
	pub pair: Pair,
}

#[derive(Clone, Debug, Copy)]
pub enum TimeType {
	DateTime(DateTime<Utc>),
	Timestamp(i64),
}

impl Datapoint {
	pub fn new(price: Option<f64>, time: TimeType, pair: Pair) -> Result<Self> {
		let utc_datetime: DateTime<Utc> = match time {
			TimeType::DateTime(datetime) => datetime,
			TimeType::Timestamp(timestamp) => NaiveDateTime::from_timestamp_opt(timestamp, 0)
				.ok_or_eyre("timestamp did not convert to NaiveDateTime")?
				.and_utc(),
		};

		Ok(Self {
			price,
			datetime: utc_datetime,
			pair,
		})
	}
}
