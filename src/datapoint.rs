use chrono::{DateTime, NaiveDateTime, Utc};
use eyre::{OptionExt, Result};

use crate::coin::Coin;

#[derive(Clone, Debug)]
pub struct Datapoint {
	pub price: Option<f64>,
	pub datetime: DateTime<Utc>,
	pub coin: Coin,
}

#[derive(Clone, Debug, Copy)]
pub enum TimeType {
	DateTime(DateTime<Utc>),
	Timestamp(i64),
}

impl Datapoint {
	pub fn new(price: Option<f64>, time: TimeType, coin: Coin) -> Result<Self> {
		let utc_datetime: DateTime<Utc> = match time {
			TimeType::DateTime(datetime) => datetime,
			TimeType::Timestamp(timestamp) => NaiveDateTime::from_timestamp_opt(timestamp, 0)
				.ok_or_eyre("timestamp did not convert to NaiveDateTime propperly")?
				.and_utc(),
		};

		Ok(Self {
			price,
			datetime: utc_datetime,
			coin,
		})
	}
}
