use std::any::type_name;
use std::collections::HashMap;
use std::num::ParseFloatError;
use std::ops::Mul;
use std::str::FromStr;

use chrono::{prelude::*, Duration};
use eyre::{eyre, ContextCompat, OptionExt, Result};
use hhmmss::Hhmmss;
use redis::Commands;
use serde_json::Value;
use shared::coin::Pair;
use tracing::{debug, warn};

use crate::datapoint::{
	Datapoint, KrakenDatapoint, KrakenInterval, TimeType, KRAKEN_MAX_DATAPOINTS,
};
use crate::interpolate::interpolate_datapoints;
use crate::COLLECTION_INTERVAL;

fn convert_str_f32<T: FromStr<Err = ParseFloatError>>(value: &Value) -> Result<T> {
	let type_name = type_name::<T>();
	let value = value
		.as_str()
		.ok_or_eyre(format!("error converting value from str to {}", type_name))?;

	Ok(value.parse()?)
}

pub fn find_discrepancies(client: &redis::Client, pair: &Pair) -> Result<Vec<Datapoint>> {
	let mut connection = client.get_connection()?;
	debug!("redis connection established");

	let timestamp =
		connection.lindex::<String, i64>(format!("{}:timestamps", pair.to_string()), -1)?;

	let last_real_dt = NaiveDateTime::from_timestamp_opt(timestamp, 0)
		.ok_or_eyre("timestamp did not convert to NaiveDateTime")?
		.and_utc();
	let next_collection_dt = Utc::now()
		.trunc_subsecs(0)
		.with_second(0)
		.ok_or_eyre("could not set seconds to zero")?;

	let missing_points = ((next_collection_dt.timestamp() - last_real_dt.timestamp()) / 60) as i32;

	let discrepancies: Vec<Datapoint> = (0..missing_points)
		.map(|t| {
			Datapoint::new(
				None,
				TimeType::DateTime(last_real_dt + COLLECTION_INTERVAL.duration().mul(t + 1)),
				pair.clone(),
			)
		})
		.filter_map(|t| t.ok())
		.collect();

	Ok(discrepancies)
}

async fn fetch_kraken_datapoints(
	pair: &Pair,
	interval: &KrakenInterval,
) -> Result<Vec<KrakenDatapoint>> {
	let fallback_name = pair.2.clone().ok_or_eyre("no fallback name")?;
	let response = reqwest::get(format!(
		"https://api.kraken.com/0/public/OHLC?pair={}&interval={}",
		fallback_name, *interval as u16
	))
	.await?
	.json::<serde_json::Value>()
	.await?;

	let response_format_err_msg = "fallback response format was not correct";

	let errors = response
		.get("error")
		.wrap_err(response_format_err_msg)?
		.as_array()
		.wrap_err(response_format_err_msg)?
		.to_owned();

	if errors.len() > 0 {
		todo!("error handling for kraken tbd");
	}

	let datapoints: Vec<KrakenDatapoint> = response
		.get("result")
		.wrap_err(response_format_err_msg)?
		.get(fallback_name)
		.wrap_err(response_format_err_msg)?
		.as_array()
		.wrap_err(response_format_err_msg)?
		.iter()
		.map(|point| KrakenDatapoint {
			timestamp: point[0].as_i64().expect(response_format_err_msg),
			open: convert_str_f32(&point[1]).expect(response_format_err_msg),
			high: convert_str_f32(&point[2]).expect(response_format_err_msg),
			low: convert_str_f32(&point[3]).expect(response_format_err_msg),
			close: convert_str_f32(&point[4]).expect(response_format_err_msg),
			vwap: convert_str_f32(&point[5]).expect(response_format_err_msg),
			volume: convert_str_f32(&point[6]).expect(response_format_err_msg),
			count: point[7].as_u64().expect(response_format_err_msg) as u16,
		})
		.collect();

	Ok(datapoints)
}

pub async fn fix_discrepancies(pair: &Pair, datapoints: Vec<Datapoint>) -> Result<Vec<Datapoint>> {
	let discrepancies = datapoints
		.into_iter()
		.filter(|d| d.price.is_none())
		.collect::<Vec<Datapoint>>();

	if discrepancies.len() < 1 {
		return Err(eyre!("no discrepancies"));
	}

	let outage_time = COLLECTION_INTERVAL
		.std_duration()
		.checked_mul(discrepancies.len() as u32)
		.ok_or_eyre("outage time calcuation overflowed")?;
	let outage_time_minutes = (outage_time.as_secs_f32() / 60.0).floor() as u32;

	let interval = KrakenInterval::default();
	let max_minutes_in_interval = interval as u16 * KRAKEN_MAX_DATAPOINTS as u16;

	debug!("outage was {:?} long", outage_time.hhmmss());
	debug!(
		"max datapoints in interval is {:?}",
		Duration::minutes(max_minutes_in_interval as i64).hhmmss()
	);

	if outage_time_minutes as u32 >= max_minutes_in_interval as u32 {
		warn!("all discrepancies will not be able to be fixed",);
		// @TODO use larger intervals if app goes out for longer than max_minues_in_interval
		let _new_interval = KrakenInterval::FiveMinutes;
	}

	let fallback_datapoints = fetch_kraken_datapoints(pair, &interval).await?;

	let last_datapoint = fallback_datapoints.last();
	let fallback_datapoints: HashMap<i64, KrakenDatapoint> = fallback_datapoints
		.clone()
		.into_iter()
		.map(|d| (d.timestamp, d))
		.collect();

	debug!(
		last_timestamp = (*last_datapoint.unwrap()).timestamp,
		last_predicted_timestamp = (*discrepancies.last().unwrap()).datetime.timestamp(),
		datapoints = fallback_datapoints.len(),
		interval = interval as u16,
		"fetched datapoints"
	);

	let fixed_discrepancies: Vec<Datapoint> = discrepancies
		.iter()
		.map(|d| Datapoint {
			price: match d.price {
				None => {
					let fallback_datapoint = fallback_datapoints.get(&d.datetime.timestamp());
					match fallback_datapoint {
						Some(fallback_datapoint) => Some(fallback_datapoint.close as f64),
						_ => None,
					}
				}
				_ => d.price,
			},
			..d.to_owned()
		})
		.filter(|d| d.price.is_some())
		.collect();

	Ok(fixed_discrepancies)
}

pub async fn initialize_datapoints(client: &redis::Client, pair: &Pair) -> Result<Vec<Datapoint>> {
	let into_datapoint = |datapoint: &KrakenDatapoint| -> Result<Datapoint> {
		Ok(Datapoint::new(
			Some(datapoint.close as f64),
			TimeType::Timestamp(datapoint.timestamp),
			pair.clone(),
		)?)
	};

	let mut connection = client.get_connection()?;
	debug!("redis connection established");

	let fallback_interval = KrakenInterval::default();
	let extra_fallback_interval = KrakenInterval::Day;

	let fallback_datapoints: Vec<Datapoint> = fetch_kraken_datapoints(pair, &fallback_interval)
		.await?
		.iter()
		.map(into_datapoint)
		.filter_map(|x| x.ok())
		.collect();
	let extra_fallback_datapoints: Vec<Datapoint> = interpolate_datapoints(
		fetch_kraken_datapoints(pair, &extra_fallback_interval)
			.await?
			.iter()
			.map(into_datapoint)
			.filter_map(|x| x.ok())
			.collect(),
		&extra_fallback_interval,
		&fallback_interval,
	);

	let first_timestamp = connection
		.lindex(format!("{}:timestamps", pair.to_string()), -1)
		.unwrap_or(i64::MIN);

	let selected_datapoints: Vec<Datapoint> = [fallback_datapoints, extra_fallback_datapoints]
		.concat()
		.into_iter()
		.filter(|datapoint| datapoint.datetime.timestamp() > first_timestamp)
		.collect();

	Ok(selected_datapoints)
}
