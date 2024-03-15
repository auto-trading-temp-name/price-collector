use std::iter::zip;

use actix_web::{
	get,
	web::{Data, Path, Query},
	HttpResponse, Responder,
};
use eyre::{eyre, OptionExt, Result};
use redis::{Client, Commands};
use serde::{Deserialize, Serialize};
use shared::coin::Pair;

use crate::{
	datapoint::{Datapoint, TimeType},
	COLLECTION_INTERVAL, CURRENT_CHAIN,
};

const MAX_DATAPOINTS: u16 = 720;

fn get_prices(
	pair_string: String,
	client: &Client,
	mut amount: u16,
	interval: u16,
) -> Result<Vec<Datapoint>> {
	let collection_interval_minutes = (COLLECTION_INTERVAL.std_duration().as_secs() / 60) as u16;
	if interval < collection_interval_minutes {
		Err(eyre!("interval smaller than collection interval"))?;
	}

	if (interval % collection_interval_minutes) != 0 {
		Err(eyre!("interval does not fit into collection interval"))?;
	}

	let amount: i32 = (amount * (interval / collection_interval_minutes)) as i32 * -1;
	let pair =
		Pair::get_pair(pair_string.as_str(), Some(CURRENT_CHAIN.into())).ok_or_eyre("invalid pair")?;
	let mut connection = client.get_connection()?;

	let timestamps: Vec<i64> = connection.lrange(
		format!("{}:timestamps", pair.to_string()),
		amount as isize,
		-1,
	)?;

	let prices: Vec<f64> =
		connection.lrange(format!("{}:prices", pair.to_string()), amount as isize, -1)?;

	let datapoints = zip(prices, timestamps)
		.map(|(price, timestamp)| Datapoint::new(price, TimeType::Timestamp(timestamp)))
		.filter_map(|x| x.ok())
		.filter(|x| x.timestamp / 60 % interval as i64 == 0)
		.collect();

	Ok(datapoints)
}

#[derive(Deserialize, Serialize)]
struct ErrorValue {
	error: String,
}

#[derive(Deserialize)]
struct DatapointRequestInfo {
	interval: u16,
	amount: Option<u16>,
}

#[get("/prices/{pair}")]
async fn prices_wrapper(
	pair: Path<String>,
	interval: Query<DatapointRequestInfo>,
	client: Data<Client>,
) -> impl Responder {
	let pair = pair.into_inner();
	let DatapointRequestInfo { interval, amount } = interval.into_inner();
	match get_prices(
		pair,
		client.as_ref(),
		u16::min(MAX_DATAPOINTS, amount.unwrap_or(u16::MAX)),
		interval,
	) {
		Ok(prices) => HttpResponse::Ok().json(prices),
		Err(error) => HttpResponse::BadRequest().json(ErrorValue {
			error: error.to_string(),
		}),
	}
}

#[get("/olhc/{pair}")]
async fn olhc() -> impl Responder {
	HttpResponse::InternalServerError().body("todo!")
}
