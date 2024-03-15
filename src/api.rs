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

const MAX_DATAPOINTS: i16 = 720;

fn generate_pair(pair_string: String) -> Option<Pair> {
	return Some(Pair::usdc_weth(Some(CURRENT_CHAIN.into())));
	todo!()
}

fn get_prices(
	pair_string: String,
	client: &Client,
	mut amount: i16,
	interval: i16,
) -> Result<Vec<Datapoint>> {
	let collection_interval_minutes = (COLLECTION_INTERVAL.std_duration().as_secs() / 60) as i16;
	if interval < collection_interval_minutes {
		Err(eyre!("interval smaller than collection interval"))?;
	}

	if (interval % collection_interval_minutes) != 0 {
		Err(eyre!("interval does not fit into collection interval"))?;
	}

	amount = amount * (interval / collection_interval_minutes) * -1;
	let pair = generate_pair(pair_string).ok_or_eyre("invalid pair")?;
	let mut connection = client.get_connection()?;

	let timestamps: Vec<i64> = connection.lrange(
		format!("{}:timestamps", pair.to_string()),
		amount.into(),
		-1,
	)?;

	let prices: Vec<f64> =
		connection.lrange(format!("{}:prices", pair.to_string()), amount.into(), -1)?;

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
struct Interval {
	interval: i16,
}

#[get("/prices/{pair}")]
async fn prices_wrapper(
	pair: Path<String>,
	interval: Query<Interval>,
	client: Data<Client>,
) -> impl Responder {
	let pair = pair.into_inner();
	let Interval { interval } = interval.into_inner();
	match get_prices(pair, client.as_ref(), MAX_DATAPOINTS, interval) {
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
