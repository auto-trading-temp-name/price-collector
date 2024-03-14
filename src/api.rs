use std::iter::zip;

use actix_web::{
	get,
	web::{Data, Path},
	HttpResponse, Responder,
};
use eyre::{OptionExt, Result};
use redis::{Client, Commands};
use serde::{Deserialize, Serialize};
use shared::coin::Pair;

use crate::{
	datapoint::{Datapoint, TimeType},
	CURRENT_CHAIN,
};

const MAX_DATAPOINTS: i16 = 720;

fn generate_pair(pair_string: String) -> Option<Pair> {
	return Some(Pair::usdc_weth(Some(CURRENT_CHAIN.into())));
	todo!()
}

fn get_prices(pair_string: String, client: &Client, amount: i16) -> Result<Vec<Datapoint>> {
	let amount_inverted = amount * -1;
	let pair = generate_pair(pair_string).ok_or_eyre("invalid pair")?;
	let mut connection = client.get_connection()?;

	let timestamps: Vec<i64> = connection.lrange(
		format!("{}:timestamps", pair.to_string()),
		amount_inverted.into(),
		-1,
	)?;

	let prices: Vec<f64> = connection.lrange(
		format!("{}:prices", pair.to_string()),
		amount_inverted.into(),
		-1,
	)?;

	let datapoints = zip(prices, timestamps)
		.map(|(price, timestamp)| Datapoint::new(price, TimeType::Timestamp(timestamp)))
		.filter_map(|x| x.ok())
		.collect();

	Ok(datapoints)
}

#[derive(Deserialize, Serialize)]
struct ErrorValue {
	error: String,
}

#[get("/prices/{pair}")]
async fn prices_wrapper(path: Path<String>, client: Data<Client>) -> impl Responder {
	let pair_string = path.into_inner();
	match get_prices(pair_string, client.as_ref(), MAX_DATAPOINTS) {
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
