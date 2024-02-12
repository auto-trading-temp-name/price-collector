use std::env;
use std::sync::Arc;

use ethers::prelude::*;
use eyre::Result;
use futures::future;
use redis::Commands;
use tracing::{debug, error, info, instrument, warn};

use crate::datapoint::Datapoint;
use shared::abis::Quoter;
use shared::coin::Coin;

pub async fn fetch_prices(
	provider: Arc<Provider<Http>>,
	base_coin: &Coin,
	coins: Vec<Coin>,
) -> Vec<(Coin, f64)> {
	let quoter = Arc::new(Quoter::new(
		env::var("QUOTER_ADDRESS")
			.expect("QUOTER_ADDRESS should be in .env")
			.parse::<Address>()
			.expect("QUOTER_ADDRESS should be a valid address"),
		provider,
	));

	let prices: Vec<f64> = future::join_all(coins.iter().map(|coin| {
		debug!(coin = ?coin, "fetched price");
		coin.get_price(base_coin, quoter.as_ref())
	}))
	.await
	.into_iter()
	.map(|result| match result {
		Ok(price) => Some(f64::from(price) / (f64::powi(10_f64, base_coin.decimals))),
		Err(error) => {
			warn!(error = ?error, "error getting price");
			None
		}
	})
	.filter_map(|x| x)
	.collect();

	coins.to_vec().into_iter().zip(prices.into_iter()).collect()
}

#[instrument(err, skip(client, datapoints))]
pub fn store_prices(client: &redis::Client, coin: &Coin, datapoints: Vec<Datapoint>) -> Result<()> {
	let mut connection = client.get_connection()?;
	debug!("redis connection established");

	let count = datapoints.len();
	let datapoints_iter = datapoints
		.iter()
		.filter(|datapoint| datapoint.price.is_some());

	if let Err(error) = connection.rpush::<String, Vec<String>, i32>(
		format!("{}:prices", coin.name),
		datapoints_iter
			.clone()
			.map(|datapoint| datapoint.price.unwrap().to_string())
			.collect(),
	) {
		error!(error = ?error, datapoints = ?datapoints, "error pushing price to redis");
	}

	if let Err(error) = connection.rpush::<String, Vec<String>, i32>(
		format!("{}:timestamps", coin.name),
		datapoints_iter
			.map(|datapoint| datapoint.datetime.timestamp().to_string())
			.collect(),
	) {
		error!(error = ?error, datapoints = ?datapoints, "error pushing timestamp to redis");
	}

	info!(coin = ?coin, count = count, "stored datapoints");
	Ok(())
}
