use std::env;
use std::sync::Arc;

use ethers::prelude::*;
use eyre::Result;
use futures::future;
use redis::Commands;
use tracing::{error, info, instrument, trace, warn};

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
		trace!(coin = ?coin, "fetched price");
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

#[instrument(err, skip(client, data))]
pub fn store_prices(client: &redis::Client, coin: &Coin, data: Vec<Datapoint>) -> Result<()> {
	let mut connection = client.get_connection()?;
	trace!("redis connection established");

	for datapoint in data {
		let Datapoint { coin, .. } = datapoint.clone();

		let Some(price) = datapoint.price else {
			continue;
		};

		if let Err(error) =
			connection.rpush::<String, String, i32>(format!("{}:prices", coin.name), price.to_string())
		{
			error!(error = ?error, "error pushing price to redis");
			continue;
		}

		if let Err(error) = connection.rpush::<String, String, i32>(
			format!("{}:timestamps", coin.name),
			datapoint.datetime.timestamp().to_string(),
		) {
			error!(error = ?error, "error pushing timestamp to redis");
			continue;
		}

		info!(coin = ?coin, datapoint = ?datapoint, "stored datapoint",);
	}

	Ok(())
}
