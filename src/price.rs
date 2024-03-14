use std::{env, ops::Div, sync::Arc};

use ethers::prelude::*;
use ethers::utils::parse_units;
use eyre::{Context, OptionExt, Result};
use futures::future;
use redis::Commands;
use rug::{float::MiniFloat, ops::CompleteRound};
use shared::{abis::Quoter, coin::Pair};
use tracing::{debug, error, info, instrument};

use crate::datapoint::Datapoint;

pub async fn fetch_prices<P>(provider: Provider<P>, pairs: &[Pair]) -> Vec<(Pair, f64)>
where
	P: JsonRpcClient + 'static,
{
	let quoter = Arc::new(Quoter::new(
		env::var("QUOTER_ADDRESS")
			.expect("QUOTER_ADDRESS should be in .env")
			.parse::<Address>()
			.expect("QUOTER_ADDRESS should be a valid address"),
		provider.into(),
	));

	let prices: Vec<f64> = future::join_all(pairs.into_iter().map(|pair| {
		debug!(pair = ?pair, "fetched price");
		async {
			let price = quoter
				.quote_exact_input_single(
					pair.1.address,
					pair.0.address,
					500,
					U256::from(
						parse_units(1.0, pair.1.decimals)
							.wrap_err(format!("1 {} did not parse correctly", pair.1.name))?,
					),
					U256::zero(),
				)
				.call()
				.await?;
			let price = std::panic::catch_unwind(|| price.as_u128())
				.ok()
				.ok_or_eyre("panic when converting price to u128")?;

			Ok((pair.clone(), price))
		}
	}))
	.await
	.into_iter()
	.filter_map(|x: Result<(Pair, u128)>| {
		if let Err(ref error) = x {
			error!(error = ?error, "error getting price");
		}

		x.ok()
	})
	.map(|(pair, price)| {
		MiniFloat::from(price)
			.borrow_excl()
			.div(i32::pow(10, pair.0.decimals))
			.complete(24)
			.to_f64()
	})
	.collect();

	pairs.to_vec().into_iter().zip(prices.into_iter()).collect()
}

#[instrument(err, skip(client))]
pub fn store_prices(client: &redis::Client, pair: &Pair, datapoints: Vec<Datapoint>) -> Result<()> {
	let mut connection = client.get_connection()?;
	debug!("redis connection established");

	let count = datapoints.len();
	let datapoints_iter = datapoints.iter();

	if let Err(error) = connection.rpush::<String, Vec<String>, i32>(
		format!("{}:prices", pair.to_string()),
		datapoints_iter
			.clone()
			.map(|datapoint| datapoint.price.to_string())
			.collect(),
	) {
		error!(error = ?error, datapoints = ?datapoints, "error pushing price to redis");
	}

	if let Err(error) = connection.rpush::<String, Vec<String>, i32>(
		format!("{}:timestamps", pair.to_string()),
		datapoints_iter
			.map(|datapoint| datapoint.timestamp.to_string())
			.collect(),
	) {
		error!(error = ?error, datapoints = ?datapoints, "error pushing timestamp to redis");
	}

	info!(count = count, "stored datapoints");
	Ok(())
}
