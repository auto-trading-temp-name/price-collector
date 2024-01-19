use std::env;
use std::sync::Arc;

use ethers::prelude::*;
use eyre::Result;
use futures::future;
use redis::Commands;

use crate::abis::Quoter;
use crate::coin::Coin;
use crate::datapoint::Datapoint;

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
		println!("fetched price for {}", coin.name);
		coin.get_price(base_coin, quoter.as_ref())
	}))
	.await
	.into_iter()
	.map(|result| match result {
		Ok(price) => Some(f64::from(price) / (f64::powi(10_f64, base_coin.decimals))),
		Err(error) => {
			eprintln!("{}", error);
			None
		}
	})
	.filter_map(|x| x)
	.collect();

	coins.to_vec().into_iter().zip(prices.into_iter()).collect()
}

pub fn store_prices(client: &redis::Client, coin: &Coin, data: Vec<Datapoint>) -> Result<()> {
	let mut connection = client.get_connection()?;
	println!("redis connection established");

	for datapoint in data {
		if datapoint.price.is_none() {
			continue;
		}

		if let Err(error) = connection.rpush::<String, String, i32>(
			format!("{}:prices", coin.name),
			datapoint.price.unwrap().to_string(),
		) {
			eprintln!("{}", error);
		}

		if let Err(error) = connection.rpush::<String, String, i32>(
			format!("{}:timestamps", coin.name),
			datapoint.datetime.timestamp().to_string(),
		) {
			eprintln!("{}", error);
		}

		println!(
			"stored price for {} at time {}",
			datapoint.coin.name,
			datapoint.datetime.timestamp()
		);
	}

	Ok(())
}
