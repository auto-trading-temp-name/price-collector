use super::abis::Quoter;
use super::coin::Coin;
use ethers::prelude::*;
use eyre::Result;
use futures::future;
use redis::Commands;
use std::env;
use std::sync::Arc;

pub async fn fetch_prices(
	provider: Arc<Provider<Http>>,
	coins: Vec<Coin>,
) -> Vec<(Coin, Option<f64>)> {
	let quoter = Arc::new(Quoter::new(
		env::var("QUOTER_ADDRESS")
			.expect("QUOTER_ADDRESS should be in .env")
			.parse::<Address>()
			.expect("QUOTER_ADDRESS should be a valid address"),
		provider,
	));

	let base_coin = &coins[0];

	let prices: Vec<Option<f64>> = future::join_all(coins[1..].iter().map(|coin| {
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
	.collect();

	let result: Vec<(Coin, Option<f64>)> = coins[1..]
		.to_vec()
		.into_iter()
		.zip(prices.into_iter())
		.collect();
	return result;
}

pub fn store_prices(
	client: Arc<redis::Client>,
	prices: Vec<(Coin, Option<f64>)>,
	timestamp: u128,
) -> Result<()> {
	let mut connection = client.get_connection()?;
	for (coin, price) in prices {
		match price {
			Some(price) => {
				match connection
					.rpush::<String, String, i32>(format!("{}:prices", coin.name), price.to_string())
				{
					Ok(_) => {}
					Err(error) => eprintln!("{}", error),
				}

				match connection
					.rpush::<String, String, i32>(format!("{}:timestamps", coin.name), timestamp.to_string())
				{
					Ok(_) => {}
					Err(error) => eprintln!("{}", error),
				}
				println!("stored price for {}", coin.name);
			}
			None => {}
		};
	}

	Ok(())
}
