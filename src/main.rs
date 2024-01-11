mod abis;

use abis::Quoter;
use clokwerk::{AsyncScheduler, TimeUnits};
use ethers::prelude::*;
use ethers::utils::parse_units;
use eyre::Result;
use futures::future;
use redis::Commands;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{env, fs};

const COINS_PATH: &str = "coins.json";

#[derive(Deserialize, Clone)]
struct Coin {
	name: Box<str>,
	address: Address,
	decimals: i32,
}

impl Coin {
	async fn get_price(&self, reference_coin: &Coin, quoter: &Quoter<Provider<Http>>) -> Result<u32> {
		Ok(
			quoter
				.quote_exact_input_single(
					self.address,
					reference_coin.address,
					500,
					U256::from(
						parse_units(1.0, self.decimals)
							.expect(format!("1 {} did not parse correctly", reference_coin.name).as_str()),
					),
					U256::zero(),
				)
				.call()
				.await?
				.as_u32(),
		)
	}
}

fn load_coins() -> Vec<Coin> {
	let coin_data: Vec<Coin> =
		serde_json::from_str(&fs::read_to_string(COINS_PATH).expect("{COINS_PATH} does not exist"))
			.expect("{COINS_PATH} is not valid");
	return coin_data;
}

async fn fetch_prices(provider: Arc<Provider<Http>>, coins: Vec<Coin>) -> Vec<(Coin, Option<f64>)> {
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

fn store_prices(
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

#[tokio::main]
async fn main() -> Result<()> {
	dotenvy::dotenv().expect(".env should exist");

	let infura_secret = env::var("INFURA_SECRET").expect("INFURA_SECRET should be in .env");
	let transport_url = format!("https://mainnet.infura.io/v3/{infura_secret}");
	let redis_uri = env::var("REDIS_URI").expect("REDIS_URI should be in .env");

	let web3_provider = Arc::new(Provider::<Http>::try_from(transport_url)?);
	let redis_client = Arc::new(redis::Client::open(redis_uri)?);

	let mut scheduler = AsyncScheduler::new();

	scheduler.every(1.minute()).run(move || {
		let provider_clone = web3_provider.clone();
		let client_clone = redis_client.clone();
		let coins = load_coins();

		async move {
			let prices = fetch_prices(provider_clone, coins).await;
			let timestamp = SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.expect("time should not go backwards")
				.as_millis();

			let _ = store_prices(client_clone, prices, timestamp);
		}
	});

	loop {
		scheduler.run_pending().await;
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
}
