mod abis;
mod coin;
mod price;

use clokwerk::{AsyncScheduler, TimeUnits};
use coin::load_coins;
use ethers::prelude::*;
use eyre::Result;
use price::{fetch_prices, store_prices};
use std::env;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
