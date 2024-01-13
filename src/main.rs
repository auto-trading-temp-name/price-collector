mod abis;
mod coin;
mod discrepancies;
mod price;

use std::sync::Arc;
use std::{env, time::Duration};

use chrono::prelude::*;
use clokwerk::{AsyncScheduler, TimeUnits};
use discrepancies::{find_discrepancies, fix_discrepancies};
use ethers::prelude::*;
use eyre::Result;

use coin::load_coins;
use price::{fetch_prices, store_prices};

#[tokio::main]
async fn main() -> Result<()> {
	dotenvy::dotenv().expect(".env should exist");

	let infura_secret = env::var("INFURA_SECRET").expect("INFURA_SECRET should be in .env");
	let transport_url = format!("https://mainnet.infura.io/v3/{infura_secret}");
	let redis_uri = env::var("REDIS_URI").expect("REDIS_URI should be in .env");

	let web3_provider = Arc::new(Provider::<Http>::try_from(transport_url)?);
	let redis_client = Arc::new(redis::Client::open(redis_uri)?);

	let mut scheduler = AsyncScheduler::new();
	let (base_coin, coins) = load_coins();

	match find_discrepancies(redis_client.clone(), &coins) {
		Ok(discrepancies) => {
			if discrepancies.len() > 0 {
				eprintln!("{} discrepancies found, fixing...", discrepancies.len());
				for (coin, timestamps) in discrepancies {
					println!("fixing discrepancies for {}", coin.name);
					fix_discrepancies(coin, timestamps);
				}
			} else {
				println!("no discrepancies found");
			}
		}
		Err(error) => eprintln!("{}", error),
	};

	scheduler.every(1.minute()).run(move || {
		let provider_clone = web3_provider.clone();
		let client_clone = redis_client.clone();
		let base_coin_clone = base_coin.clone();
		let coins_clone = coins.clone();

		async move {
			let prices = fetch_prices(provider_clone, &base_coin_clone, coins_clone).await;
			let timestamp = Utc::now().timestamp();

			let _ = store_prices(client_clone, prices, timestamp);
		}
	});

	loop {
		scheduler.run_pending().await;
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
}
