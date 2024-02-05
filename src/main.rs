mod datapoint;
mod discrepancies;
mod price;

use std::env;
use std::sync::Arc;

use chrono::{prelude::*, Duration, DurationRound};
use clokwerk::AsyncScheduler;
use datapoint::Datapoint;
use discrepancies::{find_discrepancies, fix_discrepancies, initialize_datapoints};
use ethers::prelude::*;
use eyre::Result;

use price::{fetch_prices, store_prices};
use shared::coin::load_coins;
use shared::CustomInterval;

// have to use Duration::milliseconds due to milliseconds (and micro/nanoseconds)
// being the only way to construct a chrono::Duration in a const
pub const COLLECTION_INTERVAL: CustomInterval =
	CustomInterval(Duration::milliseconds(5 * 60 * 1_000));

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

	for coin in &coins {
		match initialize_datapoints(redis_client.clone(), coin).await {
			Ok(datapoints) => {
				println!("storing initial prices for {}", coin.name);
				let _ = store_prices(&redis_client, coin, datapoints);
			}
			Err(error) => eprintln!("{}", error),
		};
	}

	match find_discrepancies(redis_client.clone(), &coins) {
		Ok(discrepancies) => {
			if discrepancies.len() > 0 {
				eprintln!("{} discrepancies found, fixing...", discrepancies.len());
				for (coin, datapoints) in discrepancies {
					println!("fixing discrepancies for {}", coin.name);
					let fixed = fix_discrepancies(coin, datapoints).await;
					if let Ok(fixed) = fixed {
						println!("discrepancies fixed");
						let _ = store_prices(&redis_client, coin, fixed);
					} else {
						eprintln!("{}", fixed.unwrap_err());
					}
				}
			} else {
				println!("no discrepancies found");
			}
		}
		Err(error) => eprintln!("{}", error),
	};

	scheduler
		.every(COLLECTION_INTERVAL.interval())
		.run(move || {
			println!("collecting prices at {}", Local::now());
			let provider_clone = web3_provider.clone();
			let client_clone = redis_client.clone();
			let base_coin_clone = base_coin.clone();
			let coins_clone = coins.clone();

			async move {
				println!("fetching prices at {}", Local::now());
				let prices = fetch_prices(provider_clone, &base_coin_clone, coins_clone).await;
				let datetime = datapoint::TimeType::DateTime(
					Utc::now()
						.duration_trunc(COLLECTION_INTERVAL.duration())
						.expect("price collection timestamp did not truncate propperly"),
				);
				for (coin, price) in prices {
					if let Ok(datapoint) = Datapoint::new(Some(price), datetime, coin.clone()) {
						match store_prices(&client_clone, &coin, vec![datapoint]) {
							Ok(_) => (),
							Err(error) => eprintln!("{}", error),
						};
					}
				}
			}
		});

	loop {
		scheduler.run_pending().await;
		tokio::time::sleep(
			Duration::milliseconds(10)
				.to_std()
				.expect("10ms sleep could not parse to std"),
		)
		.await;
	}
}
