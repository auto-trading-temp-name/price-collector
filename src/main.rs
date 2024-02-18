mod datapoint;
mod fixes;
mod interpolate;
mod price;

use std::sync::Arc;
use std::{env, env::VarError};

use chrono::{prelude::*, Duration, DurationRound};
use clokwerk::AsyncScheduler;
use datapoint::Datapoint;
use ethers::prelude::*;
use eyre::Result;
use fixes::{find_discrepancies, fix_discrepancies, initialize_datapoints};
use tracing::{error, info, warn};
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_panic::panic_hook;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

use price::{fetch_prices, store_prices};
use shared::coin::load_coins;
use shared::CustomInterval;

// have to use Duration::milliseconds due to milliseconds (and micro/nanoseconds)
// being the only way to construct a chrono::Duration in a const
pub const COLLECTION_INTERVAL: CustomInterval =
	CustomInterval(Duration::milliseconds(5 * 60 * 1_000));

#[tokio::main]
async fn main() -> Result<()> {
	let subscriber = Registry::default()
		.with(JsonStorageLayer)
		.with(BunyanFormattingLayer::new("price-collector".into(), {
			std::fs::File::options()
				.append(true)
				.create(true)
				.open("price-collector.log")?
		}))
		.with(BunyanFormattingLayer::new(
			"price-collector".into(),
			std::io::stdout,
		));

	tracing::subscriber::set_global_default(subscriber).unwrap();
	std::panic::set_hook(Box::new(panic_hook));

	dotenvy::dotenv().expect(".env should exist");

	// transaction processor is optional
	// env::var("TRANSACTION_PROCESSOR_URI").expect("TRANSACTION_PROCESSOR_URI should be in .env");
	env::var("QUOTER_ADDRESS").expect("QUOTER_ADDRESS should be in .env");
	let infura_secret = env::var("INFURA_SECRET").expect("INFURA_SECRET should be in .env");
	let redis_uri = env::var("REDIS_URI").expect("REDIS_URI should be in .env");
	let transport_url = format!("https://mainnet.infura.io/v3/{infura_secret}");

	let web3_provider = Arc::new(Provider::<Http>::try_from(transport_url)?);
	let redis_client = Arc::new(redis::Client::open(redis_uri)?);

	let mut scheduler = AsyncScheduler::new();
	let (base_coin, coins) = load_coins();

	async {
		for coin in &coins {
			match initialize_datapoints(redis_client.clone(), coin).await {
				Ok(datapoints) => {
					if datapoints.len() > 0 {
						let _ = store_prices(&redis_client, coin, datapoints);
					}
				}
				Err(error) => return error!(error = ?error, "error getting initial datapoints"),
			};

			let discrepancies = match find_discrepancies(&redis_client, coin) {
				Ok(datapoints) => datapoints,
				Err(error) => return error!(error = ?error, "error finding discrepancies"),
			};

			if discrepancies.len() < 1 {
				return info!("no discrepancies found");
			}

			warn!(coin = ?coin, count = discrepancies.len(), "fixing discrepancies");

			match fix_discrepancies(coin, discrepancies).await {
				Ok(datapoints) => {
					if let Ok(_) = store_prices(&redis_client, coin, datapoints) {
						info!("stored fixed discrepancies");
					}
				}
				Err(error) => error!(error = ?error, "error fixing discrepancies"),
			}
		}
	}
	.await;

	scheduler
		.every(COLLECTION_INTERVAL.interval())
		.run(move || {
			info!(
				interval = format!("{}s", COLLECTION_INTERVAL.duration().num_seconds()),
				"collecting prices"
			);

			let provider_clone = web3_provider.clone();
			let client_clone = redis_client.clone();
			let base_coin_clone = base_coin.clone();
			let coins_clone = coins.clone();

			async move {
				let prices = fetch_prices(provider_clone, &base_coin_clone, coins_clone).await;
				info!("fetched prices");
				let datetime = datapoint::TimeType::DateTime(
					Utc::now()
						.duration_trunc(COLLECTION_INTERVAL.duration())
						.expect("price collection timestamp did not truncate propperly"),
				);

				for (coin, price) in prices {
					let datapoint = match Datapoint::new(Some(price), datetime, coin.clone()) {
						Ok(datapoint) => datapoint,
						Err(error) => return error!(error = ?error, "error creating datapoint"),
					};

					let timestamp = datapoint.datetime.timestamp();

					match store_prices(&client_clone, &coin, vec![datapoint]) {
						Ok(_) => info!(price, coin = ?coin, "stored price"),
						Err(error) => return error!(error = ?error, "error storing prices"),
					}

					let transaction_processor_uri = match env::var("TRANSACTION_PROCESSOR_URI") {
						Ok(uri) => uri,
						Err(error) => {
							return warn!(error = ?error, "{}", match error {
								VarError::NotPresent => "TRANSACTION_PROCESSOR_URI not specified",
								_ => "error getting TRANSACTION_PROCESSOR_URI"
							})
						}
					};

					match reqwest::get(format!(
						"{}/price_update?timestamp={}",
						transaction_processor_uri, timestamp
					))
					.await
					{
						Ok(_) => info!("sent out price update signal to transaction processor"),
						Err(error) => {
							error!(error = ?error, "error sending price signal to transaction processor")
						}
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
