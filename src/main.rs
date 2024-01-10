use clokwerk::{AsyncScheduler, TimeUnits};
use ethers::prelude::*;
use ethers::utils::parse_units;
use serde::Deserialize;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use std::{env, fs};

const COINS_PATH: &str = "coins.toml";

abigen!(Quoter, "src/abis/Quoter.json");
abigen!(ERC20, "src/abis/ERC20.json");

#[derive(Deserialize)]
struct Coin {
	name: Box<str>,
	address: Address,
}

impl Coin {
	fn new(name: &str, address: &str) -> Coin {
		Coin {
			name: name.into(),
			address: address.parse().unwrap(),
		}
	}
}

fn load_coins() -> Vec<Coin> {
	let coin_data: Vec<Coin> =
		toml::from_str(&fs::read_to_string(COINS_PATH).expect("coin.toml does not exist"))
			.expect("coin.toml is not valid");
	return coin_data;
}

async fn fetch_prices(provider: Arc<Provider<Http>>) {
	let quoter = Quoter::new(
		env::var("QUOTER_ADDRESS")
			.expect("QUOTER_ADDRESS should be in .env")
			.parse::<Address>()
			.unwrap(),
		provider,
	);

	let coins = load_coins();
	let base_coin = &coins[0];
	for coin in &coins[1..] {
		quoter.quote_exact_input_single(
			coin.address,
			base_coin.address,
			500,
			U256::from(parse_units("1.0", "6").unwrap()),
			U256::zero(),
		);
	}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	dotenvy::dotenv()?;

	let infura_secret = env::var("INFURA_SECRET").expect("INFURA_SECRET should be in .env");
	let transport_url = format!("https://mainnet.infura.io/v3/{infura_secret}");

	let provider = Arc::new(Provider::<Http>::try_from(transport_url).expect(""));

	let mut scheduler = AsyncScheduler::new();

	scheduler
		.every(1.minute())
		.run(move || fetch_prices(provider.clone()));

	loop {
		scheduler.run_pending().await;
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
}
