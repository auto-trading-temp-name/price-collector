use std::sync::Arc;

use chrono::{prelude::*, Duration, DurationRound};
use eyre::{Ok, Result};
use redis::Commands;

use super::coin::Coin;

pub fn find_discrepancies(
	client: Arc<redis::Client>,
	coins: &Vec<Coin>,
) -> Result<Vec<(&Coin, Vec<i64>)>> {
	let mut connection = client.get_connection()?;
	let all_discrepancies = coins
		.into_iter()
		.map(|coin| {
			let mut timestamps: Vec<NaiveDateTime> = connection
				.lrange::<String, Vec<i64>>(format!("{}:timestamps", coin.name), 0, -1)
				.ok()?
				.into_iter()
				.map(|timestamp| NaiveDateTime::from_timestamp_opt(timestamp.to_owned(), 0))
				.filter(|x| x.is_some())
				.map(|x| x.unwrap())
				.collect();
			timestamps.push(
				NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0)?
					.duration_round(Duration::minutes(1))
					.ok()?,
			);

			let mut discrepancies = vec![];

			for i in 1..timestamps.len() - 1 {
				let current_time = timestamps[i];
				let current_timestamp = current_time.timestamp();
				let next_time = timestamps[i + 1];

				let difference = next_time - current_time;
				if difference > Duration::minutes(1) {
					let end_timestamp = timestamps.get(i + 1).unwrap_or(&next_time);
					let missing_points = ((*end_timestamp).timestamp() - current_time.timestamp()) / 60;
					discrepancies = [
						discrepancies,
						(1..missing_points)
							.map(|x| current_timestamp + (x + 1 * 60))
							.collect(),
					]
					.concat();
				}
			}

			if discrepancies.len() > 0 {
				Some((coin, discrepancies))
			} else {
				None
			}
		})
		.filter(|x| x.is_some())
		.map(|x| x.unwrap())
		.collect();

	Ok(all_discrepancies)
}

pub fn fix_discrepancies(coins: &Coin, discrepancy_timestamps: Vec<i64>) {
	todo!();
}
