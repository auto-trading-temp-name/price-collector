use eyre::{OptionExt, Result};
use tracing::{instrument, warn};

use crate::datapoint::{Datapoint, KrakenInterval, TimeType};

fn lerp(a: f64, b: f64, amount: f64) -> f64 {
	(1.0 - amount) * a + amount * b
}

#[instrument(skip(datapoints))]
pub fn interpolate_datapoints(
	mut datapoints: Vec<Datapoint>,
	input_interval: &KrakenInterval,
	output_interval: &KrakenInterval,
) -> Vec<Datapoint> {
	let input_interval = (*input_interval) as u16;
	let output_interval = (*output_interval) as u16;
	let steps = input_interval / output_interval;

	let mut rotated_datapoints = datapoints.clone();
	rotated_datapoints.rotate_left(1);
	datapoints.pop();
	rotated_datapoints.pop();

	let no_price_message = "no price in datapoint";

	datapoints
		.into_iter()
		.zip(rotated_datapoints.into_iter())
		.map(|(datapoint, next_datapoint)| {
			let price = datapoint.price.ok_or_eyre(no_price_message)?;
			let next_price = datapoint.price.ok_or_eyre(no_price_message)?;

			let timestamp = datapoint.datetime.timestamp();
			let next_timestamp = next_datapoint.datetime.timestamp();
			let coin = datapoint.coin.clone();

			let mut output = vec![datapoint];

			for i in 1..steps {
				let lerp_amount = (i as f32 / steps as f32) as f64;

				let interpolated_price = lerp(price, next_price, lerp_amount);
				let interpolated_timestamp = lerp(timestamp as f64, next_timestamp as f64, lerp_amount);

				output.push(Datapoint::new(
					Some(interpolated_price),
					TimeType::Timestamp(interpolated_timestamp as i64),
					coin.clone(),
				)?);
			}

			output.push(next_datapoint);

			Ok(output)
		})
		.filter_map(|x: Result<Vec<Datapoint>>| x.ok())
		.flatten()
		.collect()
}
