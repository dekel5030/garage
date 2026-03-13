use std::sync::Arc;
use std::time::Duration;

use lapin::options::{BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use tokio_amqp::LapinTokioExt;

use crate::config::RabbitConfig;
use crate::error::Error;

/// Thin asynchronous RabbitMQ client used for publishing integration events.
#[derive(Clone)]
pub struct RabbitClient {
	channel: Arc<Channel>,
	config: Arc<RabbitConfig>,
}

impl RabbitClient {
	/// Create a new client and declare the configured exchange.
	pub async fn new(config: RabbitConfig) -> Result<Self, Error> {
		let conn = Connection::connect(
			&config.uri,
			ConnectionProperties::default().with_tokio(),
		)
		.await
		.map_err(|e| Error::Message(format!("RabbitMQ connection error: {e}")))?;

		let channel = conn
			.create_channel()
			.await
			.map_err(|e| Error::Message(format!("RabbitMQ channel error: {e}")))?;

		channel
			.exchange_declare(
				&config.exchange,
				ExchangeKind::Topic,
				ExchangeDeclareOptions {
					durable: true,
					auto_delete: false,
					internal: false,
					nowait: false,
					passive: false,
				},
				FieldTable::default(),
			)
			.await
			.map_err(|e| Error::Message(format!("RabbitMQ declare exchange error: {e}")))?;

		channel
			.confirm_select(ConfirmSelectOptions::default())
			.await
			.map_err(|e| Error::Message(format!("RabbitMQ confirm-select error: {e}")))?;

		Ok(Self {
			channel: Arc::new(channel),
			config: Arc::new(config),
		})
	}

	/// Publish a JSON payload to the configured exchange with the given routing key.
	///
	/// Best-effort at-least-once: retries a few times on transient failures and logs errors.
	pub async fn publish(&self, routing_key: &str, payload: &[u8]) -> Result<(), Error> {
		const MAX_ATTEMPTS: u32 = 3;
		const BACKOFF_MS: u64 = 500;

		let mut attempt = 0;
		let mut last_error_message: Option<String> = None;
		loop {
			attempt += 1;

			let confirm = self
				.channel
				.basic_publish(
					&self.config.exchange,
					routing_key,
					BasicPublishOptions {
						mandatory: false,
						immediate: false,
					},
					payload,
					BasicProperties::default().with_delivery_mode(2),
				)
				.await
				.map_err(|e| Error::Message(format!("RabbitMQ publish error: {e}")));

			match confirm {
				Ok(confirm) => {
					let outcome = confirm
						.await
						.map_err(|e| Error::Message(format!("RabbitMQ confirm await error: {e}")))?;
					if outcome.is_ack() {
						info!(
							exchange = %self.config.exchange,
							routing_key = %routing_key,
							"RabbitMQ: object_created event published"
						);
						return Ok(());
					}

					last_error_message = Some(format!("RabbitMQ publish not acknowledged: {:?}", outcome));
					warn!(
						exchange = %self.config.exchange,
						routing_key = %routing_key,
						"RabbitMQ publish not acknowledged: {:?}",
						outcome
					);
				}
				Err(e) => {
					last_error_message = Some(e.to_string());
					warn!(
						"RabbitMQ publish attempt {} failed for routing_key={}: {}",
						attempt, routing_key, e
					);
				}
			}

			if attempt >= MAX_ATTEMPTS {
				let msg = last_error_message.unwrap_or_else(|| {
					format!(
						"RabbitMQ publish failed after {} attempts (routing_key={})",
						MAX_ATTEMPTS, routing_key
					)
				});
				error!(
					exchange = %self.config.exchange,
					routing_key = %routing_key,
					error = %msg,
					"RabbitMQ: failed to publish object_created event after retries"
				);
				return Err(Error::Message(msg));
			}

			tokio::time::sleep(Duration::from_millis(BACKOFF_MS * attempt as u64)).await;
		}
	}
}

