use std::collections::HashMap;

use serde::Serialize;

/// Integration event emitted when an object version becomes the latest
/// complete data-bearing version for a given key.
/// Serialized with camelCase for .NET consumer compatibility.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectCreatedEvent {
	/// Object key within the bucket.
	pub key: String,
	/// Bucket name.
	pub bucket: String,
	/// Size of the object payload in bytes.
	pub size: u64,
	/// Content type from the request (e.g. video/mp4).
	pub content_type: String,
	/// ISO8601 timestamp when the event was created (e.g. 2026-03-13T14:15:00Z).
	pub occurred_at: String,
	/// User-defined metadata from x-amz-meta-* headers.
	pub metadata: HashMap<String, String>,
}

