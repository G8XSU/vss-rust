use std::net::SocketAddr;
use std::sync::Arc;

use aws_sdk_dynamodb::Client;
use axum::Router;
use axum::routing::post;

use crate::api::{delete_object, get_object, list_key_versions, put_object};
use crate::dynamodb_store::DynamoDbStore;

pub(crate) mod api;
pub(crate) mod types;
pub(crate) mod store;
pub(crate) mod dynamodb_store;

#[tokio::main]
async fn main() {
	let shared_config = aws_config::from_env().endpoint_url("http://localhost:8000").load().await;
	let client = Client::new(&shared_config);

	// Wrap DynamoDbBackend in Arc (Atomic Reference Counter) for sharing across threads
	let store = Arc::new(DynamoDbStore::new(client));

	let app = Router::new()
		.route("/getObject", post(get_object))
		.route("/putObjects", post(put_object))
		.route("/listKeyVersions", post(list_key_versions))
		.route("/deleteObject", post(delete_object))
		.with_state(store);

	let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
	axum::Server::bind(&addr)
		.serve(app.into_make_service())
		.await.unwrap();
}
