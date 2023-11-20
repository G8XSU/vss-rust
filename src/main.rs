use std::sync::Arc;
use tower::ServiceBuilder;
use hyper::body::aggregate;
use axum::{debug_handler, Router};
use std::net::SocketAddr;
use std::str::Bytes;
use aws_sdk_dynamodb::Client;
use axum::handler::HandlerWithoutStateExt;
// use axum::middleware::MapRequestLayer;
use axum::routing::post;
use hyper::Body;
use tower::util::MapRequestLayer;
use crate::api::get_object;
use crate::store::DynamoDbBackend;
// use aws_config::meta::region::RegionProviderChain;
// use tower_http::map_request::Layer as MapRequestLayer;

pub(crate) mod api;
pub(crate) mod types;
pub(crate) mod store;

#[tokio::main]
async fn main() {


	let shared_config = aws_config::from_env().endpoint_url("http://localhost:8000").load().await;
	let client = Client::new(&shared_config);

	let store = Arc::new(DynamoDbBackend::new(client));

	let app = Router::new()
		.route("/getObject", post(get_object))
		.with_state(store)
		// .route("/PutObject", post(api::put_object))
		// .route("/DeleteObject", post(api::delete_object))
		// .route("/ListKeyVersions", post(api::list_key_versions)
		// )
		;

	// Wrap DynamoDbBackend in Arc (Atomic Reference Counter) for sharing across threads

	// let app = ServiceBuilder::new()
	// 	.layer(MapRequestLayer::new(move |(req, _)| (req, store.clone())))
	// 	.service(router);

	let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
	// axum::Server::bind(&addr).serve(app.into_service()).await.unwrap();
	// And run our service
	axum::Server::bind(&addr)
		.serve(app.into_make_service())
		.await.unwrap();

	// let app = ServiceBuilder::new()
	//   .layer_fn(|svc| MapRequestLayer::new(
	//       move |req| { (req, store.clone()) })
	//   )
	//   .service(router);
	//
	// let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
	//
	// axum::Server::bind(&addr)
	//   .serve(app.into_make_service_with_connect_info::<_, std::net::SocketAddr, _>())
	//   .await
	//   .unwrap();
}

// #[debug_handler]
// async fn handler(db: Arc<DynamoDbBackend>, body: Body) -> String {
// 	"Hello, world".to_string()
// }
