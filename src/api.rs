use std::sync::Arc;

use ::prost::Message;
use axum::body::Body;
use axum::body::Bytes;
use axum::debug_handler;
use axum::extract::State;
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;

use crate::dynamodb_store::DynamoDbStore;
use crate::store::KvStore;
use crate::types::{DeleteObjectRequest, GetObjectRequest, ListKeyVersionsRequest, PutObjectRequest};

#[debug_handler]
pub async fn get_object(
	State(kvstore): State<Arc<DynamoDbStore>>,
	body: Bytes,
) -> impl IntoResponse {
	let request = match GetObjectRequest::decode(body.as_ref()) {
		Ok(req) => req,
		Err(_) => return Response::builder().status(StatusCode::BAD_REQUEST).body(Body::from("Unable to decode GetObjectRequest")).unwrap(),
	};

	match kvstore.get(request).await {
		Ok(response) => {
			let mut rsp = Response::new(Body::from(response.encode_to_vec()));
			*rsp.status_mut() = StatusCode::OK;
			rsp
		}
		Err(err) => {
			eprintln!("Failed to get object: {:?}", err);
			Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR).body(Body::from("Failed to get object")).unwrap()
		}
	}
}

pub async fn put_object(
	State(kvstore): State<Arc<DynamoDbStore>>,
	body: Bytes,
) -> impl IntoResponse {
	let request = match PutObjectRequest::decode(body.as_ref()) {
		Ok(req) => req,
		Err(_err) => return Response::builder().status(StatusCode::BAD_REQUEST).body(Body::from("Unable to decode PutObjectRequest")).unwrap(),
	};

	match kvstore.put(request).await {
		Ok(response) => {
			let mut rsp = Response::new(Body::from(response.encode_to_vec()));
			*rsp.status_mut() = StatusCode::OK;
			rsp
		}
		Err(err) => {
			eprintln!("Failed to put object: {:?}", err);
			Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR).body(Body::from("Failed to put object")).unwrap()
		}
	}
}

pub async fn delete_object(
	State(kvstore): State<Arc<DynamoDbStore>>,
	body: Bytes,
) -> impl IntoResponse {
	let request = match DeleteObjectRequest::decode(body.as_ref()) {
		Ok(req) => req,
		Err(_err) => return Response::builder().status(StatusCode::BAD_REQUEST).body(Body::from("Unable to decode DeleteObjectRequest")).unwrap(),
	};

	match kvstore.delete(request).await {
		Ok(response) => {
			let mut rsp = Response::new(Body::from(response.encode_to_vec()));
			*rsp.status_mut() = StatusCode::OK;
			rsp
		}
		Err(err) => {
			eprintln!("Failed to delete object: {:?}", err);
			Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR).body(Body::from("Failed to delete object")).unwrap()
		}
	}
}

pub async fn list_key_versions(
	State(kvstore): State<Arc<DynamoDbStore>>,
	body: Bytes,
) -> impl IntoResponse {
	let request = match ListKeyVersionsRequest::decode(body.as_ref()) {
		Ok(req) => req,
		Err(_err) => return Response::builder().status(StatusCode::BAD_REQUEST).body(Body::from("Unable to decode ListKeyVersionsRequest")).unwrap(),
	};

	match kvstore.list_key_versions(request).await {
		Ok(response) => {
			let mut rsp = Response::new(Body::from(response.encode_to_vec()));
			*rsp.status_mut() = StatusCode::OK;
			rsp
		}
		Err(err) => {
			eprintln!("Failed to list key versions: {:?}", err);
			Response::builder().status(StatusCode::INTERNAL_SERVER_ERROR).body(Body::from("Failed to list key versions")).unwrap()
		}
	}
}