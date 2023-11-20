use std::sync::Arc;

use ::prost::Message;
use axum::{debug_handler, RequestExt};
use axum::body::Bytes;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;

use crate::store::DynamoDbBackend;
use crate::types::GetObjectRequest;

#[debug_handler]
pub async fn get_object(
	State(db): State<Arc<DynamoDbBackend>>,
	body: Bytes
) -> impl IntoResponse {
	println!("RequestBodyBytesBase64: {}", base64::encode(&body));
	let request = match GetObjectRequest::decode(body.as_ref()) {
		Ok(req) => req,
		Err(_) => return Response::builder().status(StatusCode::BAD_REQUEST).body(Body::from("Unable to decode GetObjectRequest")).unwrap(),
	};

	match db.get(request).await {
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
//
// pub async fn put_object(
// 	Bytes(bytes): Bytes,
// 	Extension(db): Extension<Arc<DynamoDbBackend>>
// ) -> impl IntoResponse {
// 	let request = match PutObjectRequest::decode(bytes.as_ref()) {
// 		Ok(req) => req,
// 		Err(_err) => return (StatusCode::BAD_REQUEST, "Unable to decode PutObjectRequest").into_response(),
// 	};
//
// 	match db.put(request).await {
// 		Ok(response) => {
// 			match response.encode_to_vec() {
// 				Ok(response_bytes) => Response::new(Body::from(response_bytes)),
// 				Err(err) => {
// 					eprintln!("Failed encoding PutObjectResponse into byte vec: {:?}", err);
// 					(StatusCode::INTERNAL_SERVER_ERROR, "Failed encoding response to byte vec").into_response()
// 				}
// 			}
// 		},
// 		Err(err) => {
// 			eprintln!("Failed to put object: {:?}", err);
// 			(StatusCode::INTERNAL_SERVER_ERROR, "Failed to put object").into_response()
// 		}
// 	}
// }
//
// pub async fn delete_object(
// 	Bytes(bytes): Bytes,
// 	Extension(db): Extension<Arc<DynamoDbBackend>>
// ) -> impl IntoResponse {
// 	let request = match DeleteObjectRequest::decode(bytes.as_ref()) {
// 		Ok(req) => req,
// 		Err(_err) => return (StatusCode::BAD_REQUEST, "Unable to decode DeleteObjectRequest").into_response(),
// 	};
//
// 	match db.delete(request).await {
// 		Ok(response) => {
// 			match response.encode_to_vec() {
// 				Ok(response_bytes) => Response::new(Body::from(response_bytes)),
// 				Err(err) => {
// 					eprintln!("Failed encoding DeleteObjectResponse into byte vec: {:?}", err);
// 					(StatusCode::INTERNAL_SERVER_ERROR, "Failed encoding response to byte vec").into_response()
// 				}
// 			}
// 		},
// 		Err(err) => {
// 			eprintln!("Failed to delete object: {:?}", err);
// 			(StatusCode::INTERNAL_SERVER_ERROR, "Failed to delete object").into_response()
// 		}
// 	}
// }
//
// pub async fn list_key_versions(
// 	Bytes(bytes): Bytes,
// 	Extension(db): Extension<Arc<DynamoDbBackend>>
// ) -> impl IntoResponse {
// 	let request = match ListKeyVersionsRequest::decode(bytes.as_ref()) {
// 		Ok(req) => req,
// 		Err(_err) => return (StatusCode::BAD_REQUEST, "Unable to decode ListKeyVersionsRequest").into_response(),
// 	};
//
// 	match db.list_key_versions(request).await {
// 		Ok(response) => {
// 			match response.encode_to_vec() {
// 				Ok(response_bytes) => Response::new(Body::from(response_bytes)),
// 				Err(err) => {
// 					eprintln!("Failed encoding ListKeyVersionsResponse into byte vec: {:?}", err);
// 					(StatusCode::INTERNAL_SERVER_ERROR, "Failed encoding response to byte vec").into_response()
// 				}
// 			}
// 		},
// 		Err(err) => {
// 			eprintln!("Failed to list key versions: {:?}", err);
// 			(StatusCode::INTERNAL_SERVER_ERROR, "Failed to list key versions").into_response()
// 		}
// 	}
// }