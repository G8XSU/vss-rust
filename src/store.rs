use axum::async_trait;

use crate::types::{DeleteObjectRequest, DeleteObjectResponse, GetObjectRequest, GetObjectResponse, ListKeyVersionsRequest, ListKeyVersionsResponse, PutObjectRequest, PutObjectResponse};

#[async_trait]
pub trait KvStore {
	async fn get(&self, request: GetObjectRequest) -> std::io::Result<GetObjectResponse>;
	async fn put(&self, request: PutObjectRequest) -> std::io::Result<PutObjectResponse>;
	async fn delete(&self, request: DeleteObjectRequest) -> std::io::Result<DeleteObjectResponse>;
	async fn list_key_versions(&self, request: ListKeyVersionsRequest) -> std::io::Result<ListKeyVersionsResponse>;
}
