use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, Delete, Put, TransactWriteItem};

use crate::types::{DeleteObjectRequest, DeleteObjectResponse, GetObjectRequest, GetObjectResponse, KeyValue, ListKeyVersionsRequest, ListKeyVersionsResponse, PutObjectRequest, PutObjectResponse};

pub struct DynamoDbBackend {
	pub client: Client,
}
/*
```bash
aws dynamodb create-table \
    --table-name VSS \
    --attribute-definitions \
        AttributeName=store_id,AttributeType=S \
        AttributeName=key,AttributeType=S \
    --key-schema AttributeName=storeId,KeyType=HASH AttributeName=key,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 \
    --table-class STANDARD \
    --endpoint-url http://localhost:8000
```
*/
const VSS_TABLE: &'static str = "VSS";

impl DynamoDbBackend {
	pub fn new(client: Client) -> Self {
		Self { client }
	}

	pub async fn get(&self, request: GetObjectRequest) -> std::io::Result<GetObjectResponse> {
		match self.client.get_item()
			.table_name(VSS_TABLE)
			.key("store_id".to_string(), AttributeValue::S(request.store_id))
			.key("key".to_string(), AttributeValue::S(request.key.clone()))
			.consistent_read(true)
			.send().await {
			Ok(output) => match output.item {
				Some(item) => {
					let version = item.get("version").cloned().and_then(|av| av.as_n().ok().and_then(|v| v.parse::<i64>().ok())).unwrap_or(0);
					let value = item.get("value").and_then(|av| av.as_b().ok().cloned().map(Blob::into_inner)).unwrap_or_default();
					let response = GetObjectResponse {
						value: Some(KeyValue { version, value, key: request.key }),
						..Default::default()
					};
					Ok(response)
				}
				None => Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Key not found")),
			},
			Err(err) => Err(std::io::Error::new(std::io::ErrorKind::Other, err.to_string())),
		}
	}

	pub async fn put(&self, request: PutObjectRequest) -> std::io::Result<PutObjectResponse> {
		let put_transact_items: Vec<TransactWriteItem> = request.transaction_items.iter()
			.map(|kv| {
				let record = build_vss_item(&request.store_id, kv);
				let put = Put::builder()
					.set_item(Some(record))
					.table_name(VSS_TABLE)
					.condition_expression("attribute_not_exists(id) OR version = :v")
					.expression_attribute_values(":v".to_string(), AttributeValue::N(kv.version.to_string()))
					.build().unwrap();
				TransactWriteItem::builder().put(put).build()
			})
			.collect();

		let delete_transact_items: Vec<TransactWriteItem> = request.delete_items.iter()
			.map(|kv| {
				let mut key: HashMap<String, AttributeValue> = HashMap::new();
				key.insert("store_id".to_string(), AttributeValue::S(request.store_id.clone()));
				key.insert("key".to_string(), AttributeValue::S(kv.key.clone()));
				let delete = Delete::builder()
					.set_key(Some(key))
					.table_name(VSS_TABLE)
					.condition_expression("version = :v")
					.expression_attribute_values(":v".to_string(), AttributeValue::N(kv.version.to_string()))
					.build().unwrap();
				TransactWriteItem::builder().delete(delete).build()
			})
			.collect();

		let mut all_transact_items = put_transact_items;
		all_transact_items.extend(delete_transact_items);

		self.client.transact_write_items()
			.set_transact_items(Some(all_transact_items))
			.send()
			.await
			.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to put object: {:?}", err)))?;

		Ok(PutObjectResponse::default())
	}

	pub async fn delete(&self, request: DeleteObjectRequest) -> std::io::Result<DeleteObjectResponse> {
		let mut query = self.client.delete_item().table_name(VSS_TABLE)
			.key("store_id".to_string(), AttributeValue::S(request.store_id))
			.key("key".to_string(), AttributeValue::S(request.key_value.as_ref().unwrap().key.clone()));

		if request.key_value.as_ref().unwrap().version != -1 {
			query = query.condition_expression("version = :v")
				.expression_attribute_values(":v".to_string(), AttributeValue::N(request.key_value.unwrap().version.to_string()));
		}
		match query
			.send()
			.await {
			Ok(_) => Ok(DeleteObjectResponse {}),
			Err(err) => Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to delete object: {:?}", err))),
		}
	}

	pub async fn list_key_versions(&self, request: ListKeyVersionsRequest) -> std::io::Result<ListKeyVersionsResponse> {
		let mut expr_attr_values: HashMap<String, AttributeValue> = HashMap::new();
		let mut expr_attr_names: HashMap<String, String> = HashMap::new();

		let key_cond_expr = if let Some(ref key_prefix) = request.key_prefix {
			expr_attr_names.insert("#key".into(), "key".to_string());
			expr_attr_values.insert(":keyPrefixVal".into(), AttributeValue::S(key_prefix.clone()));
			"store_id = :storeIdVal and begins_with(#key, :keyPrefixVal)"
		} else {
			"store_id = :storeIdVal"
		};
		expr_attr_values.insert(":storeIdVal".into(), AttributeValue::S(request.store_id.clone()));

		let mut query = self.client.query()
			.table_name(VSS_TABLE)
			.key_condition_expression(key_cond_expr);

		if let Some(start_key) = &request.page_token {
			let mut expr_start_key: HashMap<String, AttributeValue> = HashMap::new();
			expr_start_key.insert("store_id".into(), AttributeValue::S(request.store_id.clone()));
			expr_start_key.insert("#key".into(), AttributeValue::S(start_key.clone()));
			expr_attr_names.insert("#key".into(), "key".to_string());
			query = query.set_exclusive_start_key(Some(expr_start_key));
		}

		query = query.set_expression_attribute_values(Some(expr_attr_values));
		query = query.set_expression_attribute_names(Some(expr_attr_names));

		match query
			.send()
			.await {
			Ok(output) => {
				let key_versions = output.items.unwrap_or_default().into_iter().map(|item| {
					KeyValue {
						key: item.get("key").and_then(|av| av.as_s().ok()).unwrap().to_string(),
						version: item.get("version").cloned().and_then(|av| av.as_n().ok().and_then(|v| v.parse::<i64>().ok())).unwrap_or(0),
						..Default::default()
					}
				}).collect();

				let next_page_token = output.last_evaluated_key.map(|lek| lek.get("key").and_then(|av| av.as_s().ok()).unwrap().to_string());
				Ok(ListKeyVersionsResponse { key_versions, next_page_token, ..Default::default() })
			}
			Err(err) => Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to list key versions: {:?}", err))),
		}
	}
}

fn build_vss_item(store_id: &str, kv: &KeyValue) -> HashMap<String, AttributeValue> {
	let mut item: HashMap<String, AttributeValue> = HashMap::new();
	item.insert("store_id".to_string(), AttributeValue::S(store_id.to_owned()));
	item.insert("key".to_string(), AttributeValue::S(kv.key.clone()));
	item.insert("value".to_string(), AttributeValue::B(Blob::new(kv.value.clone())));
	item.insert("version".to_string(), AttributeValue::N(kv.version.to_string()));
	item
}