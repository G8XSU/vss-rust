use aws_sdk_dynamodb::Client;
use std::sync::Arc;
use prost::Message;
use bytes::Bytes;
use std::collections::HashMap;
use std::str::FromStr;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, TransactWriteItem, Put, Delete, ExpectedAttributeValue, ComparisonOperator};
use crate::types::{DeleteObjectRequest, DeleteObjectResponse, GetObjectRequest, GetObjectResponse, KeyValue, ListKeyVersionsRequest, ListKeyVersionsResponse, PutObjectRequest, PutObjectResponse};

pub struct DynamoDbBackend {
	pub client: Client,
}

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

	// pub async fn put(&self, request: PutObjectRequest) -> std::io::Result<PutObjectResponse> {
	// 	let put_transact_items: Vec<TransactWriteItem> = request.transaction_items.iter()
	// 		.map(|kv| {
	// 			let record = build_vss_item(&request.store_id, kv); // TODO: handle this unwrap properly
	// 			let put = Put::builder()
	// 				.set_item(Some(record))
	// 				.table_name("YOUR_TABLE_NAME")
	// 				.condition_expression(Some("attribute_not_exists(#id) OR #version = :v".to_string())) // You might need to change this as per your attribute name of version
	// 				.expression_attribute_values(":v".into(), AttributeValue::N(kv.version.to_string()))
	// 				.build().unwrap();
	// 			TransactWriteItem::builder().put(put).build()
	// 		})
	// 		.collect();
	//
	// 	let delete_transact_items: Vec<TransactWriteItem> = request.delete_items.iter()
	// 		.map(|kv| {
	// 			let mut key: HashMap<String, AttributeValue> = HashMap::new();
	// 			key.insert("store_id".to_string(), AttributeValue::S(request.store_id.clone()));
	// 			key.insert("key".to_string(), AttributeValue::S(kv.key.clone()));
	// 			let delete = Delete::builder()
	// 				.set_key(Some(key))
	// 				.table_name("YOUR_TABLE_NAME")
	// 				.condition_expression(Some("#version = :v".to_string())) // You might need to change this as per your attribute name of version
	// 				.expression_attribute_values(":v".into(), AttributeValue::N(kv.version.to_string()))
	// 				.build().unwrap();
	// 			TransactWriteItem::builder().delete(delete).build()
	// 		})
	// 		.collect();
	//
	// 	let mut all_transact_items = put_transact_items;
	// 	all_transact_items.extend(delete_transact_items);
	//
	// 	self.client.transact_write_items()
	// 		.set_transact_items(Some(all_transact_items))
	// 		.send()
	// 		.await
	// 		.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to put object: {:?}", err)))?;
	//
	// 	Ok(PutObjectResponse::default())
	// }
	//
	// pub async fn delete(&self, request: DeleteObjectRequest) -> std::io::Result<DeleteObjectResponse> {
	// 	match self.client.delete_item().table_name("YOUR_TABLE_NAME")
	// 		.key("store_id".to_string(), AttributeValue::S(request.store_id))
	// 		.key("key".to_string(), AttributeValue::S(request.key.clone()))
	// 		.expected("version".to_string(), ExpectedAttributeValue::builder()
	// 			.value(AttributeValue::N(request.version.expect("Version must be set for delete operation").to_string()))
	// 			.comparison_operator(ComparisonOperator::Eq)
	// 			.build())
	// 		.send()
	// 		.await {
	// 		Ok(_) => Ok(DeleteObjectResponse {}), // DeleteObjectResponse struct should provide appropriate success response
	// 		Err(err) => Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to delete object: {:?}", err))),
	// 	}
	// }
	//
	// pub async fn list_key_versions(&self, request: ListKeyVersionsRequest) -> std::io::Result<ListKeyVersionsResponse> {
	// 	let mut expr_attr_values: HashMap<String, AttributeValue> = HashMap::new();
	// 	expr_attr_values.insert(":storeIdVal".into(), AttributeValue::S(request.store_id.clone()));
	// 	if let Some(ref key_prefix) = request.key_prefix {
	// 		expr_attr_values.insert(":keyPrefixVal".into(), AttributeValue::S(key_prefix.clone()));
	// 	}
	//
	// 	// Prepare key condition expression
	// 	let key_cond_expr = if request.key_prefix.is_some() {
	// 		"store_id = :storeIdVal and begins_with(key, :keyPrefixVal)"
	// 	} else {
	// 		"store_id = :storeIdVal"
	// 	};
	//
	// 	let mut query = self.client.query()
	// 		.table_name("YOUR_TABLE_NAME")
	// 		.key_condition_expression(key_cond_expr)
	// 		.set_expression_attribute_values(Some(expr_attr_values));
	//
	// 	if let Some(start_key) = &request.page_token {
	// 		let mut expr_start_key: HashMap<String, AttributeValue> = HashMap::new();
	// 		expr_start_key.insert("store_id".into(), AttributeValue::S(request.store_id.clone()));
	// 		expr_start_key.insert("key".into(), AttributeValue::S(start_key.clone()));
	// 		query = query.set_exclusive_start_key(Some(expr_start_key));
	// 	}
	//
	// 	match query
	// 		.send()
	// 		.await {
	// 		Ok(output) => {
	// 			let key_versions = output.items.unwrap_or_default().into_iter().map(|item| {
	// 				KeyValue {
	// 					key: item.get("key").and_then(|av| av.s.clone()).unwrap_or_default(),
	// 					version: item.get("version").and_then(|av| av.n.as_ref().map(|n| n.parse().unwrap())).unwrap_or_default(),
	// 					value: item.get("value").and_then(|av| av.b.clone()).unwrap_or_default(),
	// 				}
	// 			}).collect();
	//
	// 			Ok(ListKeyVersionsResponse { key_versions, next_page_token: output.last_evaluated_key.map(|lek| lek.get("key").and_then(|av| av.s.clone()).unwrap_or_default()), ..Default::default() }) // Assuming ListKeyVersionsResponse has similar structure mentioned here
	// 		}
	// 		Err(err) => Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to list key versions: {:?}", err))),
	// 	}
	// }
}

fn build_vss_item(store_id: &str, kv: &KeyValue) -> HashMap<String, AttributeValue> {
	let mut item: HashMap<String, AttributeValue> = HashMap::new();
	item.insert("store_id".to_string(), AttributeValue::S(store_id.to_owned()));
	item.insert("key".to_string(), AttributeValue::S(kv.key.clone()));
	item.insert("value".to_string(), AttributeValue::B(Blob::new(kv.value.clone())));
	item.insert("version".to_string(), AttributeValue::N(kv.version.to_string()));
	item
}