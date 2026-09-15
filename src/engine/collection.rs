use std::sync::Arc;

use anyhow::{Result, anyhow};
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde_json::{Value, from_value};
use uuid::Uuid;

use crate::{Database, DeleteResult, InsertResult, RecordId, UpdateResult, json};

pub struct Collection {
    pub(crate) db: Arc<Database>,
    pub(crate) name: String,
}

impl Collection {
    pub fn new(db: Arc<Database>, name: String) -> Self {
        Self { db: db, name: name }
    }

    fn canonical_id_key(id_val: &Value) -> String {
        match id_val {
            Value::String(s) => s.clone(),
            _ => id_val.to_string(),
        }
    }

    fn match_filter(doc: &Value, filter: &Value) -> bool {
        if filter.is_null() {
            return true;
        }
        if let (Some(doc_obj), Some(filter_obj)) = (doc.as_object(), filter.as_object()) {
            for (k, v) in filter_obj {
                if doc_obj.get(k) != Some(v) {
                    return false;
                }
            }
            true
        } else {
            false
        }
    }

    async fn find_internal_records(
        &self,
        filter: &Value,
        limit: usize,
    ) -> Result<Vec<(RecordId, Value)>> {
        let mut results = Vec::new();

        if let Some(id_val) = filter.get("_id") {
            let key = Self::canonical_id_key(id_val);
            let catalog = self.db.catalog.read().await;
            if let Some(idx_map) = catalog.primary_indexes.get(&self.name) {
                if let Some(&rid) = idx_map.get(&key) {
                    let page_guard = self.db.bpm.fetch_page_write(rid.page_id).await?;
                    if let Some(bytes) = page_guard.get(rid.slot_id) {
                        let doc: Value = serde_json::from_slice(bytes)?;
                        if Self::match_filter(&doc, filter) {
                            results.push((rid, doc));
                        }
                    }
                }
            }
            return Ok(results);
        }

        let pages = {
            let catalog = self
                .db
                .catalog
                .read()
                .await;

            catalog
                .collections
                .get(&self.name)
                .cloned()
                .unwrap_or_default()
        };

        for page_id in pages {
            let page_guard = self.db.bpm.fetch_page_write(page_id).await?;
            let slot_count = page_guard.slot_count();

            for slot_id in 0..slot_count {
                if let Some(bytes) = page_guard.get(slot_id) {
                    if let Ok(doc) = serde_json::from_slice::<Value>(bytes) {
                        if Self::match_filter(&doc, filter) {
                            results.push((RecordId { page_id, slot_id }, doc));
                            if results.len() >= limit {
                                return Ok(results);
                            }
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    pub async fn insert_one(&self, mut doc: Value) -> Result<InsertResult> {
        let doc_obj = doc
            .as_object_mut()
            .ok_or_else(|| anyhow!("Document must be a JSON object"))?;

        let inserted_id = if let Some(id) = doc_obj.get("_id") {
            id.clone()
        } else {
            let generated = json!(Uuid::new_v4().to_string());
            doc_obj.insert("_id".to_string(), generated.clone());
            generated
        };

        let bytes = serde_json::to_vec(&doc)?;
        let mut target_page_id = None;

        let pages = {
            let catalog = self.db.catalog.read().await;

            catalog
                .collections
                .get(&self.name)
                .cloned()
                .unwrap_or_default()
        };

        for page_id in pages {
            let page_guard = self.db.bpm.fetch_page_write(page_id).await?;
            if page_guard.available_space() >= bytes.len() + 4 {
                target_page_id = Some(page_id);
                break;
            }
        }

        let page_id = match target_page_id {
            Some(pid) => pid,
            None => {
                let new_pid = self.db.disk_manager.allocate_page().await?;
                let mut catalog = self.db.catalog.write().await;
                catalog
                    .collections
                    .get_mut(&self.name)
                    .unwrap()
                    .push(new_pid);
                new_pid
            }
        };

        let mut page_guard = self.db.bpm.fetch_page_write(page_id).await?;
        let slot_id = page_guard
            .insert(&bytes)
            .ok_or_else(|| anyhow!("Failed to insert record into page"))?;
        page_guard.mark_dirty();

        let rid = RecordId { page_id, slot_id };

        let key = Self::canonical_id_key(&inserted_id);
        let mut catalog = self.db.catalog.write().await;
        if let Some(idx_map) = catalog.primary_indexes.get_mut(&self.name) {
            idx_map.insert(key, rid);
        }

        drop(catalog);
        self.db.persist_catalog().await?;

        Ok(InsertResult { inserted_id })
    }

    pub async fn insert_many(&self, docs: Vec<Value>) -> Result<Vec<InsertResult>> {
        let mut results: Vec<InsertResult> = Vec::new();

        for doc in docs {
            results.push(self.insert_one(doc).await.unwrap());
        }

        Ok(results)
    }

    pub async fn find_one(&self, filter: Value) -> Result<Option<Value>> {
        let matches = self.find_internal_records(&filter, 1).await?;
        Ok(matches.into_iter().next().map(|(_, doc)| doc))
    }

    pub async fn find_one_as<T>(&self, filter: Value) -> Result<Option<T>>
    where
        T: ?Sized + DeserializeOwned,
    {
        self.find_one(filter).await.map(|value| {
            if let Some(v) = value {
                return from_value(v).unwrap();
            }
            None
        })
    }

    pub async fn find(&self, filter: Value) -> Result<Vec<Value>> {
        let matches = self.find_internal_records(&filter, usize::MAX).await?;
        Ok(matches.into_iter().map(|(_, doc)| doc).collect())
    }

    pub async fn find_as<T>(&self, filter: Value) -> Result<Vec<T>>
    where
        T: ?Sized + DeserializeOwned + Send,
    {
        self.find(filter).await.map(|values| {
            values
                .par_iter()
                .map(|value| from_value(value.clone()).unwrap())
                .collect::<Vec<T>>()
        })
    }

    pub async fn update(&self, filter: Value, update: Value) -> Result<UpdateResult> {
        let update_fields = update
            .as_object()
            .ok_or_else(|| anyhow!("Update argument must be a JSON object"))?;

        let matches = self.find_internal_records(&filter, usize::MAX).await?;
        if matches.is_empty() {
            return Ok(UpdateResult {
                matched_count: 0,
                modified_count: 0,
            });
        }

        let matched_count = matches.len();
        let mut modified_count = 0;

        for (rid, mut doc) in matches {
            for (k, v) in update_fields {
                if k != "_id" {
                    doc[k] = v.clone();
                }
            }

            let new_bytes = serde_json::to_vec(&doc)?;
            let mut page_guard = self.db.bpm.fetch_page_write(rid.page_id).await?;
            let in_place_success = page_guard.update(rid.slot_id, &new_bytes);

            if in_place_success {
                page_guard.mark_dirty();
            } else {
                page_guard.delete(rid.slot_id);
                page_guard.mark_dirty();
                drop(page_guard);

                self.insert_one(doc).await?;
            }

            modified_count += 1;
        }

        Ok(UpdateResult {
            matched_count,
            modified_count,
        })
    }

    pub async fn delete(&self, filter: Value) -> Result<DeleteResult> {
        let matches = self.find_internal_records(&filter, usize::MAX).await?;

        if matches.is_empty() {
            return Ok(DeleteResult { deleted_count: 0 });
        }

        let mut deleted_count = 0;

        for (rid, doc) in matches {
            let mut page_guard = self.db.bpm.fetch_page_write(rid.page_id).await?;
            let deleted = page_guard.delete(rid.slot_id);

            if deleted {
                page_guard.mark_dirty();

                if let Some(id_val) = doc.get("_id") {
                    let key = Self::canonical_id_key(id_val);
                    let mut catalog = self.db.catalog.write().await;
                    if let Some(idx_map) = catalog.primary_indexes.get_mut(&self.name) {
                        idx_map.remove(&key);
                    }
                }

                deleted_count += 1;
            }
        }

        if deleted_count > 0 {
            self.db.persist_catalog().await?;
        }

        Ok(DeleteResult { deleted_count })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde::{Deserialize, Serialize};
use serde_json::json;
    use tokio::fs;
    use uuid::Uuid;

    use crate::{Database, engine::collection::Collection};

    async fn delete(path: &str) -> Result<(), &'static str> {
        if let Err(_) = fs::remove_file(path).await {}
        Ok(())
    }

    async fn collection(db_name: &str, collection: &'static str) -> Result<Collection, &'static str> {
        let db = Database::open(db_name).await.unwrap();
        db.create(collection).await.unwrap();
        Ok(db.collection(collection).await)
    }

    #[tokio::test]
    async fn test_insert_one_document() -> Result<(), &'static str> {
        const DB_NAME: &'static str = "insert_one.bin";
        const EMAIL: &'static str = "jeo@deo.com";

        let collection = collection(DB_NAME, "subscriptions").await.unwrap();

        // Radom Generated `_id` UUID V4
        let result = collection
            .insert_one(json!({"email": EMAIL}))
            .await
            .unwrap();

        assert_eq!(Uuid::from_str(&result.inserted_id.to_string().trim_matches('"')).is_ok(), true);

        // Custom Generated `_id` 
        const ID: &'static str = "custom-id-1";

        let result = collection
            .insert_one(json!({"_id": ID, "email": EMAIL}))
            .await
            .unwrap();

        assert_eq!(ID, result.inserted_id);

        delete(DB_NAME).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_many_document() -> Result<(), &'static str> {
        const DB_NAME: &'static str = "insert_many.bin";
        const EMAILS: [&'static str; 2] = ["jeo@deo.com", "jane@deo.com"];

        let collection = collection(DB_NAME, "subscriptions").await.unwrap();

        let results = collection
            .insert_many(vec![
                json!({"email": EMAILS[0]}),
                json!({"email": EMAILS[1]}),
            ])
            .await
            .unwrap();

        assert_eq!(results.len(), 2);

        for result in results {
            assert_eq!(Uuid::from_str(&result.inserted_id.to_string().trim_matches('"')).is_ok(), true);
        }

        delete(DB_NAME).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_find_one_documents() -> Result<(), &'static str> {
        const DB_NAME: &'static str = "find_one.bin";
        const EMAIL: &'static str = "hello@example.com";

        let collection = collection(DB_NAME, "subscriptions").await.unwrap();

        let result = collection
            .insert_one(json!({"email": EMAIL}))
            .await
            .unwrap();

        let subscription = collection
            .find_one(json!({"_id": result.inserted_id}))
            .await
            .unwrap();

        assert_eq!(subscription.is_some(), true);
        assert_eq!(subscription.unwrap().get("email").unwrap(), EMAIL);

        delete(DB_NAME).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_find_one_as_document() -> Result<(), &'static str> {
        const DB_NAME: &'static str = "find_one_as.bin";
        const EMAIL: &'static str = "hello@example.com";

        let collection = collection(DB_NAME, "subscriptions").await.unwrap();

        let result = collection
            .insert_one(json!({"email": EMAIL}))
            .await
            .unwrap();

        #[derive(Deserialize, Serialize)]
        pub struct Subscription {
            pub _id: String,
            pub email: String,
        }

        let subscription = collection
            .find_one_as::<Subscription>(json!({"_id": result.inserted_id}))
            .await
            .unwrap();

        assert_eq!(subscription.is_some(), true);

        let sub = subscription.unwrap();

        assert_eq!(sub._id, result.inserted_id);
        assert_eq!(sub.email, EMAIL);

        delete(DB_NAME).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_find_documents() -> Result<(), &'static str> {
        const DB_NAME: &'static str = "find.bin";
        let collection = collection(DB_NAME, "users").await.unwrap();

        let mut guests: Vec<crate::Value> = Vec::new();

        // Generate 20 Guest accounts
        for i in 0..20 {
            guests.push(json!({
                "email": format!("guest-{}@company.com", i),
                "role": "guest"
            }));
        }

        let mut admins: Vec<crate::Value> = Vec::new();

        // Generate 5 Guest accounts
        for i in 0..3 {
            admins.push(json!({
                "email": format!("admin-{}@company.com", i),
                "role": "admin"
            }));
        }

        collection.insert_many(guests.clone()).await.unwrap();
        collection.insert_many(admins.clone()).await.unwrap();

        let guest_accounts = collection.find(json!({
            "role": "guest"
        })).await.unwrap();

        let admin_accounts= collection.find(json!({
            "role": "admin"
        })).await.unwrap();

        assert_eq!(guest_accounts.len(), 20);
        assert_eq!(admin_accounts.len(), 3);

        // Check guest fields
        for (i, value) in guests.iter().enumerate() {
            assert_eq!(guest_accounts[i].get("email").unwrap(), value.get("email").unwrap());
            assert_eq!(guest_accounts[i].get("role").unwrap(), value.get("role").unwrap());
        }

        // Check admin fields
        for (i, value) in admins.iter().enumerate() {
            assert_eq!(admin_accounts[i].get("email").unwrap(), value.get("email").unwrap());
            assert_eq!(admin_accounts[i].get("role").unwrap(), value.get("role").unwrap());
        }

        delete(DB_NAME).await.unwrap();
        
        Ok(())
    }

    #[tokio::test]
    async fn test_find_as_documents() -> Result<(), &'static str> {
        const DB_NAME: &'static str = "find_as.bin";
        let collection = collection(DB_NAME, "users").await.unwrap();

        let mut guests: Vec<crate::Value> = Vec::new();

        // Generate 20 Guest accounts
        for i in 0..20 {
            guests.push(json!({
                "email": format!("guest-{}@company.com", i),
                "role": "guest"
            }));
        }

        let mut admins: Vec<crate::Value> = Vec::new();

        // Generate 5 Guest accounts
        for i in 0..3 {
            admins.push(json!({
                "email": format!("admin-{}@company.com", i),
                "role": "admin"
            }));
        }

        collection.insert_many(guests.clone()).await.unwrap();
        collection.insert_many(admins.clone()).await.unwrap();

        #[derive(Debug, Serialize, Deserialize)]
        pub struct User {
            pub _id: String,
            pub email: String,
            pub role: String,
        }

        let guest_accounts = collection.find_as::<User>(json!({
            "role": "guest"
        })).await.unwrap();

        let admin_accounts= collection.find_as::<User>(json!({
            "role": "admin"
        })).await.unwrap();

        assert_eq!(guest_accounts.len(), 20);
        assert_eq!(admin_accounts.len(), 3);

        // Check guest fields
        for (i, value) in guests.iter().enumerate() {
            assert_eq!(guest_accounts[i].email, value.get("email").unwrap().to_string().trim_matches('"'));
            assert_eq!(guest_accounts[i].role, value.get("role").unwrap().to_string().trim_matches('"'));
        }

        // Check admin fields
        for (i, value) in admins.iter().enumerate() {
            assert_eq!(admin_accounts[i].email, value.get("email").unwrap().to_string().trim_matches('"'));
            assert_eq!(admin_accounts[i].role, value.get("role").unwrap().to_string().trim_matches('"'));
        }

        delete(DB_NAME).await.unwrap();
        
        Ok(())
    }

    #[tokio::test]
    async fn test_update_document() -> Result<(), &'static str> {
        const DB_NAME: &'static str = "update.bin";
        let collection = collection(DB_NAME, "users").await.unwrap();

        #[derive(Debug, Serialize, Deserialize)]
        pub struct User {
            pub _id: String,
            pub email: String,
            pub password: String,
        }

        // Generate 5 accounts
        for i in 0..5 {
            collection.insert_one(json!({
                "email": format!("employee-{}@company.com", i),
                "password": format!("password#{}", 1)
            })).await.unwrap();
        }

        // Updated second account password
        let result = collection.update(
            json!({"email": "employee-2@company.com"}),
            json!({"password": "test@123"}),
        ).await.unwrap();

        assert_eq!(result.matched_count, 1);
        assert_eq!(result.modified_count, 1);

        let user = collection.find_one_as::<User>(json!({
            "email": "employee-2@company.com"
        })).await.unwrap().unwrap();

        assert_eq!("test@123", user.password);

        delete(DB_NAME).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_document() -> Result<(), &'static str> {
        const DB_NAME: &'static str = "update.bin";
        let collection = collection(DB_NAME, "users").await.unwrap();

        // Generate 5 inactive
        for i in 0..5 {
            collection.insert_one(json!({
                "email": format!("test-{}@company.com", i),
                "status": "inactive",
                "password": format!("password#{}", 1)
            })).await.unwrap();
        }

        // Generate 20 active
        for i in 0..20 {
            collection.insert_one(json!({
                "email": format!("employee-{}@company.com", i),
                "status": "active",
                "password": format!("password#{}", 1)
            })).await.unwrap();
        }

        let result = collection.delete(json!({
            "status": "inactive",
        })).await.unwrap();

        assert_eq!(result.deleted_count, 5);

        delete(DB_NAME).await.unwrap();

        Ok(())
    }
}
