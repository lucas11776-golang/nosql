use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{Database, DeleteResult, InsertResult, RecordId, UpdateResult};

pub struct Collection<'a> {
    pub(crate) db: &'a Database,
    pub(crate) name: String,
}

impl<'a> Collection<'a> {
    pub fn new(db: &'a Database, name: String) -> Self {
        Self {
            db: db,
            name: name
        }
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
            let catalog = self.db.catalog.read().await;
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
                            results.push((
                                RecordId { page_id, slot_id },
                                doc,
                            ));
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

    pub async fn insert_many(&self, mut docs: Vec<Value>) -> Result<Vec<InsertResult>> {
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

    pub async fn find(&self, filter: Value) -> Result<Vec<Value>> {
        let matches = self.find_internal_records(&filter, usize::MAX).await?;
        Ok(matches.into_iter().map(|(_, doc)| doc).collect())
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
