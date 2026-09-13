use anyhow::{anyhow};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::engine::collection::Collection;
use crate::engine::manager::{BufferPoolManager, DiskManager};
use crate::engine::slotted::SlottedPage;

pub mod engine;
pub mod connection;

pub use serde_json::{json, Value};
pub use anyhow::Result;

pub const PAGE_SIZE: usize = 4096;
pub type PageId = u32;
pub type SlotId = u16;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordId {
    pub page_id: PageId,
    pub slot_id: SlotId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertResult {
    pub inserted_id: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateResult {
    pub matched_count: usize,
    pub modified_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResult {
    pub deleted_count: usize,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Catalog {
    pub collections: HashMap<String, Vec<PageId>>,
    pub primary_indexes: HashMap<String, HashMap<String, RecordId>>,
}

pub struct Database {
    pub(crate) bpm: Arc<BufferPoolManager>,
    pub(crate) disk_manager: Arc<DiskManager>,
    pub(crate) catalog: RwLock<Catalog>,
}

impl Database {
    pub async fn open(db_file: &str) -> Result<Arc<Self>> {
        let dm = Arc::new(DiskManager::new(db_file)?);
        let bpm = Arc::new(BufferPoolManager::new(10, dm.clone()));

        let num_pages = {
            let guard = dm.num_pages.lock().await;
            *guard
        };

        let catalog = if num_pages == 0 {
            let page_id = dm.allocate_page().await?;
            let initial_cat = Catalog::default();
            let bytes = serde_json::to_vec(&initial_cat)?;

            let mut page = SlottedPage::new(page_id);
            let len_bytes = (bytes.len() as u32).to_le_bytes();
            page.data[8..12].copy_from_slice(&len_bytes);
            page.data[12..12 + bytes.len()].copy_from_slice(&bytes);

            dm.write_page(page_id, &page.data)?;
            initial_cat
        } else {
            let mut page_buf = [0u8; PAGE_SIZE];
            dm.read_page(0, &mut page_buf)?;

            let payload_len = u32::from_le_bytes(page_buf[8..12].try_into()?) as usize;
            let bytes = &page_buf[12..12 + payload_len];
            serde_json::from_slice(bytes)?
        };

        Ok(Arc::new(Self {
            bpm,
            disk_manager: dm,
            catalog: RwLock::new(catalog),
        }))
    }

    pub(crate) async fn persist_catalog(&self) -> Result<()> {
        let cat = self.catalog.read().await;
        let bytes = serde_json::to_vec(&*cat)?;

        if bytes.len() > PAGE_SIZE - 12 {
            return Err(anyhow!("Catalog metadata exceeds single Page 0 size limit"));
        }

        let mut page = SlottedPage::new(0);
        let len_bytes = (bytes.len() as u32).to_le_bytes();
        page.data[8..12].copy_from_slice(&len_bytes);
        page.data[12..12 + bytes.len()].copy_from_slice(&bytes);

        self.disk_manager.write_page(0, &page.data)
    }

    pub async fn collection(self: &Arc<Self>, name: &str) -> Collection {
        let mut cat = self.catalog.write().await;
        cat.collections.entry(name.to_string()).or_default();
        cat.primary_indexes.entry(name.to_string()).or_default();

        Collection::new(Arc::clone(&self), String::from(name))
    }

    pub async fn create(&self, collection: &str) -> Result<()> {
        let mut cat = self.catalog.write().await;
        if cat.collections.contains_key(collection) {
            return Err(anyhow!("Collection '{}' already exists", collection));
        }

        cat.collections.insert(collection.to_string(), Vec::new());
        cat.primary_indexes
            .insert(collection.to_string(), HashMap::new());
        drop(cat);

        self.persist_catalog().await
    }

    pub async fn delete(&self, name: &str) -> Result<()> {
        let mut cat = self.catalog.write().await;
        if cat.collections.remove(name).is_none() {
            return Err(anyhow!("Collection '{}' does not exist", name));
        }

        cat.primary_indexes.remove(name);

        drop(cat);

        self.persist_catalog().await
    }

    pub async fn rename(&self, collection_old: &str, collection_new: &str) -> Result<()> {
        let mut cat = self.catalog.write().await;

        if !cat.collections.contains_key(collection_old) {
            return Err(anyhow!("Collection '{}' does not exist", collection_old));
        }
        if cat.collections.contains_key(collection_new) {
            return Err(anyhow!(
                "Target collection name '{}' already exists",
                collection_new
            ));
        }

        if let Some(pages) = cat.collections.remove(collection_old) {
            cat.collections.insert(collection_new.to_string(), pages);
        }

        if let Some(index) = cat.primary_indexes.remove(collection_old) {
            cat.primary_indexes
                .insert(collection_new.to_string(), index);
        }

        drop(cat);

        self.persist_catalog().await
    }

    pub async fn has(&self, collection: &str) -> bool {
        let cat = self.catalog.read().await;
        cat.collections.contains_key(collection)
    }

    pub async fn list(&self) -> Vec<String> {
        let cat = self.catalog.read().await;
        cat.collections.keys().cloned().collect()
    }

    pub async fn close(&self) -> Result<()> {
        self.bpm.flush_all().await
    }
}



#[cfg(test)]
mod tests {
    use tokio::fs;

    use crate::Database;

    async fn delete(path: &str) -> Result<(), &'static str> {
        fs::remove_file(path).await.unwrap();
        Ok(())
    }

    #[tokio::test]
    pub async fn test_open_nosql_database() -> Result<(), &'static str> {
        const DB_NAME: &'static str = "open_db.bin";

        Database::open(DB_NAME).await.unwrap();

        assert_eq!(fs::File::open(DB_NAME).await.is_err(), false);

        delete(DB_NAME).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    pub async fn test_create_collection() -> Result<(), &'static str>  {
        const DB_NAME: &'static str = "create_collection.bin";

        let db = Database::open(DB_NAME).await.unwrap();

        db.create("users").await.unwrap();

        assert_eq!(db.has("users").await, true);

        delete(DB_NAME).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    pub async fn test_rename_collection() -> Result<(), &'static str>  {
        const DB_NAME: &'static str = "rename_db.bin";
        const COLLECTION_OLD: &'static str = "user";
        const COLLECTION_NEW: &'static str = "users";

        let db = Database::open(DB_NAME).await.unwrap();

        db.create(COLLECTION_OLD).await.unwrap();
        db.rename(COLLECTION_OLD, COLLECTION_NEW).await.unwrap();

        assert_eq!(db.has(COLLECTION_OLD).await, false);
        assert_eq!(db.has(COLLECTION_NEW).await, true);

        delete(DB_NAME).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    pub async fn test_list_collection() -> Result<(), &'static str>  {
        const DB_NAME: &'static str = "list_db.bin";
        const COLLECTIONS: [&'static str; 2] =  ["users", "password_resets"];

        let db = Database::open(DB_NAME).await.unwrap();

        for collection in COLLECTIONS {
            db.create(collection).await.unwrap();
        }

        let list = db.list().await;

        for collection in list {
            assert_eq!(COLLECTIONS.contains(&collection.as_str()), true);
        }

        delete(DB_NAME).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    pub async fn test_delete_collection() -> Result<(), &'static str>  {
        const DB_NAME: &'static str = "delete_db.bin";
        const COLLECTION: &'static str = "users";

        let db = Database::open(DB_NAME).await.unwrap();

        db.create(COLLECTION).await.unwrap();

        assert_eq!(db.has(COLLECTION).await, true);

        db.delete(COLLECTION).await.unwrap();

        assert_eq!(db.has(COLLECTION).await, false);

        delete(DB_NAME).await.unwrap();

        Ok(())
    }
}
