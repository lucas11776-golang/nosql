use anyhow::{Result};
use nosql::Database;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let db = Database::open("db.bin").await?;

    // --- 1. CREATE/DROP/RENAME COLLECTION ---

    let collection = "test123";
    let collection_renamed = "collection_renamed";

    db.create(collection).await?;

    println!("\r\n NEW COLLECTIONS: {:?}", db.list().await);

    db.rename(collection, collection_renamed).await?;

    println!("\r\n RENAMED COLLECTIONS: {:?}", db.list().await);
    
    db.delete(collection_renamed).await.unwrap();

    println!("\r\n COLLECTIONS: {:?}", db.list().await);

    let users = db.collection("users").await;

    // --- 2. INSERT ONE ---
    println!("\n--- 1. INSERTING DOCUMENTS (Auto UUID _id) ---");
    let res1 = users
        .insert_one(json!({
            "name": "Alice",
            "email": "alice@example.com",
            "role": "developer",
            "status": "pending"
        }))
        .await?;
    println!("Inserted Alice with auto-generated UUID _id: {:?}", res1);

    let res2 = users
        .insert_one(json!({
            "name": "Bob",
            "email": "bob@example.com",
            "role": "developer",
            "status": "pending"
        }))
        .await?;
    println!("Inserted Bob with auto-generated UUID _id: {:?}", res2);

    let res3 = users
        .insert_one(json!({
            "_id": "custom_id",
            "name": "Charlie",
            "email": "charlie@example.com",
            "role": "admin",
            "status": "active"
        }))
        .await?;
    println!("Inserted Charlie with explicit _id: {:?}", res3);

    // --- 3. QUERYING ---
    println!("\n--- 2. QUERYING DOCUMENTS ---");
    let alice = users.find_one(json!({ "_id": res1.inserted_id })).await?;
    println!("Find Alice by UUID _id:\n{:#?}", alice);

    let developers = users.find(json!({ "role": "developer" })).await?;
    println!("Find All Developers: Found {} records", developers.len());

    // --- 4. BATCH UPDATE  ---
    println!("\n--- 3. UPDATING MULTIPLE DOCUMENTS ---");
    let update_res = users
        .update(
            json!({ "status": "pending" }),
            json!({ "status": "active", "verified": true }),
        )
        .await?;
    println!("Batch Update Result: {:?}", update_res);

    let updated_devs = users.find(json!({ "role": "developer" })).await?;
    println!("Developers after batch update:\n{:#?}", updated_devs);

    // --- 5. BATCH DELETE ---
    println!("\n--- 4. DELETING DOCUMENTS ---");
    let delete_res = users
        .delete(json!({ "role": "developer" }))
        .await?;
    println!("Batch Delete Result: {:?}", delete_res);

    let remaining_users = users.find(json!({})).await?;
    println!("Remaining users in collection:\n{:#?}", remaining_users);

    db.close().await?;
    println!("\nDatabase operation completed cleanly.");

    Ok(())
}