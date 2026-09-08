# NoSQL Database for Rust (`nosql`)

A simple, asynchronous, lightweight NoSQL-like document database for Rust, built on top of `tokio` and `serde_json`. It stores data in a single binary file using slotted page/collection-based storage and provides intuitive CRUD operations with JSON-like queries.

## Features

- **Asynchronous API**: Powered by `tokio`.
- **Collection Management**: Create, rename, delete, and list collections.
- **Unified Imports**: All types (`Database`, `Result`, `Value`, `json`) are conveniently re-exported directly from the root of the `nosql` crate.
- **JSON-like Document CRUD**:
  - Insert single or multiple documents with automatic UUID generation or custom `_id`s.
  - Query using matching JSON criteria (`find`, `find_one`).
  - Batch update matching documents.
  - Batch delete matching documents.
- **Embedded Storage**: Simple file-backed storage (e.g., `db.bin`).

## Installation

Add the following to your `Cargo.toml`:

```toml
[dependencies]
nosql = { path = "..." } # Replace with local path if used as a local dependency
tokio = { version = "1.0", features = ["full"] }
```

## Quick Start

Import everything you need directly from `nosql`:

```rust
use nosql::{Database, Result, Value, json};
```

### 1. Opening and Closing the Database

The database is backed by a binary file. Open it asynchronously using `Database::open`:

```rust
use nosql::{Database, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Open (or create) the database file
    let db = Database::open("db.bin").await?;

    // Perform database operations here...

    // Cleanly close and flush database state
    db.close().await?;
    Ok(())
}
```

### 2. Collection Management

You can create, list, rename, and delete collections dynamically.

```rust
// Create a new collection
db.create("users").await?;

// List all active collections
let collections = db.list().await?;
println!("Collections: {:?}", collections);

// Rename a collection
db.rename("users", "customers").await?;

// Delete a collection
db.delete("customers").await?;
```

### 3. Inserting Documents

To perform operations on documents, retrieve a handle to a collection:

```rust
let users = db.collection("users").await;
```

#### Insert One (With Auto-Generated UUID)
If no `_id` is specified, a UUID is automatically generated for the document:

```rust
use nosql::json;

let res = users.insert_one(json!({
    "name": "Alice",
    "email": "alice@example.com",
    "role": "developer",
    "status": "pending"
})).await?;

println!("Inserted ID: {:?}", res.inserted_id);
```

#### Insert One (With Custom ID)
You can specify a custom `_id` in your document:

```rust
let res = users.insert_one(json!({
    "_id": "custom_id_123",
    "name": "Charlie",
    "role": "admin"
})).await?;
```

#### Insert Many (Bulk Insert)
Insert multiple documents at once:

```rust
let docs = vec![
    json!({ "name": "Bob", "role": "developer" }),
    json!({ "name": "Dave", "role": "designer" }),
];
let res_list = users.insert_many(docs).await?;
```

### 4. Querying Documents

Retrieve documents by passing a matching JSON query filter.

#### Find One
Finds the first document that matches the given JSON filter:

```rust
// Find by auto-generated or custom _id
let user = users.find_one(json!({ "_id": "custom_id_123" })).await?;
println!("Found: {:#?}", user);
```

#### Find All / Match Multiple
Finds all documents matching the specified criteria:

```rust
// Find all developers
let developers = users.find(json!({ "role": "developer" })).await?;
println!("Found {} developers.", developers.len());

// Find all documents (empty filter)
let all_users = users.find(json!({})).await?;
```

### 5. Updating Documents

Update all documents matching a filter. Specify the query criteria and the new key-value pairs to set/merge:

```rust
// Update all users with "pending" status to "active" and add a "verified" flag
let update_res = users.update(
    json!({ "status": "pending" }),
    json!({ "status": "active", "verified": true }),
).await?;

println!("Updated {} documents.", update_res.modified_count);
```

### 6. Deleting Documents

Batch delete all documents matching a specified query criteria:

```rust
// Delete all users whose role is "developer"
let delete_res = users.delete(json!({ "role": "developer" })).await?;

println!("Deleted {} documents.", delete_res.deleted_count);
```

---

## Complete Example

Below is the complete example matching `src/main.rs`, illustrating the full lifecycle of database and collection management using the unified imports.

```rust
use nosql::{Database, Result, Value, json};

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

    // --- 3. INSERT MANY ---
    println!("\n--- 1. INSERTING MANY DOCUMENTS (Auto UUID _id) ---");
    let mut list: Vec<Value> = Vec::new();

    for i in 0..5 {
        list.push(json!({
            "name": format!("Peterson-{}", i),
            "email": format!("peterson-{}@gmail.com", i),
            "role": "employee",
            "status": "pending"
        }));
    }

    let res_list = users.insert_many(list).await.unwrap();

    println!("Inserted many users: {:?}", res_list);

    // --- 4. QUERYING ---
    println!("\n--- 2. QUERYING DOCUMENTS ---");
    let alice = users.find_one(json!({ "_id": res1.inserted_id })).await?;
    println!("Find Alice by UUID _id:\n{:#?}", alice);

    let developers = users.find(json!({ "role": "developer" })).await?;
    println!("Find All Developers: Found {} records", developers.len());

    // --- 5. BATCH UPDATE  ---
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

    // --- 6. BATCH DELETE ---
    println!("\n--- 4. DELETING DOCUMENTS ---");
    let delete_res = users.delete(json!({ "role": "developer" })).await?;
    println!("Batch Delete Result: {:?}", delete_res);

    let remaining_users = users.find(json!({})).await?;
    println!("Remaining users in collection:\n{:#?}", remaining_users);

    db.close().await?;
    println!("\nDatabase operation completed cleanly.");

    Ok(())
}
```
