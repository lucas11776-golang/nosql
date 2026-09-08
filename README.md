# NoSQL Database for Rust (`nosql`)

A simple, asynchronous, lightweight NoSQL-like document database for Rust, built on top of `tokio` and `serde_json`. It stores data in a single binary file using slotted page/collection-based storage and provides intuitive CRUD operations with JSON-like queries.

## Features

- **Asynchronous API**: Powered by `tokio`.
- **Collection Management**: Create, rename, delete, and list collections.
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
serde_json = "1.0"
tokio = { version = "1.0", features = ["full"] }
anyhow = "1.0"
```

## Quick Start

Here is a quick overview of how to open a database, perform collection operations, and execute CRUD queries.

### 1. Opening and Closing the Database

The database is backed by a binary file. Open it asynchronously using `Database::open`:

```rust
use nosql::Database;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
use serde_json::json;

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

Below is a complete, runnable example illustrating the full lifecycle of database and collection management.

```rust
use anyhow::Result;
use nosql::Database;
use serde_json::{Value, json};

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialize the database
    let db = Database::open("db.bin").await?;

    // 2. Collection Operations
    let collection = "test_collection";
    let renamed = "test_collection_renamed";

    db.create(collection).await?;
    println!("Collections after creation: {:?}", db.list().await?);

    db.rename(collection, renamed).await?;
    println!("Collections after rename: {:?}", db.list().await?);

    db.delete(renamed).await?;
    println!("Collections after deletion: {:?}", db.list().await?);

    // 3. Document Operations on "users" collection
    let users = db.collection("users").await;

    // --- Insert One ---
    let res1 = users.insert_one(json!({
        "name": "Alice",
        "email": "alice@example.com",
        "role": "developer",
        "status": "pending"
    })).await?;
    println!("Inserted Alice: {:?}", res1);

    // --- Insert One with Custom ID ---
    let res2 = users.insert_one(json!({
        "_id": "custom_id",
        "name": "Charlie",
        "role": "admin",
        "status": "active"
    })).await?;
    println!("Inserted Charlie with custom ID: {:?}", res2);

    // --- Insert Many ---
    let mut batch = Vec::new();
    for i in 1..=3 {
        batch.push(json!({
            "name": format!("User-{}", i),
            "email": format!("user{}@example.com", i),
            "role": "developer",
            "status": "pending"
        }));
    }
    users.insert_many(batch).await?;

    // --- Querying ---
    // Find single document
    let query_res = users.find_one(json!({ "_id": "custom_id" })).await?;
    println!("Query result for custom_id:\n{:#?}", query_res);

    // Find multiple documents
    let developers = users.find(json!({ "role": "developer" })).await?;
    println!("Found {} developers.", developers.len());

    // --- Updating ---
    let update_res = users.update(
        json!({ "status": "pending" }),
        json!({ "status": "active", "verified": true }),
    ).await?;
    println!("Updated documents: {:?}", update_res);

    // --- Deleting ---
    let delete_res = users.delete(json!({ "role": "developer" })).await?;
    println!("Deleted documents: {:?}", delete_res);

    // 4. Safely Close Database
    db.close().await?;
    println!("Database closed cleanly.");

    Ok(())
}
```
