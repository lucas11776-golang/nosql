use std::str::FromStr;

use nosql::{Database, Result, json};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    pub _id: String,
    pub email: String,
    pub role: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let db = Database::open("db.bin").await?;

    let users = db.collection("users").await;

    let inserted = users
        .insert_one(json!({"email": "thembangubeni04@gmail.com"}))
        .await
        .unwrap();

    let _id = Uuid::from_str(&inserted.inserted_id.to_string().trim_matches('"')).unwrap();

    println!("\r\n\r\n{:?}\r\n\r\n", inserted.inserted_id);

    Ok(())
}
