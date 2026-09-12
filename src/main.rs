use std::sync::{Arc, LazyLock, RwLock};

use nosql::{Database, Result, connection::Connection};
use serde::{Deserialize, Serialize};

// static CONNECTIONS: LazyLock<RwLock<>> =
//     LazyLock::new(|| RwLock::new(Connections::new()));

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

    let usrs = users
        .find_one_as::<User>(nosql::json!({
            "email": "peterson-4@gmail.com",
        }))
        .await?;

    // for user in usrs {
    //     println!("USER: {:?}", user)
    // }

    println!("USER: {:?}", usrs);

    Ok(())
}
