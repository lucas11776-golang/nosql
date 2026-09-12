use nosql::{Database, Result};
use serde::{Deserialize, Serialize};

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
