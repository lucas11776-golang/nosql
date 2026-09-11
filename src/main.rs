use std::sync::{Arc, LazyLock, RwLock};

use nosql::{Database, Result, connection::Connection};





// static CONNECTIONS: LazyLock<RwLock<>> =
//     LazyLock::new(|| RwLock::new(Connections::new()));


#[tokio::main]
async fn main() -> Result<()> {
    let db = Database::open("db.bin").await?;



    Connection::new("127.0.0.1", 2222)
        .await
        .unwrap()
        .accept()
        .await?;





    Ok(())
}
