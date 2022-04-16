use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use scylla::query::Query;
use scylla::statement::Consistency;
use scylla::{Session, SessionBuilder};

pub const TEST_TABLE: &str = "t";
static UNIQUE_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn unique_name() -> String {
    let cnt = UNIQUE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let name = format!(
        "test_rust_{}_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        cnt
    );
    println!("unique_name: {}", name);
    name
}

fn get_create_table_query() -> String {
    format!("CREATE TABLE IF NOT EXISTS {} (pk int, t int, v text, s text, PRIMARY KEY (pk, t)) WITH cdc = {{'enabled':true}};", TEST_TABLE)
}

pub async fn create_test_db(session: &Arc<Session>) -> anyhow::Result<String> {
    let ks = unique_name();
    let mut create_keyspace_query = Query::new(format!(
        "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}};", ks
    ));
    create_keyspace_query.set_consistency(Consistency::All);

    session.query(create_keyspace_query, &[]).await?;
    session.await_schema_agreement().await?;
    session.use_keyspace(&ks, false).await?;

    // Create test table
    let create_table_query = get_create_table_query();
    session.query(create_table_query, &[]).await?;
    session.await_schema_agreement().await?;
    Ok(ks)
}

pub async fn populate_simple_db_with_pk(session: &Arc<Session>, pk: u32) -> anyhow::Result<()> {
    for i in 0..3 {
        session
            .query(
                format!(
                    "INSERT INTO {} (pk, t, v, s) VALUES ({}, {}, 'val{}', 'static{}');",
                    TEST_TABLE, pk, i, i, i
                ),
                &[],
            )
            .await?;
    }
    Ok(())
}

fn get_uri() -> String {
    std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string())
}

pub async fn prepare_simple_db() -> anyhow::Result<(Arc<Session>, String)> {
    let uri = get_uri();
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let shared_session = Arc::new(session);

    let ks = create_test_db(&shared_session).await?;
    Ok((shared_session, ks))
}
