use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::unprepared::Statement;

pub const TEST_TABLE: &str = "t";
static UNIQUE_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn now() -> chrono::Duration {
    chrono::Duration::milliseconds(chrono::Local::now().timestamp_millis())
}

pub fn unique_name() -> String {
    let cnt = UNIQUE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let name = format!("test_rust_{}_{}", now().num_seconds(), cnt);
    println!("unique_name: {name}");
    name
}

fn get_create_table_query() -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {TEST_TABLE} (pk int, t int, v text, s text, PRIMARY KEY (pk, t)) WITH cdc = {{'enabled':true}};"
    )
}

fn get_create_keyspace_query(
    keyspace: &str,
    replication_factor: u8,
    tablets_enabled: bool,
) -> String {
    let tablets_clause = if tablets_enabled {
        " AND tablets={'enabled': true}"
    } else {
        " AND tablets={'enabled': false}"
    };

    format!(
        "CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}{tablets_clause};"
    )
}

pub async fn create_test_db(
    session: &Arc<Session>,
    schema: &[String],
    replication_factor: u8,
    tablets_enabled: bool,
) -> anyhow::Result<String> {
    let ks = unique_name();
    let mut create_keyspace_query = Statement::new(get_create_keyspace_query(
        &ks,
        replication_factor,
        tablets_enabled,
    ));
    create_keyspace_query.set_consistency(Consistency::All);

    session.query_unpaged(create_keyspace_query, &[]).await?;
    session.await_schema_agreement().await?;
    session.use_keyspace(&ks, false).await?;

    // Create test tables
    for query in schema {
        session.query_unpaged(query.clone(), &[]).await?;
    }
    session.await_schema_agreement().await?;
    Ok(ks)
}

pub async fn populate_simple_db_with_pk(session: &Arc<Session>, pk: u32) -> anyhow::Result<()> {
    for i in 0..3 {
        session
            .query_unpaged(
                format!(
                    "INSERT INTO {TEST_TABLE} (pk, t, v, s) VALUES ({pk}, {i}, 'val{i}', 'static{i}');",
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

#[derive(Debug)]
pub struct CdcWithTabletsNotSupported(pub String);

impl fmt::Display for CdcWithTabletsNotSupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CDC with tablets is not supported: {}", self.0)
    }
}

impl std::error::Error for CdcWithTabletsNotSupported {}

pub async fn prepare_db(
    schema: &[String],
    replication_factor: u8,
    tablets_enabled: bool,
) -> anyhow::Result<(Arc<Session>, String)> {
    let uri = get_uri();
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    let shared_session = Arc::new(session);

    match create_test_db(&shared_session, schema, replication_factor, tablets_enabled).await {
        Ok(ks) => Ok((shared_session, ks)),
        Err(e) => {
            let msg = e.to_string();
            if tablets_enabled && msg.contains("issue #16317") {
                Err(anyhow::Error::new(CdcWithTabletsNotSupported(msg)))
            } else {
                Err(e)
            }
        }
    }
}

pub async fn prepare_simple_db(tablets_enabled: bool) -> anyhow::Result<(Arc<Session>, String)> {
    prepare_db(&[get_create_table_query()], 1, tablets_enabled).await
}

#[macro_export]
macro_rules! skip_if_not_supported {
    ($expr:expr) => {
        match $expr.await {
            Ok(val) => val,
            Err(e) => {
                if let Some(src) = e
                    .root_cause()
                    .downcast_ref::<scylla_cdc_test_utils::CdcWithTabletsNotSupported>()
                {
                    eprintln!("Skipping test: {}", src);
                    return;
                } else {
                    panic!("failed: {e}");
                }
            }
        }
    };
}
