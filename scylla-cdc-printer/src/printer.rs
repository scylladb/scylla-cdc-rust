use anyhow;
use async_trait::async_trait;
use chrono::NaiveDateTime;

use scylla_cdc::consumer::{CDCRow, Consumer, ConsumerFactory};

const OUTPUT_WIDTH: i64 = 72;

struct PrinterConsumer;

#[async_trait]
impl Consumer for PrinterConsumer {
    async fn consume_cdc(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        let mut row_to_print = String::new();
        // Print header with cdc-specific columns independent from the schema of base table
        // (cdc$stream_id, cdc$time, etc.)
        row_to_print.push_str(&print_row_change_header(&data));

        // Print columns dependent on the base schema
        // The appearing order of the columns is undefined
        let column_names = data.get_non_cdc_column_names();
        for column in column_names {
            let value_field_name = column.to_owned();
            let deleted_elems_field_name = column.to_owned() + "_deleted_elements";
            let is_value_deleted_field_name = column.to_owned() + "_deleted";

            if data.column_exists(column) {
                if let Some(value) = data.get_value(column) {
                    row_to_print.push_str(&print_field(
                        value_field_name.as_str(),
                        format!("{:?}", value).as_str(),
                    ));
                } else {
                    row_to_print.push_str(&print_field(value_field_name.as_str(), "null"));
                }
            }

            if data.collection_exists(column) {
                row_to_print.push_str(&print_field(
                    deleted_elems_field_name.as_str(),
                    format!("{:?}", data.get_deleted_elements(column)).as_str(),
                ));
            }

            if data.column_deletable(column) {
                row_to_print.push_str(&print_field(
                    is_value_deleted_field_name.as_str(),
                    format!("{:?}", data.is_value_deleted(column)).as_str(),
                ));
            }
        }

        // Print end line
        row_to_print.push_str(
            "└────────────────────────────────────────────────────────────────────────────┘\n",
        );
        println!("{}", row_to_print);

        Ok(())
    }
}

pub struct PrinterConsumerFactory;

#[async_trait]
impl ConsumerFactory for PrinterConsumerFactory {
    async fn new_consumer(&self) -> Box<dyn Consumer> {
        Box::new(PrinterConsumer)
    }
}

fn print_row_change_header(data: &CDCRow<'_>) -> String {
    let mut header_to_print = String::new();
    let stream_id = data.stream_id.to_string();
    let (secs, nanos) = data.time.get_timestamp().unwrap().to_unix();
    let timestamp = NaiveDateTime::from_timestamp_opt(secs as i64, nanos)
        .unwrap()
        .to_string();
    let operation = data.operation.to_string();
    let batch_seq_no = data.batch_seq_no.to_string();
    let end_of_batch = data.end_of_batch.to_string();
    let time_to_live = data.ttl.map_or("null".to_string(), |ttl| ttl.to_string());

    header_to_print.push_str(
        "┌──────────────────────────── Scylla CDC log row ────────────────────────────┐\n",
    );
    header_to_print.push_str(&print_field("Stream id:", &stream_id));
    header_to_print.push_str(&print_field("Timestamp:", &timestamp));
    header_to_print.push_str(&print_field("Operation type:", &operation));
    header_to_print.push_str(&print_field("Batch seq no:", &batch_seq_no));
    header_to_print.push_str(&print_field("End of batch:", &end_of_batch));
    header_to_print.push_str(&print_field("TTL:", &time_to_live));
    header_to_print.push_str(
        "├────────────────────────────────────────────────────────────────────────────┤\n",
    );
    header_to_print
}

fn print_field(field_name: &str, field_value: &str) -> String {
    let mut field_to_print = format!("│ {}: {}", field_name, field_value);
    let left_spaces: i64 =
        OUTPUT_WIDTH - field_name.chars().count() as i64 - field_value.chars().count() as i64;

    for _ in 0..left_spaces {
        field_to_print.push(' ');
    }

    field_to_print.push_str(" │\n");
    field_to_print
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time;

    use super::*;
    use chrono::Utc;
    use scylla_cdc::log_reader::CDCLogReaderBuilder;
    use scylla_cdc_test_utils::{populate_simple_db_with_pk, prepare_simple_db, TEST_TABLE};

    const SECOND_IN_MILLIS: u64 = 1_000;
    const SLEEP_INTERVAL: u64 = SECOND_IN_MILLIS / 10;
    const WINDOW_SIZE: u64 = SECOND_IN_MILLIS / 10 * 3;
    const SAFETY_INTERVAL: u64 = SECOND_IN_MILLIS / 10;

    #[tokio::test]
    async fn test_cdc_log_printer() {
        let start = Utc::now();
        let end = start + chrono::Duration::seconds(2);

        let (shared_session, ks) = prepare_simple_db().await.unwrap();

        let partition_key_1 = 0;
        let partition_key_2 = 1;
        populate_simple_db_with_pk(&shared_session, partition_key_1)
            .await
            .unwrap();
        populate_simple_db_with_pk(&shared_session, partition_key_2)
            .await
            .unwrap();

        let (mut _cdc_log_printer, handle) = CDCLogReaderBuilder::new()
            .session(shared_session)
            .keyspace(ks.as_str())
            .table_name(TEST_TABLE)
            .start_timestamp(start)
            .end_timestamp(end)
            .window_size(time::Duration::from_millis(WINDOW_SIZE))
            .safety_interval(time::Duration::from_millis(SAFETY_INTERVAL))
            .sleep_interval(time::Duration::from_millis(SLEEP_INTERVAL))
            .consumer_factory(Arc::new(PrinterConsumerFactory))
            .build()
            .await
            .expect("Creating cdc log printer failed!");

        handle.await.unwrap();
    }
}
