# Tutorial for scylla-cdc-rust

This document will guide you through all steps of creating an example 
application using the library. 
We're going to create a simpler version of the [printer](scylla-cdc-printer). 

For the purpose of this tutorial, we're going to assume that the table
we want to observe the logs for was created by using the following query:

```cassandraql
CREATE TABLE ks.t (pk int, ck int, v int, vs set<int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': 'true'}
```

## Creating the CDC consumer
The most important part of using the library is to define a callback that will be executed 
after reading a CDC log from the database. 
Such callback is defined by implementing the `Consumer` trait located in `scylla-cdc::consumer`. 
As for now, we will define a struct with no member variables for this purpose:
```rust
struct TutorialConsumer;
```
Since the callback will be executed asynchronously, 
we have to use the [async-trait](https://crates.io/crates/async-trait) crate 
to implement the `Consumer` trait. 
We also use the [anyhow](https://crates.io/crates/anyhow) crate for error handling.
```rust
use anyhow;
use async_trait::async_trait;
use scylla_cdc::consumer::*;

struct TutorialConsumer;

#[async_trait]
impl Consumer for TutorialConsumer {
    async fn consume_cdc(&mut self, _data: CDCRow<'_>) -> anyhow::Result<()> {
        println!("Hello, scylla-cdc!");
        Ok(())
    }
}
```

We'll cover on how to use the `CDCRow` structure in [Using CDCRow](tutorial.md#using-cdcrow).

The library is going to create one instance of `TutorialConsumer` per CDC stream, so we also need to define
a `ConsumerFactory` for them:
```rust
struct TutorialConsumerFactory;

#[async_trait]
impl ConsumerFactory for TutorialConsumerFactory {
    async fn new_consumer(&self) -> Box<dyn Consumer> {
        Box::new(TutorialConsumer)
    }
}
```

### Adding shared state to the consumer
Different instances of `Consumer` are being used in separate Tokio tasks. 
Due to that, the runtime might schedule them on separate threads.

Because of that, a struct implementing the `Consumer` trait should also implement `Send` trait 
and a struct implementing the `ConsumerFactory` trait should implement `Send` and `Sync` traits.
Luckily, Rust implements these traits by default if all member variables of a struct implement them.

If the consumers need to share some state, like a reference to an object, 
they can be wrapped in an [Arc](<https://doc.rust-lang.org/std/sync/struct.Arc.html>).

An example of that might be a `Consumer` that counts rows read by all its instances:

```rust
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingConsumer {
    counter: Arc<AtomicUsize>
}

#[async_trait]
impl Consumer for CountingConsumer {
    async fn consume_cdc(&mut self, _: CDCRow<'_>) -> anyhow::Result<()> {
        let curr = self.counter.fetch_add(1, Ordering::SeqCst);
        println!("Row no.{}", curr + 1);
        Ok(())
    }
}
```

__Note__: in general, keeping a mutable state in the `Consumer` is not recommended, 
since it requires synchronization (i.e. a mutex or an atomic like `AtomicUsize`),
which reduces the speedup granted by Tokio by running the `Consumer` logic on multiple cores.

## Starting the application
Now we're ready to create our `main` function:
```rust
use scylla::SessionBuilder;
use scylla_cdc::log_reader::CDCLogReaderBuilder;
use std::sync::Arc;
use std::time::SystemTime;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let session = Arc::new(
        SessionBuilder::new()
            .known_node("127.0.0.1:9042")
            .build()
            .await?,
    );
    let end = chrono::Duration::from_std(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap(),
    ).unwrap();
    let start = end - chrono::Duration::seconds(360);
    
    let factory = Arc::new(TutorialConsumerFactory);
    
    let (_, handle) = CDCLogReaderBuilder::new()
        .session(session)
        .keyspace("ks")
        .table_name("t")
        .start_timestamp(start)
        .end_timestamp(end)
        .consumer_factory(factory)
        .build()
        .await
        .expect("Creating the log reader failed!");

    handle.await
}
```
As we can see, we have to configure a few things in order to start the log reader:

- First, we have to create a connection to the database, 
using the `Session` struct from [Scylla Rust Driver](https://crates.io/crates/scylla).
- Second, specify the keyspace and the table name.
- Then, we create time bounds for our reader. 
This step is not compulsory - by default the reader will start reading from now and will continue reading forever.
In our case, we are going to read all logs added during the last 6 minutes.
- Next, create the factory. 
- Now, we can build the log reader.

To see more information about possible configuration of the log reader, see the [documentation](https://docs.rs/scylla-cdc/latest/scylla-cdc/log_reader/index.html).
Notice, that the `Session` with the database and the factory must be wrapped inside an `Arc`.

After creating the log reader we can `await` the handle it returns 
so that our application will terminate as soon as the reader finishes.

Now - let's insert some rows into the table. 
After inserting 3 rows and running the application, you should see the output:
``` 
Hello, scylla-cdc!
Hello, scylla-cdc!
Hello, scylla-cdc!
```
The application printed one line for each CDC log consumed. 
Now we can proceed and see how to use `CDCRow` struct to process the data.

## Using CDCRow
Having learned how to run the log reader, we can finally create the main logic of our application.
We will edit the `consume_cdc` function of the `TutorialConsumer`.

In general, there are four types of columns in CDC. You can access them in the following way:

- CDC metadata columns can be accessed directly as member variables (e.g. `data.stream_id`)
- values of columns can be accessed by method `get_value` or `take_value`
- to check if a column was deleted in given operation, use `is_value_deleted`
- to check which elements were deleted from a collection, use `get_deleted_elements` or `take_deleted_elements`

The data returned is of type [CqlValue](<https://docs.rs/scylla/latest/scylla/frame/response/result/enum.CqlValue.html>)
from the driver.

For more detailed information about how to interpret these values, 
check the [CDC documentation](<https://docs.scylladb.com/using-scylla/cdc/cdc-log-table/>).

It is assumed that the user knows the metadata of their table, 
but they can check if such a column exists in the CDC log, e.g. with method `column_exists`. 
Refer to the [API documentation](https://docs.rs/scylla-cdc/latest/scylla-cdc/consumer/struct.CDCRow.html) to find an appropriate method.

Back to our application, we will add printing of the data from the table:
```rust
async fn consume_cdc(&mut self, mut data: CDCRow<'_>) -> anyhow::Result<()> {
    println!("_________");
    println!("Time UUID: {:?}, operation type: {:?}", data.time, data.operation);
    println!("pk: {}, ck: {}",
             data.take_value("pk").unwrap().as_int().unwrap(),
             data.take_value("ck").unwrap().as_int().unwrap());
    let v = match data.take_value("v") {
        Some(val) => Some(val.as_int().unwrap()),
        None => None,
    };
    println!("v: {:?}, was deleted: {}",
             v,
             data.is_value_deleted("v"));
    let vs = match data.take_value("vs") {
        Some(val) => Some(val.into_vec().unwrap()),
        None => None,
    };
    println!("vs: {:?}, was deleted: {}, deleted elements: {:?}",
             vs,
             data.is_value_deleted("vs"),
             data.take_deleted_elements("vs"));
    println!("_________");
    Ok(())
}
```
An example of the output:
``` 
_________
Time UUID: 41183ab8-d07c-11ec-d982-2470743b0298, operation type: RowUpdate
pk: 7, ck: 5
v: None, was deleted: false
vs: None, was deleted: false, deleted elements: [Int(6)]
_________
_________
Time UUID: cc07e47a-d07c-11ec-ebc7-e1924abf3e16, operation type: RowUpdate
pk: 1, ck: 2
v: Some(8), was deleted: false
vs: None, was deleted: false, deleted elements: []
_________
```

__Note__: CDCRow's lifetime does not allow it to exist after the function terminates.
If the user wants to save the data for later 
(e.g. in a consumer that saves all consumed rows in a `vec`)
they should map the rows to another data structure.
## Saving progress
User can periodically save progress while consuming CDC logs and restore it in case of some error.
This way, the reading can start from last saved checkpoint instead of a timestamp given by the user.

Progress is identified by checkpoints using the `Checkpoint` struct defined as follows:
```rust
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    pub timestamp: Duration,
    pub stream_id: StreamID,
    pub generation: GenerationTimestamp,
}
```

Managing saving and restoring is used by implementing `CDCCheckpointSaver` trait defined as follows:
```rust
#[async_trait]
pub trait CDCCheckpointSaver: Send + Sync {
    async fn save_checkpoint(&self, checkpoint: &Checkpoint) -> anyhow::Result<()>;
    async fn save_new_generation(&self, generation: &GenerationTimestamp) -> anyhow::Result<()>;
    async fn load_last_generation(&self) -> anyhow::Result<Option<GenerationTimestamp>>;
    async fn load_last_checkpoint(
        &self,
        stream_id: &StreamID,
    ) -> anyhow::Result<Option<chrono::Duration>>;
}
```
User can use any way they want to manage checkpoints, for example they can store it in a database or in a file. 
There is a default implementation that uses ScyllaDB called `TableBackedCheckpointSaver`. 
Its source code is located in file [checkpoints.rs](scylla-cdc/src/checkpoints.rs) and can serve as an inspiration how to write new checkpoint savers.

Example of usage for `TableBackedCheckpointSaver`:
```rust
let user_checkpoint_saver = Arc::new(
    TableBackedCheckpointSaver::new_with_default_ttl(session, "ks", "checkpoints")
        .await
        .unwrap(),
);
```
This example will create checkpoint_saver that will save checkpoints in `keyspace.table_name` table using default TTL of 7 days.
User can also explicitly provide TTL:
```rust
let ttl: i64 = 3600; // TTL of 3600 seconds (one hour).
let user_checkpoint_saver = Arc::new(
    TableBackedCheckpointSaver::new(session, "ks", "checkpoints", ttl)
        .await
        .unwrap(),
);
```
__Note__: TTL is measured in seconds.


To save progress, the user needs to enable it while building the `CDCLogReader` and provide an `Arc` containing an object that implements `CDCCheckpointSaver`. 
```rust
let (log_reader, handle) = CDCLogReaderBuilder::new()
    // ...
    .should_save_progress(true) // Mark that we want to save progress.
    .should_load_progress(true) // Mark that we want to start consuming CDC logs from the last saved checkpoint.
    .pause_between_saves(time::Duration::from_millis(100)) // Save progress each 100 ms. If not specified, a default value of 10 seconds is used.
    .checkpoint_saver(user_checkpoint_saver) // Use `user_checkpoint_saver to manage checkpoints.
    .build();
```
__Note__: Setting saving/loading progress requires also setting checkpoint_saver to be used.
