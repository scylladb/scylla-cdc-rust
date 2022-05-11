# scylla-cdc-rust
Scylla-cdc-rust is a library that enables consuming the 
[Scylla Change Data Capture Log](https://docs.scylladb.com/using-scylla/cdc/)
in Rust applications. 
The library automatically and transparently handles errors and topology changes 
of the underlying Scylla cluster. 
Thanks to that, the API allows the user to read the CDC log without having to deeply understand 
the internal structure of CDC.

It is recommended to get familiar with the documentation of CDC first, 
in order to understand the concept: 
<https://docs.scylladb.com/using-scylla/cdc/>

The library was written in pure Rust,
using [Scylla Rust Driver](https://crates.io/crates/scylla)
and [Tokio](https://crates.io/crates/tokio).

## Getting started
The best place to get started with the library is the [tutorial](tutorial.md). 
You can also check the [Scylla University lessons](https://university.scylladb.com/courses/scylla-operations/lessons/change-data-capture-cdc/)
to learn more about the CDC.

## Examples
The repository also contains two exemplary applications:

- [Printer](scylla-cdc-printer): prints all changes from CDC log for a selected table.
- [Replicator](scylla-cdc-replicator): replicates a table from one Scylla cluster to another by reading the CDC log.

## Contact
Use the GitHub Issues to report bugs or errors. 
You can also join [ScyllaDB-Users Slack channel](http://slack.scylladb.com/) and discuss on `#cdc` channel.

## Useful links

- [Scylla Rust Driver on GitHub](https://github.com/scylladb/scylla-rust-driver)
- [Scylla Docs - Change Data Capture (CDC)](https://docs.scylladb.com/using-scylla/cdc/)
- [Scylla University - Change Data Capture (CDC)](https://university.scylladb.com/courses/scylla-operations/lessons/change-data-capture-cdc/)
- [ScyllaDB YouTube - Change Data Capture in Scylla](https://www.youtube.com/watch?v=392Nbfrq7Dg)
- [ScyllaDB Blog - Using Change Data Capture (CDC) in Scylla](https://www.scylladb.com/2020/07/23/using-change-data-capture-cdc-in-scylla/)
- [scylla-cdc-java - A library for Java](https://github.com/scylladb/scylla-cdc-java)
- [scylla-cdc-go - A library for Go](https://github.com/scylladb/scylla-cdc-go)

## License
The library is licensed under Apache License 2.0. 
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 
or in the LICENSE.txt file in the repository.