### Rust BDC

#### The Rust Big-Data Cluster is an attempt to tackle a few personal goals I have set recently:

1. Learn Rust at a deeper level, i.e. async execution, OOB using structs and enums, error handling, SIMD
2. Learn about distributed systems (Gossip Glomers Challenge on Fly.io)
3. Better understand the technical process required to build a data querying engine such as Pandas, Polars, or Apache Spark


The main components of the system so far, include:
1. A message system to encode requests and responses
2. A cluster-node structure where the cluster is made up of, inter alia, nodes
3. Nodes that act as execution units to process requests
4. Cluster exceptions module to consolidate various kinds of errors into a single class
5. A transaction system to enable requests to be broken down into smaller execution steps
6. A datastore system where nodes can retain data in the form of a dataframe
7. A file system manager to manage various file ops such as reading from a config file or ingesting data from a file