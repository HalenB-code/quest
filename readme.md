# Rust BDC

*A distributed data processing engine written in Rust.*

Rust BDC (Big Data Cluster) is a personal systems engineering project that explores the design and implementation of a distributed query engine inspired by platforms such as Pandas, Polars and Apache Spark.

Rather than relying on existing distributed computing frameworks, this project focuses on building the core components from first principles to better understand how modern data processing systems operate internally.

---

## Project Goals

This project exists to explore several technical areas simultaneously:

- Learn Rust beyond the basics, including:
  - asynchronous execution with Tokio
  - ownership and borrowing in large systems
  - trait-based design
  - error handling
  - concurrency
  - SIMD optimisation (future)

- Learn distributed systems concepts through practical implementation, including:
  - node communication
  - message passing
  - transaction coordination
  - fault handling
  - distributed execution
  - ideas inspired by the Fly.io Gossip Glomers challenges

- Develop a deeper understanding of how distributed analytics engines such as Apache Spark execute queries across multiple machines.

---

## Current Architecture

The project currently consists of several core subsystems.

### Message Framework

A strongly typed messaging system used to encode requests and responses between the cluster and worker nodes.

---

### Cluster

The cluster coordinates execution across multiple nodes.

Responsibilities include:

- receiving client requests
- distributing work
- tracking transactions
- coordinating multi-step execution
- collecting and aggregating results

---

### Nodes

Nodes act as distributed execution units.

Each node is responsible for:

- processing requests
- maintaining local state
- executing query plan steps
- reading and writing local data
- communicating execution results back to the cluster

---

### Transaction Manager

Requests are decomposed into smaller execution steps.

The transaction system tracks:

- transaction lifecycle
- execution state
- query plans
- rollback and recovery (future)

---

### Data Store

Each node maintains a local in-memory dataframe used for query execution.

Future work will extend this into a larger distributed storage model.

---

### File System Manager

Provides common filesystem operations including:

- configuration loading
- data ingestion
- intermediate file storage
- output management

---

### Error Handling

A unified error framework provides consistent error reporting throughout the system.

---

## Current Development

The project is currently focused on implementing distributed **Group By** operations.

The initial implementation follows a MapReduce-style execution model:

1. The cluster distributes work to worker nodes.
2. Each node performs a local aggregation.
3. Intermediate results are either:
   - returned directly when sufficiently small, or
   - written to disk when they exceed message size limits.
4. The cluster gathers intermediate results.
5. A final aggregation produces the result returned to the client.

This work lays the foundation for a more complete distributed query engine.

---

## Future Roadmap

Some of the planned areas of development include:

- distributed joins
- filtering and projection
- query optimisation
- execution planning
- shuffle operations
- fault tolerance
- replication
- monitoring and reporting dashboard
- storage engine improvements
- benchmarking and performance tuning
- SIMD optimisations
- distributed scheduling

---

## Why This Project?

Many excellent distributed data processing frameworks already exist. The purpose of Rust BDC is not to replace them, but to understand how they work by implementing their fundamental concepts from the ground up.

Building the system from first principles provides practical experience with distributed systems, concurrency, query execution, and systems programming in Rust—knowledge that is difficult to gain by only using existing frameworks.

---

## Project Status

**🚧 Early Development**

This repository is an educational systems engineering project under active development. The primary objective is to explore distributed systems and Rust by building a functioning analytics engine from the ground up.

Contributions, discussions, and feedback are always welcome.