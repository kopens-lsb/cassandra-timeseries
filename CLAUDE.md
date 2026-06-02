# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workflow, build, test, and style

See **[AGENTS.md](AGENTS.md)** — it is the source of truth for environment, build (`.build/sh/ai-build`), targeted testing (`.build/sh/ai-ci-test <FQCN>`), code style, the git workflow, and hard boundaries (never touch `src/gen-java/`, `lib/`, or the CQL grammar without asking). Do not duplicate or contradict it. The notes below cover only what AGENTS.md does not: the big-picture architecture.

Key reminders that bite agents:
- `ai-ci-test` runs a whole test **class**; there is no method-level filter. Never run the full suite.
- Always use the `.build/sh/ai-*` wrappers, never bare `ant` — they summarize logs and set the working directory.
- `modules/accord` is a git submodule (the Accord transaction engine, developed at `apache/cassandra-accord`).

## Architecture: the write and read paths

Cassandra is a leaderless, masterless distributed row store. Every node is a peer; any node can coordinate any request. The two most important flows to understand:

**Coordinator side** — [service/StorageProxy.java](src/java/org/apache/cassandra/service/StorageProxy.java) is the heart of distributed reads and writes. It uses the replication strategy + token ring ([locator/](src/java/org/apache/cassandra/locator/), [dht/](src/java/org/apache/cassandra/dht/)) to pick replicas, fans out messages over [net/](src/java/org/apache/cassandra/net/) (`MessagingService`), applies the requested `ConsistencyLevel`, and runs read repair. Each message type has a `*VerbHandler` (e.g. [db/MutationVerbHandler.java](src/java/org/apache/cassandra/db/MutationVerbHandler.java), [db/ReadCommandVerbHandler.java](src/java/org/apache/cassandra/db/ReadCommandVerbHandler.java)).

**Replica/storage side** — a write becomes a `Mutation` ([db/Mutation.java](src/java/org/apache/cassandra/db/Mutation.java)) applied to a `Keyspace` → `ColumnFamilyStore` ([db/ColumnFamilyStore.java](src/java/org/apache/cassandra/db/ColumnFamilyStore.java), the in-memory handle for one table). The write hits the commit log ([db/commitlog/](src/java/org/apache/cassandra/db/commitlog/)) for durability and a `Memtable` ([db/memtable/](src/java/org/apache/cassandra/db/memtable/)) in memory. Memtables flush to immutable **SSTables** on disk ([io/sstable/](src/java/org/apache/cassandra/io/sstable/)), which are merged over time by **compaction** ([db/compaction/](src/java/org/apache/cassandra/db/compaction/)). Reads merge the memtable with all relevant SSTables. The LSM data model — partitions, rows, cells, clusterings, tombstones — lives in [db/partitions/](src/java/org/apache/cassandra/db/partitions/), [db/rows/](src/java/org/apache/cassandra/db/rows/), and the many `Clustering*`/`*ReadCommand` classes directly under [db/](src/java/org/apache/cassandra/db/).

## Major subsystems (top-level packages under `src/java/org/apache/cassandra/`)

- **cql3/** — CQL parser, statements, and execution. Grammar is generated from `src/antlr/Cql.g` into `src/gen-java/` (both off-limits to edit). This is where a query string becomes a `ReadCommand`/`Mutation`.
- **tcm/** — Transactional Cluster Metadata: the newer log-based, linearizable mechanism for cluster/schema/topology changes. Largely replaces the old gossip-driven schema propagation; understand it before touching ring/membership/schema state.
- **gms/** — Gossip, used for liveness/failure detection and legacy state dissemination.
- **schema/** — table/keyspace/type definitions and the schema model.
- **service/** — node lifecycle and orchestration: [service/CassandraDaemon.java](src/java/org/apache/cassandra/service/CassandraDaemon.java) (startup/main), `StorageService` (ring membership, bootstrap/decommission, nodetool backend), `StartupChecks`.
- **transport/** — the native CQL protocol server (client-facing wire protocol).
- **net/** — internode messaging (Netty-based), verbs, and serialization.
- **streaming/** — bulk SSTable transfer between nodes (bootstrap, repair, rebuild).
- **repair/** + **service/ActiveRepairService** — anti-entropy repair (Merkle trees, sync).
- **db/view/** — materialized views; **index/** + **db/index** — secondary indexes (incl. SAI).
- **concurrent/** — the thread/stage execution model (`Stage`, custom executors); Cassandra uses a staged event-driven (SEDA-style) model.
- **config/** — `cassandra.yaml` mapping (`Config`, `DatabaseDescriptor`), the central config access point.
- **hints/**, **batchlog/**, **journal/** — hinted handoff, batched-write durability, and the generic append log.
- **db/marshal/** — `AbstractType` column type system (validation, comparison, serialization).
- **tools/** + **bin/** — `nodetool`, `cqlsh`, `sstable*` offline tools, stress.

## Tests

Test sources live under `test/`, split by kind (this maps to AGENTS.md's testing rules):
- `test/unit/` — JUnit unit/integration tests (the common case for `ai-ci-test`).
- `test/distributed/` — **jvm-dtest**: multi-node clusters in one JVM, the preferred way to test distributed behavior in Java.
- `test/burn/`, `test/long/`, `test/microbench/` (JMH), `test/simulator/` (deterministic simulation), `test/harry/` (property/fuzz). See [TESTING.md](TESTING.md) for what belongs where.
