# Project Structure

The active KV server implementation lives under `src/`.

```text
src/
  main.cpp                 Application entry point
  app/                     KV server lifecycle and TCP command loop
  cluster/                 Cluster node model, consistent hashing, request routing
  config/                  Runtime config schema, YAML parser, config loader
  log/                     Async/sync logging and blocking queue
  protocol/                KV text protocol and RESP request parsing/response encoding
  storage/                 KVStore interface, memory store, TTL/LRU, WAL and snapshot persistence
  net/                     Worker thread pool

configs/
  kvserver.yaml            Default single-node config
  cluster-node1.yaml       Local five-node cluster sample
  cluster-node2.yaml       Local five-node cluster sample
  cluster-node3.yaml       Local five-node cluster sample
  cluster-node4.yaml       Local five-node cluster sample
  cluster-node5.yaml       Local five-node cluster sample
  wal-test.yaml            WAL persistence test config

tests/
  unit/                    Unit tests
  integration/             Disabled integration tests requiring a running kvserver
tools/
  bench.cpp                RESP benchmark client
```

Legacy web-server source directories have been removed. The active build target
only uses `src/`.

## Module Responsibilities

### `src/main.cpp`

Process entry point. It creates `KVServer`, initializes it from CLI/config, and
enters the server loop.

### `src/app/`

Application orchestration layer. `KVServer` owns the runtime lifecycle:

- load config
- initialize logging
- initialize storage
- initialize cluster routing
- listen for TCP clients
- execute KV protocol requests

### `src/config/`

Runtime configuration layer.

- `server_config.h`: typed config schema
- `config_loader.*`: CLI + YAML merge logic
- `yaml_parser.*`: small YAML subset parser

### `src/protocol/`

KV text protocol layer.

- parses request lines into `Request`
- parses RESP Array frames from Redis clients into `Request`
- validates command arity
- encodes text or RESP responses

This layer does not touch storage or sockets.

### `src/storage/`

Local storage engine layer.

- `KVStore`: storage interface
- `MemoryStore`: thread-safe in-memory map with TTL and LRU eviction
- `WriteAheadLog`: append-only WAL file with truncation after snapshot
- `SnapshotFile`: full-state snapshot save/load
- `PersistentMemoryStore`: `MemoryStore` wrapped with snapshot load, WAL replay, and write-ahead persistence

### `src/cluster/`

Distributed routing layer.

- `ClusterNode`: node identity and endpoint
- `ConsistentHash`: maps keys to owner nodes
- `ClusterRouter`: health-checks peers, builds an alive-node ring, tracks the static Raft leader, chooses replica nodes, and forwards requests

Current cluster consistency supports best-effort replication and a static-leader
Raft mode with majority write commits. Automatic election and log conflict
repair are not implemented yet.

### `src/log/`

Logging utilities reused by the KV server.

- `Log`: sync/async log writer
- `block_queue`: blocking queue used by async logging and tested independently

### `configs/`

Runtime YAML files.

- `kvserver.yaml`: default single-node config
- `cluster-node1.yaml` through `cluster-node5.yaml`: local five-node cluster sample
- `wal-test.yaml`: WAL persistence test config

### `tests/`

Unit and disabled integration tests.

- unit tests cover protocol, storage, WAL, consistent hashing, and block queue
- integration tests are disabled by default because they require a running `kvserver`
