# TinyKVServer

C++20 实现的轻量级分布式 KV 存储原型。

当前能力：

- 文本 KV 协议：`PING`、`PUT`、`GET`、`DEL`、`EXISTS`
- RESP 协议：兼容 Redis 客户端常用命令 `PING`、`SET`、`GET`、`DEL`、`EXISTS`
- 单机内存 KV 存储
- WAL + Snapshot 持久化恢复
- 一致性哈希路由
- 多节点请求转发
- 同步/异步日志
- YAML 配置

## Build

```bash
make kvserver
```

## Run

单节点：

```bash
./kvserver -f configs/kvserver.yaml
```

覆盖端口：

```bash
./kvserver -f configs/kvserver.yaml -p 19006
```

五节点本地集群：

```bash
make run-cluster-1
make run-cluster-2
make run-cluster-3
make run-cluster-4
make run-cluster-5
```

或直接运行：

```bash
./kvserver -f configs/cluster-node1.yaml  # 127.0.0.1:19021
./kvserver -f configs/cluster-node2.yaml  # 127.0.0.1:19022
./kvserver -f configs/cluster-node3.yaml  # 127.0.0.1:19023
./kvserver -f configs/cluster-node4.yaml  # 127.0.0.1:19024
./kvserver -f configs/cluster-node5.yaml  # 127.0.0.1:19025
```

## Persistence

默认单机配置启用 WAL 和 Snapshot：

```yaml
store:
  wal_enabled: true
  wal_file: "./data/node-1/kv.wal"
  snapshot_file: "./data/node-1/kv.snapshot"
  snapshot_threshold: 1000
```

写入会先追加 WAL；每累计 `snapshot_threshold` 次写操作后生成一次
snapshot，并截断 WAL。重启恢复时先加载 snapshot，再 replay 新 WAL。

## Protocol

文本协议：

```text
PING
PUT <key> <value>
GET <key>
DEL <key>
EXISTS <key>
```

示例：

```bash
printf 'PUT user:1 alice\nGET user:1\n' | nc -q 0 127.0.0.1 9006
```

返回：

```text
OK
VALUE alice
```

RESP / Redis 客户端：

```bash
redis-cli -p 9006 ping
redis-cli -p 9006 set user:1 alice
redis-cli -p 9006 get user:1
redis-cli -p 9006 exists user:1
redis-cli -p 9006 del user:1
```

没有 `redis-cli` 时可以直接发送 RESP 帧：

```bash
make demo-resp
```

## Test

```bash
make test
```

项目结构见 [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md)。
