# TinyKVServer

C++20 实现的轻量级分布式 KV 存储原型。

当前能力：

- 文本 KV 协议：`PING`、`PUT`、`GET`、`DEL`、`EXISTS`
- RESP 协议：兼容 Redis 客户端常用命令 `PING`、`SET`、`GET`、`DEL`、`EXISTS`
- 单机内存 KV 存储
- WAL + Snapshot 持久化恢复
- TTL 过期键
- LRU 内存淘汰
- 一致性哈希路由
- 健康检查与存活节点路由
- 多副本复制
- Raft 模式多数派提交
- 多节点请求转发
- 同步/异步日志
- YAML 配置
- benchmark 压测工具

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
make run-cluster
```

也可以分别启动：

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

集群配置会定期健康检查 peer。默认五节点配置使用 Raft 模式，leader 为 `node-1`，写入需要多数派确认后才返回成功。

```yaml
cluster:
  replication_factor: 2
  health_check_interval_ms: 1000
  consistency: "raft"
  leader_id: "node-1"
```

当前 Raft 实现是静态 leader + 多数派提交：follower 会把读写转发给 leader，leader 将写入复制到存活 follower，达到多数派后本地提交。暂未实现自动 leader 选举、任期持久化和日志冲突回退。

## Persistence

默认单机配置启用 WAL 和 Snapshot：

```yaml
store:
  wal_enabled: true
  wal_file: "./data/node-1/kv.wal"
  snapshot_file: "./data/node-1/kv.snapshot"
  snapshot_threshold: 1000
  max_keys: 0
```

写入会先追加 WAL；每累计 `snapshot_threshold` 次写操作后生成一次
snapshot，并截断 WAL。重启恢复时先加载 snapshot，再 replay 新 WAL。

`max_keys` 控制 LRU 容量，`0` 表示不限制。超过容量时会淘汰最近最少使用的 key。

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
redis-cli -p 9006 set session:1 token EX 60
redis-cli -p 9006 get user:1
redis-cli -p 9006 exists user:1
redis-cli -p 9006 ttl session:1
redis-cli -p 9006 del user:1
```

## Benchmark

启动服务后运行：

```bash
make bench
```

可覆盖参数：

```bash
make bench PORT=9006 REQUESTS=100000 CLIENTS=100
```

输出 QPS、平均延迟、p95 和 p99 延迟。

## Dashboard

启动 KVServer 后运行：

```bash
make dashboard
```

浏览器打开：

```text
http://127.0.0.1:8080
```

Dashboard 可以查看单机/五节点状态，执行 KV 命令、TTL 命令和本地压测。

没有 `redis-cli` 时可以直接发送 RESP 帧：

```bash
make demo-resp
```

## Test

```bash
make test
```

项目结构见 [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md)。
