#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR="/tmp/tinykv-raft-fault-test"
BASE_PORT="${BASE_PORT:-19520}"
REDIS_CLI="${REDIS_CLI:-redis-cli}"

if ! command -v "$REDIS_CLI" >/dev/null 2>&1; then
    echo "redis-cli is required for raft fault tests" >&2
    exit 2
fi

mkdir -p "$TMP_DIR"
rm -rf "$TMP_DIR/data"

pids=()
ports=()

cleanup() {
    for pid in "${pids[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT

make_configs() {
    local peers=""
    for i in 1 2 3 4 5; do
        local port=$((BASE_PORT + i))
        ports[$i]="$port"
        if [[ -n "$peers" ]]; then
            peers+=","
        fi
        peers+="node-$i@127.0.0.1:$port"
    done

    for i in 1 2 3 4 5; do
        local port="${ports[$i]}"
        local cfg="$TMP_DIR/node$i.yaml"
        sed -e "s/port: 1902[1-5]/port: $port/" \
            -e "s#data_dir: \"./data/node-$i\"#data_dir: \"$TMP_DIR/data/node-$i\"#" \
            -e "s#wal_file: \"./data/node-$i/kv.wal\"#wal_file: \"$TMP_DIR/data/node-$i/kv.wal\"#" \
            -e "s#snapshot_file: \"./data/node-$i/kv.snapshot\"#snapshot_file: \"$TMP_DIR/data/node-$i/kv.snapshot\"#" \
            -e "s/snapshot_threshold: 1000/snapshot_threshold: 3/" \
            -e "s#peers: \".*\"#peers: \"$peers\"#" \
            "$ROOT_DIR/configs/cluster-node$i.yaml" > "$cfg"
    done
}

start_node() {
    local i="$1"
    "$ROOT_DIR/kvserver" -f "$TMP_DIR/node$i.yaml" >/tmp/tinykv-raft-node$i.out 2>&1 &
    pids[$i]=$!
}

stop_node() {
    local i="$1"
    if [[ -n "${pids[$i]:-}" ]]; then
        kill "${pids[$i]}" 2>/dev/null || true
        wait "${pids[$i]}" 2>/dev/null || true
        pids[$i]=""
    fi
}

text_cmd() {
    local port="$1"
    local command="$2"
    printf '%s\n' "$command" | nc -N 127.0.0.1 "$port" 2>/dev/null || true
}

wait_for_ping() {
    local port="$1"
    for _ in $(seq 1 50); do
        if [[ "$(text_cmd "$port" PING)" == "PONG" ]]; then
            return 0
        fi
        sleep 0.1
    done
    echo "node on port $port did not become ready" >&2
    return 1
}

status_for() {
    local port="$1"
    text_cmd "$port" RAFT_STATUS
}

wait_for_leader() {
    for _ in $(seq 1 80); do
        for i in 1 2 3 4 5; do
            if [[ -z "${pids[$i]:-}" ]]; then
                continue
            fi
            local status
            status="$(status_for "${ports[$i]}")"
            if [[ "$status" == *"role=leader"* ]]; then
                echo "$i"
                return 0
            fi
        done
        sleep 0.25
    done
    echo "leader was not elected" >&2
    return 1
}

redis_set() {
    "$REDIS_CLI" -p "$1" set "$2" "$3" 2>/dev/null
}

redis_get() {
    "$REDIS_CLI" -p "$1" get "$2" 2>/dev/null
}

expect_eq() {
    local actual="$1"
    local expected="$2"
    local message="$3"
    if [[ "$actual" != "$expected" ]]; then
        echo "FAIL: $message: expected '$expected', got '$actual'" >&2
        exit 1
    fi
    echo "PASS: $message"
}

make_configs

echo "case 1: majority write/read on 3 of 5 nodes"
for i in 1 2 3; do
    start_node "$i"
    wait_for_ping "${ports[$i]}"
done
leader="$(wait_for_leader)"
expect_eq "$(redis_set "${ports[$leader]}" fault:majority ok)" "OK" "majority write succeeds"
sleep 0.5
expect_eq "$(redis_get "${ports[2]}" fault:majority)" "ok" "majority value is readable from follower"
for i in 1 2 3; do
    stop_node "$i"
done

echo "case 2: minority cannot elect leader or commit"
rm -rf "$TMP_DIR/data"
for i in 1 2; do
    start_node "$i"
    wait_for_ping "${ports[$i]}"
done
sleep 3
minority_result="$(redis_set "${ports[1]}" fault:minority bad || true)"
if [[ "$minority_result" != *"leader_unavailable"* && "$minority_result" != *"raft_no_quorum"* ]]; then
    echo "FAIL: minority write should be rejected, got '$minority_result'" >&2
    exit 1
fi
echo "PASS: minority write is rejected"
for i in 1 2; do
    stop_node "$i"
done

echo "case 3: leader failure triggers new election"
rm -rf "$TMP_DIR/data"
for i in 1 2 3 4 5; do
    start_node "$i"
    wait_for_ping "${ports[$i]}"
done
leader="$(wait_for_leader)"
expect_eq "$(redis_set "${ports[$leader]}" fault:before-fail before)" "OK" "write before leader failure"
stop_node "$leader"
new_leader="$(wait_for_leader)"
if [[ "$new_leader" == "$leader" ]]; then
    echo "FAIL: stopped leader is still reported as leader" >&2
    exit 1
fi
expect_eq "$(redis_set "${ports[$new_leader]}" fault:after-fail after)" "OK" "write after leader failure"
sleep 0.5
for i in 1 2 3 4 5; do
    if [[ -n "${pids[$i]:-}" ]]; then
        expect_eq "$(redis_get "${ports[$i]}" fault:before-fail)" "before" "surviving node-$i keeps pre-failure value"
        expect_eq "$(redis_get "${ports[$i]}" fault:after-fail)" "after" "surviving node-$i receives post-failure value"
    fi
done
for i in 1 2 3 4 5; do
    stop_node "$i"
done

echo "case 4: late joiner catches up missing raft log entries"
rm -rf "$TMP_DIR/data"
for i in 1 2 3; do
    start_node "$i"
    wait_for_ping "${ports[$i]}"
done
leader="$(wait_for_leader)"
expect_eq "$(redis_set "${ports[$leader]}" fault:old old)" "OK" "write before late joiner"
start_node 4
wait_for_ping "${ports[4]}"
sleep 1
leader="$(wait_for_leader)"
expect_eq "$(redis_set "${ports[$leader]}" fault:new new)" "OK" "write after late joiner"
sleep 1
expect_eq "$(redis_get "${ports[4]}" fault:old)" "old" "late joiner catches old entry"
expect_eq "$(redis_get "${ports[4]}" fault:new)" "new" "late joiner receives new entry"
for i in 1 2 3 4; do
    stop_node "$i"
done

echo "case 5: committed raft log survives full cluster restart"
rm -rf "$TMP_DIR/data"
for i in 1 2 3; do
    start_node "$i"
    wait_for_ping "${ports[$i]}"
done
leader="$(wait_for_leader)"
expect_eq "$(redis_set "${ports[$leader]}" fault:persist durable)" "OK" "write before full restart"
sleep 1
for i in 1 2 3; do
    stop_node "$i"
done
for i in 1 2 3; do
    start_node "$i"
    wait_for_ping "${ports[$i]}"
done
leader="$(wait_for_leader)"
expect_eq "$(redis_get "${ports[$leader]}" fault:persist)" "durable" "value survives full restart"
status="$(status_for "${ports[$leader]}")"
if [[ "$status" != *"last_log="* ]]; then
    echo "FAIL: raft status after restart did not expose last_log: $status" >&2
    exit 1
fi
echo "PASS: raft status is available after restart"
for i in 1 2 3; do
    stop_node "$i"
done

echo "case 6: raft snapshot compacts log and catches up stale follower"
rm -rf "$TMP_DIR/data"
for i in 1 2 3 4 5; do
    start_node "$i"
    wait_for_ping "${ports[$i]}"
done
leader="$(wait_for_leader)"
sleep 1
leader="$(wait_for_leader)"
for n in 1 2 3 4 5; do
    expect_eq "$(redis_set "${ports[$leader]}" "fault:snap:$n" "v$n")" "OK" "snapshot warmup write $n"
done
sleep 0.5
metrics="$(text_cmd "${ports[$leader]}" METRICS)"
snapshot_index="$(printf '%s\n' "$metrics" | awk '/tinykv_raft_snapshot_last_included_index/ {print $2}')"
if [[ -z "$snapshot_index" || "$snapshot_index" -le 0 ]]; then
    echo "FAIL: expected leader snapshot index > 0, metrics: $metrics" >&2
    exit 1
fi
echo "PASS: leader compacted raft log at snapshot index $snapshot_index"

stale=3
if [[ "$stale" == "$leader" ]]; then
    stale=2
fi
stop_node "$stale"
for n in 6 7 8 9 10; do
    expect_eq "$(redis_set "${ports[$leader]}" "fault:snap:$n" "v$n")" "OK" "write while follower-$stale is down $n"
done
start_node "$stale"
wait_for_ping "${ports[$stale]}"
sleep 1
leader="$(wait_for_leader)"
expect_eq "$(redis_set "${ports[$leader]}" fault:snap:trigger trigger)" "OK" "trigger snapshot catch-up"
sleep 1
expect_eq "$(redis_get "${ports[$stale]}" fault:snap:10)" "v10" "stale follower catches up via snapshot"
expect_eq "$(redis_get "${ports[$stale]}" fault:snap:trigger)" "trigger" "stale follower continues after snapshot"

echo "all raft fault consistency tests passed"
