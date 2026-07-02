CXX ?= g++
PORT ?= 9006
REQUESTS ?= 10000
CLIENTS ?= 50

DEBUG ?= 1
ifeq ($(DEBUG), 1)
    CXXFLAGS += -g -std=c++20
else
    CXXFLAGS += -O2 -std=c++20
endif

CXXFLAGS += -Isrc

SRC_DIR = ./src
CONFIG_SRC = $(SRC_DIR)/config/config_loader.cpp \
             $(SRC_DIR)/config/yaml_parser.cpp
LOG_SRC = $(SRC_DIR)/log/log.cpp
STORAGE_SRC = $(SRC_DIR)/storage/memory_store.cpp \
              $(SRC_DIR)/storage/wal.cpp \
              $(SRC_DIR)/storage/snapshot.cpp \
              $(SRC_DIR)/storage/persistent_memory_store.cpp
PROTOCOL_SRC = $(SRC_DIR)/protocol/kv_protocol.cpp
CLUSTER_SRC = $(SRC_DIR)/cluster/consistent_hash.cpp \
              $(SRC_DIR)/cluster/router.cpp
NET_SRC = $(SRC_DIR)/net/thread_pool.cpp
APP_SRC = $(SRC_DIR)/main.cpp \
          $(SRC_DIR)/app/kv_server.cpp \
          $(CONFIG_SRC) \
          $(LOG_SRC) \
          $(STORAGE_SRC) \
          $(PROTOCOL_SRC) \
          $(CLUSTER_SRC) \
          $(NET_SRC)

kvserver: $(APP_SRC)
	$(CXX) -o kvserver $^ $(CXXFLAGS) -lpthread

clean:
	rm -f kvserver bench

.PHONY: run demo demo-resp run-cluster run-cluster-1 run-cluster-2 run-cluster-3 run-cluster-4 run-cluster-5 demo-cluster bench dashboard compose-up compose-down test test-raft
run: kvserver
	./kvserver

demo:
	printf 'PING\nPUT user:1 alice\nGET user:1\n' | nc -N 127.0.0.1 $(PORT)

demo-resp:
	{ printf '*1\r\n$$4\r\nPING\r\n'; printf '*3\r\n$$3\r\nSET\r\n$$6\r\nuser:1\r\n$$5\r\nalice\r\n'; printf '*2\r\n$$3\r\nGET\r\n$$6\r\nuser:1\r\n'; printf '*2\r\n$$6\r\nEXISTS\r\n$$6\r\nuser:1\r\n'; } | nc -N 127.0.0.1 $(PORT)

run-cluster: kvserver
	@set -e; \
	./kvserver -f configs/cluster-node1.yaml & p1=$$!; \
	./kvserver -f configs/cluster-node2.yaml & p2=$$!; \
	./kvserver -f configs/cluster-node3.yaml & p3=$$!; \
	./kvserver -f configs/cluster-node4.yaml & p4=$$!; \
	./kvserver -f configs/cluster-node5.yaml & p5=$$!; \
	trap 'kill $$p1 $$p2 $$p3 $$p4 $$p5 2>/dev/null' INT TERM EXIT; \
	echo "TinyKV cluster running: 19021 19022 19023 19024 19025"; \
	wait

run-cluster-1: kvserver
	./kvserver -f configs/cluster-node1.yaml

run-cluster-2: kvserver
	./kvserver -f configs/cluster-node2.yaml

run-cluster-3: kvserver
	./kvserver -f configs/cluster-node3.yaml

run-cluster-4: kvserver
	./kvserver -f configs/cluster-node4.yaml

run-cluster-5: kvserver
	./kvserver -f configs/cluster-node5.yaml

demo-cluster:
	printf 'PING\nPUT user:1 alice\nGET user:1\nEXISTS user:1\n' | nc -N 127.0.0.1 19021

bench: tools/bench.cpp
	$(CXX) -o bench tools/bench.cpp -std=c++20 -lpthread
	./bench --port $(PORT) --requests $(REQUESTS) --clients $(CLIENTS)

dashboard:
	python3 tools/dashboard_server.py

compose-up:
	docker compose up --build

compose-down:
	docker compose down

test:
	cd tests && mkdir -p build && cd build && cmake .. && make && ctest --output-on-failure

test-raft: kvserver
	bash tests/integration/raft_fault_consistency.sh
