#ifndef STORE_PERSISTENT_MEMORY_STORE_H
#define STORE_PERSISTENT_MEMORY_STORE_H

#include "kv_store.h"
#include "memory_store.h"
#include "snapshot.h"
#include "wal.h"

#include <cstddef>
#include <mutex>
#include <string>

class PersistentMemoryStore : public KVStore
{
public:
    bool init(const std::string &wal_file);
    bool init(const std::string &wal_file,
              const std::string &snapshot_file,
              size_t snapshot_threshold);

    bool put(const std::string &key, const std::string &value) override;
    std::optional<std::string> get(const std::string &key) const override;
    bool del(const std::string &key) override;
    bool exists(const std::string &key) const override;

private:
    bool maybe_snapshot_locked();

private:
    MemoryStore m_memory;
    WriteAheadLog m_wal;
    SnapshotFile m_snapshot;
    std::mutex m_mutex;
    size_t m_snapshot_threshold = 1000;
    size_t m_ops_since_snapshot = 0;
};

#endif
