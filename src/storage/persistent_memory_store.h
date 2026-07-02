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
    bool put_with_expire_at(const std::string &key,
                            const std::string &value,
                            long long expire_at_ms) override;
    bool put_ttl(const std::string &key, const std::string &value, int ttl_seconds) override;
    std::optional<std::string> get(const std::string &key) const override;
    bool del(const std::string &key) override;
    bool exists(const std::string &key) const override;
    bool expire(const std::string &key, int ttl_seconds) override;
    bool expire_at(const std::string &key, long long expire_at_ms) override;
    long long ttl(const std::string &key) const override;
    void set_max_keys(size_t max_keys) override;
    size_t size() const override;
    std::vector<StoreEntry> snapshot_entries() const override;
    bool replace_with_snapshot(const std::vector<StoreEntry> &entries) override;

private:
    bool maybe_snapshot_locked();
    static long long ttl_to_expire_at_ms(int ttl_seconds);

private:
    MemoryStore m_memory;
    WriteAheadLog m_wal;
    SnapshotFile m_snapshot;
    std::mutex m_mutex;
    size_t m_snapshot_threshold = 1000;
    size_t m_ops_since_snapshot = 0;
};

#endif
