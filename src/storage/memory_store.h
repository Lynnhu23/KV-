#ifndef STORE_MEMORY_STORE_H
#define STORE_MEMORY_STORE_H

#include "kv_store.h"

#include <chrono>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

class MemoryStore : public KVStore
{
public:
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
    std::unordered_map<std::string, std::string> snapshot() const;
    void clear();

private:
    struct Entry
    {
        std::string value;
        long long expire_at_ms = 0;
        std::list<std::string>::iterator lru_it;
    };

    static long long now_ms();
    static long long ttl_to_expire_at_ms(int ttl_seconds);
    bool expired(const Entry &entry, long long now) const;
    void touch_locked(const std::string &key) const;
    void evict_expired_locked() const;
    void enforce_capacity_locked() const;

private:
    mutable std::mutex m_mutex;
    mutable std::unordered_map<std::string, Entry> m_data;
    mutable std::list<std::string> m_lru;
    size_t m_max_keys = 0;
};

#endif
