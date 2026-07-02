#ifndef STORE_KV_STORE_H
#define STORE_KV_STORE_H

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

struct StoreEntry
{
    std::string key;
    std::string value;
    long long expire_at_ms = 0;
};

class KVStore
{
public:
    virtual ~KVStore() = default;

    virtual bool put(const std::string &key, const std::string &value) = 0;
    virtual bool put_with_expire_at(const std::string &key,
                                    const std::string &value,
                                    long long expire_at_ms) = 0;
    virtual bool put_ttl(const std::string &key, const std::string &value, int ttl_seconds) = 0;
    virtual std::optional<std::string> get(const std::string &key) const = 0;
    virtual bool del(const std::string &key) = 0;
    virtual bool exists(const std::string &key) const = 0;
    virtual bool expire(const std::string &key, int ttl_seconds) = 0;
    virtual bool expire_at(const std::string &key, long long expire_at_ms) = 0;
    virtual long long ttl(const std::string &key) const = 0;
    virtual void set_max_keys(size_t max_keys) = 0;
    virtual size_t size() const = 0;
    virtual std::vector<StoreEntry> snapshot_entries() const = 0;
};

#endif
