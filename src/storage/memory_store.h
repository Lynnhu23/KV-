#ifndef STORE_MEMORY_STORE_H
#define STORE_MEMORY_STORE_H

#include "kv_store.h"

#include <shared_mutex>
#include <unordered_map>

class MemoryStore : public KVStore
{
public:
    bool put(const std::string &key, const std::string &value) override;
    std::optional<std::string> get(const std::string &key) const override;
    bool del(const std::string &key) override;
    bool exists(const std::string &key) const override;
    std::unordered_map<std::string, std::string> snapshot() const;
    void clear();

private:
    mutable std::shared_mutex m_mutex;
    std::unordered_map<std::string, std::string> m_data;
};

#endif
