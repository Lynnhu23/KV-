#ifndef STORE_KV_STORE_H
#define STORE_KV_STORE_H

#include <optional>
#include <string>

class KVStore
{
public:
    virtual ~KVStore() = default;

    virtual bool put(const std::string &key, const std::string &value) = 0;
    virtual std::optional<std::string> get(const std::string &key) const = 0;
    virtual bool del(const std::string &key) = 0;
    virtual bool exists(const std::string &key) const = 0;
};

#endif
