#ifndef STORE_SNAPSHOT_H
#define STORE_SNAPSHOT_H

#include "kv_store.h"

#include <string>
#include <vector>

class SnapshotFile
{
public:
    explicit SnapshotFile(std::string path = "");

    void set_path(std::string path);
    const std::string &path() const;
    bool save(const std::vector<StoreEntry> &entries) const;
    bool load(KVStore &store) const;

private:
    static std::string escape(const std::string &value);
    static bool unescape(const std::string &value, std::string &out);

private:
    std::string m_path;
};

#endif
