#ifndef STORE_WAL_H
#define STORE_WAL_H

#include "kv_store.h"

#include <fstream>
#include <mutex>
#include <string>

class WriteAheadLog
{
public:
    bool open(const std::string &path);
    bool append_put(const std::string &key, const std::string &value);
    bool append_del(const std::string &key);
    bool replay(KVStore &store) const;
    bool reset();

private:
    static std::string escape(const std::string &value);
    static bool unescape(const std::string &value, std::string &out);

private:
    std::string m_path;
    std::ofstream m_out;
    mutable std::mutex m_mutex;
};

#endif
