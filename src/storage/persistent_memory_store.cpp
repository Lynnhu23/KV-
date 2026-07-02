#include "persistent_memory_store.h"

#include <chrono>

bool PersistentMemoryStore::init(const std::string &wal_file)
{
    return init(wal_file, wal_file + ".snapshot", 1000);
}

bool PersistentMemoryStore::init(const std::string &wal_file,
                                 const std::string &snapshot_file,
                                 size_t snapshot_threshold)
{
    if (!m_wal.open(wal_file))
    {
        return false;
    }

    m_snapshot.set_path(snapshot_file);
    m_snapshot_threshold = snapshot_threshold;
    m_ops_since_snapshot = 0;

    m_memory.clear();
    if (!m_snapshot.load(m_memory))
    {
        return false;
    }
    return m_wal.replay(m_memory);
}

bool PersistentMemoryStore::put(const std::string &key, const std::string &value)
{
    return put_with_expire_at(key, value, 0);
}

bool PersistentMemoryStore::put_with_expire_at(const std::string &key,
                                               const std::string &value,
                                               long long expire_at_ms)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_wal.append_put(key, value, expire_at_ms))
    {
        return false;
    }

    if (!m_memory.put_with_expire_at(key, value, expire_at_ms))
    {
        return false;
    }
    ++m_ops_since_snapshot;
    return maybe_snapshot_locked();
}

bool PersistentMemoryStore::put_ttl(const std::string &key, const std::string &value, int ttl_seconds)
{
    if (ttl_seconds <= 0)
    {
        return false;
    }
    return put_with_expire_at(key, value, ttl_to_expire_at_ms(ttl_seconds));
}

std::optional<std::string> PersistentMemoryStore::get(const std::string &key) const
{
    return m_memory.get(key);
}

bool PersistentMemoryStore::del(const std::string &key)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_wal.append_del(key))
    {
        return false;
    }

    m_memory.del(key);
    ++m_ops_since_snapshot;
    return maybe_snapshot_locked();
}

bool PersistentMemoryStore::exists(const std::string &key) const
{
    return m_memory.exists(key);
}

bool PersistentMemoryStore::expire(const std::string &key, int ttl_seconds)
{
    if (ttl_seconds <= 0)
    {
        return false;
    }
    return expire_at(key, ttl_to_expire_at_ms(ttl_seconds));
}

bool PersistentMemoryStore::expire_at(const std::string &key, long long expire_at_ms)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_wal.append_expire(key, expire_at_ms))
    {
        return false;
    }
    if (!m_memory.expire_at(key, expire_at_ms))
    {
        return false;
    }
    ++m_ops_since_snapshot;
    return maybe_snapshot_locked();
}

long long PersistentMemoryStore::ttl(const std::string &key) const
{
    return m_memory.ttl(key);
}

void PersistentMemoryStore::set_max_keys(size_t max_keys)
{
    m_memory.set_max_keys(max_keys);
}

size_t PersistentMemoryStore::size() const
{
    return m_memory.size();
}

std::vector<StoreEntry> PersistentMemoryStore::snapshot_entries() const
{
    return m_memory.snapshot_entries();
}

bool PersistentMemoryStore::replace_with_snapshot(const std::vector<StoreEntry> &entries)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_memory.replace_with_snapshot(entries))
    {
        return false;
    }
    if (!m_snapshot.save(m_memory.snapshot_entries()))
    {
        return false;
    }
    if (!m_wal.reset())
    {
        return false;
    }
    m_ops_since_snapshot = 0;
    return true;
}

bool PersistentMemoryStore::maybe_snapshot_locked()
{
    if (m_snapshot_threshold == 0 || m_ops_since_snapshot < m_snapshot_threshold)
    {
        return true;
    }

    if (!m_snapshot.save(m_memory.snapshot_entries()))
    {
        return false;
    }
    if (!m_wal.reset())
    {
        return false;
    }

    m_ops_since_snapshot = 0;
    return true;
}

long long PersistentMemoryStore::ttl_to_expire_at_ms(int ttl_seconds)
{
    using namespace std::chrono;
    auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    return now + static_cast<long long>(ttl_seconds) * 1000;
}
