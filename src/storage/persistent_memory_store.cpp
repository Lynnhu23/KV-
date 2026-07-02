#include "persistent_memory_store.h"

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
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_wal.append_put(key, value))
    {
        return false;
    }

    if (!m_memory.put(key, value))
    {
        return false;
    }
    ++m_ops_since_snapshot;
    return maybe_snapshot_locked();
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

bool PersistentMemoryStore::maybe_snapshot_locked()
{
    if (m_snapshot_threshold == 0 || m_ops_since_snapshot < m_snapshot_threshold)
    {
        return true;
    }

    if (!m_snapshot.save(m_memory.snapshot()))
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
