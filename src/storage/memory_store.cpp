#include "memory_store.h"

#include <mutex>

bool MemoryStore::put(const std::string &key, const std::string &value)
{
    if (key.empty())
    {
        return false;
    }

    std::unique_lock lock(m_mutex);
    m_data[key] = value;
    return true;
}

std::optional<std::string> MemoryStore::get(const std::string &key) const
{
    std::shared_lock lock(m_mutex);
    auto it = m_data.find(key);
    if (it == m_data.end())
    {
        return std::nullopt;
    }
    return it->second;
}

bool MemoryStore::del(const std::string &key)
{
    std::unique_lock lock(m_mutex);
    return m_data.erase(key) > 0;
}

bool MemoryStore::exists(const std::string &key) const
{
    std::shared_lock lock(m_mutex);
    return m_data.find(key) != m_data.end();
}

std::unordered_map<std::string, std::string> MemoryStore::snapshot() const
{
    std::shared_lock lock(m_mutex);
    return m_data;
}

void MemoryStore::clear()
{
    std::unique_lock lock(m_mutex);
    m_data.clear();
}
