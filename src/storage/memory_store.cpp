#include "memory_store.h"

#include <chrono>
#include <mutex>

bool MemoryStore::put(const std::string &key, const std::string &value)
{
    return put_with_expire_at(key, value, 0);
}

bool MemoryStore::put_with_expire_at(const std::string &key,
                                     const std::string &value,
                                     long long expire_at_ms)
{
    if (key.empty())
    {
        return false;
    }

    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_data.find(key);
    if (it != m_data.end())
    {
        it->second.value = value;
        it->second.expire_at_ms = expire_at_ms;
        touch_locked(key);
    }
    else
    {
        m_lru.push_front(key);
        m_data.emplace(key, Entry{value, expire_at_ms, m_lru.begin()});
    }
    enforce_capacity_locked();
    return true;
}

bool MemoryStore::put_ttl(const std::string &key, const std::string &value, int ttl_seconds)
{
    if (ttl_seconds <= 0)
    {
        return false;
    }
    return put_with_expire_at(key, value, ttl_to_expire_at_ms(ttl_seconds));
}

std::optional<std::string> MemoryStore::get(const std::string &key) const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_data.find(key);
    if (it == m_data.end())
    {
        return std::nullopt;
    }
    if (expired(it->second, now_ms()))
    {
        m_lru.erase(it->second.lru_it);
        m_data.erase(it);
        return std::nullopt;
    }
    touch_locked(key);
    return it->second.value;
}

bool MemoryStore::del(const std::string &key)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_data.find(key);
    if (it == m_data.end())
    {
        return false;
    }
    m_lru.erase(it->second.lru_it);
    m_data.erase(it);
    return true;
}

bool MemoryStore::exists(const std::string &key) const
{
    return get(key).has_value();
}

bool MemoryStore::expire(const std::string &key, int ttl_seconds)
{
    if (ttl_seconds <= 0)
    {
        return false;
    }
    return expire_at(key, ttl_to_expire_at_ms(ttl_seconds));
}

bool MemoryStore::expire_at(const std::string &key, long long expire_at_ms)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_data.find(key);
    if (it == m_data.end() || expired(it->second, now_ms()))
    {
        if (it != m_data.end())
        {
            m_lru.erase(it->second.lru_it);
            m_data.erase(it);
        }
        return false;
    }
    it->second.expire_at_ms = expire_at_ms;
    touch_locked(key);
    return true;
}

long long MemoryStore::ttl(const std::string &key) const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_data.find(key);
    if (it == m_data.end())
    {
        return -2;
    }

    long long now = now_ms();
    if (expired(it->second, now))
    {
        m_lru.erase(it->second.lru_it);
        m_data.erase(it);
        return -2;
    }
    if (it->second.expire_at_ms == 0)
    {
        return -1;
    }

    long long remaining_ms = it->second.expire_at_ms - now;
    return remaining_ms <= 0 ? -2 : (remaining_ms + 999) / 1000;
}

void MemoryStore::set_max_keys(size_t max_keys)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_max_keys = max_keys;
    enforce_capacity_locked();
}

size_t MemoryStore::size() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    evict_expired_locked();
    return m_data.size();
}

std::vector<StoreEntry> MemoryStore::snapshot_entries() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    evict_expired_locked();

    std::vector<StoreEntry> entries;
    entries.reserve(m_data.size());
    for (const auto &[key, entry] : m_data)
    {
        entries.push_back({key, entry.value, entry.expire_at_ms});
    }
    return entries;
}

std::unordered_map<std::string, std::string> MemoryStore::snapshot() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    evict_expired_locked();

    std::unordered_map<std::string, std::string> out;
    out.reserve(m_data.size());
    for (const auto &[key, entry] : m_data)
    {
        out.emplace(key, entry.value);
    }
    return out;
}

void MemoryStore::clear()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_data.clear();
    m_lru.clear();
}

long long MemoryStore::now_ms()
{
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

long long MemoryStore::ttl_to_expire_at_ms(int ttl_seconds)
{
    return now_ms() + static_cast<long long>(ttl_seconds) * 1000;
}

bool MemoryStore::expired(const Entry &entry, long long now) const
{
    return entry.expire_at_ms > 0 && entry.expire_at_ms <= now;
}

void MemoryStore::touch_locked(const std::string &key) const
{
    auto it = m_data.find(key);
    if (it == m_data.end())
    {
        return;
    }
    m_lru.erase(it->second.lru_it);
    m_lru.push_front(key);
    it->second.lru_it = m_lru.begin();
}

void MemoryStore::evict_expired_locked() const
{
    long long now = now_ms();
    for (auto it = m_data.begin(); it != m_data.end();)
    {
        if (expired(it->second, now))
        {
            m_lru.erase(it->second.lru_it);
            it = m_data.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void MemoryStore::enforce_capacity_locked() const
{
    evict_expired_locked();
    while (m_max_keys > 0 && m_data.size() > m_max_keys && !m_lru.empty())
    {
        const std::string key = m_lru.back();
        m_lru.pop_back();
        m_data.erase(key);
    }
}
