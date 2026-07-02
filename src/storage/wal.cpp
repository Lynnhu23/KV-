#include "wal.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <vector>

namespace fs = std::filesystem;

namespace
{
std::vector<std::string> split_tab(const std::string &line)
{
    std::vector<std::string> parts;
    std::string current;
    for (char ch : line)
    {
        if (ch == '\t')
        {
            parts.push_back(current);
            current.clear();
        }
        else
        {
            current.push_back(ch);
        }
    }
    parts.push_back(current);
    return parts;
}
}

bool WriteAheadLog::open(const std::string &path)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_path = path;

    fs::path wal_path(path);
    if (wal_path.has_parent_path())
    {
        std::error_code ec;
        fs::create_directories(wal_path.parent_path(), ec);
        if (ec)
        {
            return false;
        }
    }

    m_out.open(m_path, std::ios::app);
    return m_out.is_open();
}

bool WriteAheadLog::append_put(const std::string &key, const std::string &value)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_out.is_open())
    {
        return false;
    }

    m_out << "PUT\t" << escape(key) << '\t' << escape(value) << '\n';
    m_out.flush();
    return static_cast<bool>(m_out);
}

bool WriteAheadLog::append_del(const std::string &key)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_out.is_open())
    {
        return false;
    }

    m_out << "DEL\t" << escape(key) << '\n';
    m_out.flush();
    return static_cast<bool>(m_out);
}

bool WriteAheadLog::replay(KVStore &store) const
{
    std::ifstream in(m_path);
    if (!in.is_open())
    {
        return true;
    }

    std::string line;
    while (std::getline(in, line))
    {
        auto parts = split_tab(line);
        if (parts.empty())
        {
            continue;
        }

        if (parts[0] == "PUT" && parts.size() == 3)
        {
            std::string key;
            std::string value;
            if (!unescape(parts[1], key) || !unescape(parts[2], value))
            {
                return false;
            }
            if (!store.put(key, value))
            {
                return false;
            }
        }
        else if (parts[0] == "DEL" && parts.size() == 2)
        {
            std::string key;
            if (!unescape(parts[1], key))
            {
                return false;
            }
            store.del(key);
        }
        else
        {
            return false;
        }
    }

    return true;
}

bool WriteAheadLog::reset()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_out.is_open())
    {
        m_out.close();
    }

    m_out.open(m_path, std::ios::trunc);
    if (!m_out.is_open())
    {
        return false;
    }
    m_out.close();

    m_out.open(m_path, std::ios::app);
    return m_out.is_open();
}

std::string WriteAheadLog::escape(const std::string &value)
{
    std::string out;
    out.reserve(value.size());
    for (char ch : value)
    {
        switch (ch)
        {
        case '\\': out += "\\\\"; break;
        case '\t': out += "\\t"; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        default: out.push_back(ch); break;
        }
    }
    return out;
}

bool WriteAheadLog::unescape(const std::string &value, std::string &out)
{
    out.clear();
    out.reserve(value.size());

    for (size_t i = 0; i < value.size(); ++i)
    {
        char ch = value[i];
        if (ch != '\\')
        {
            out.push_back(ch);
            continue;
        }

        if (++i >= value.size())
        {
            return false;
        }

        switch (value[i])
        {
        case '\\': out.push_back('\\'); break;
        case 't': out.push_back('\t'); break;
        case 'n': out.push_back('\n'); break;
        case 'r': out.push_back('\r'); break;
        default: return false;
        }
    }

    return true;
}
