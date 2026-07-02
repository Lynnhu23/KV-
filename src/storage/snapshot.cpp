#include "snapshot.h"

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

SnapshotFile::SnapshotFile(std::string path)
    : m_path(std::move(path))
{
}

void SnapshotFile::set_path(std::string path)
{
    m_path = std::move(path);
}

const std::string &SnapshotFile::path() const
{
    return m_path;
}

bool SnapshotFile::save(const std::vector<StoreEntry> &entries) const
{
    if (m_path.empty())
    {
        return false;
    }

    fs::path snapshot_path(m_path);
    if (snapshot_path.has_parent_path())
    {
        std::error_code ec;
        fs::create_directories(snapshot_path.parent_path(), ec);
        if (ec)
        {
            return false;
        }
    }

    fs::path tmp_path = snapshot_path;
    tmp_path += ".tmp";

    {
        std::ofstream out(tmp_path, std::ios::trunc);
        if (!out.is_open())
        {
            return false;
        }

        out << "TINYKV_SNAPSHOT_V2\n";
        for (const auto &entry : entries)
        {
            out << escape(entry.key) << '\t'
                << escape(entry.value) << '\t'
                << entry.expire_at_ms << '\n';
        }
        out.flush();
        if (!out)
        {
            return false;
        }
    }

    std::error_code ec;
    fs::rename(tmp_path, snapshot_path, ec);
    if (ec)
    {
        fs::remove(snapshot_path, ec);
        ec.clear();
        fs::rename(tmp_path, snapshot_path, ec);
    }
    return !ec;
}

bool SnapshotFile::load(KVStore &store) const
{
    if (m_path.empty())
    {
        return true;
    }

    std::ifstream in(m_path);
    if (!in.is_open())
    {
        return true;
    }

    std::string line;
    if (!std::getline(in, line))
    {
        return false;
    }
    bool v1 = line == "TINYKV_SNAPSHOT_V1";
    bool v2 = line == "TINYKV_SNAPSHOT_V2";
    if (!v1 && !v2)
    {
        return false;
    }

    while (std::getline(in, line))
    {
        auto parts = split_tab(line);
        if ((v1 && parts.size() != 2) || (v2 && parts.size() != 3))
        {
            return false;
        }

        std::string key;
        std::string value;
        if (!unescape(parts[0], key) || !unescape(parts[1], value))
        {
            return false;
        }
        long long expire_at_ms = v2 ? std::stoll(parts[2]) : 0;
        if (!store.put_with_expire_at(key, value, expire_at_ms))
        {
            return false;
        }
    }

    return true;
}

std::string SnapshotFile::escape(const std::string &value)
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

bool SnapshotFile::unescape(const std::string &value, std::string &out)
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
