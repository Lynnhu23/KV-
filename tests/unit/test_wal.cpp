#include <gtest/gtest.h>

#include "storage/persistent_memory_store.h"

#include <filesystem>

namespace fs = std::filesystem;

TEST(WALTest, ReplaysPutAndDelete)
{
    fs::path wal_path = fs::temp_directory_path() / "tinykv_wal_test.log";
    fs::remove(wal_path);

    {
        PersistentMemoryStore store;
        ASSERT_TRUE(store.init(wal_path.string()));
        EXPECT_TRUE(store.put("keep", "value with spaces"));
        EXPECT_TRUE(store.put("gone", "value"));
        EXPECT_TRUE(store.del("gone"));
    }

    {
        PersistentMemoryStore store;
        ASSERT_TRUE(store.init(wal_path.string()));

        auto value = store.get("keep");
        ASSERT_TRUE(value.has_value());
        EXPECT_EQ(*value, "value with spaces");
        EXPECT_FALSE(store.get("gone").has_value());
    }

    fs::remove(wal_path);
}

TEST(WALTest, SnapshotCompactsWalAndRestoresState)
{
    fs::path dir = fs::temp_directory_path() / "tinykv_snapshot_test";
    fs::path wal_path = dir / "kv.wal";
    fs::path snapshot_path = dir / "kv.snapshot";
    fs::remove_all(dir);
    fs::create_directories(dir);

    {
        PersistentMemoryStore store;
        ASSERT_TRUE(store.init(wal_path.string(), snapshot_path.string(), 2));
        EXPECT_TRUE(store.put("keep", "first"));
        EXPECT_TRUE(store.put("keep", "second"));
        EXPECT_TRUE(fs::exists(snapshot_path));
        EXPECT_TRUE(fs::exists(wal_path));
        EXPECT_EQ(fs::file_size(wal_path), 0u);

        EXPECT_TRUE(store.put("after", "snapshot"));
        EXPECT_TRUE(store.del("missing"));
    }

    {
        PersistentMemoryStore store;
        ASSERT_TRUE(store.init(wal_path.string(), snapshot_path.string(), 2));

        auto keep = store.get("keep");
        ASSERT_TRUE(keep.has_value());
        EXPECT_EQ(*keep, "second");

        auto after = store.get("after");
        ASSERT_TRUE(after.has_value());
        EXPECT_EQ(*after, "snapshot");
        EXPECT_FALSE(store.get("missing").has_value());
    }

    fs::remove_all(dir);
}
