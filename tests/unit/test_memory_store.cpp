#include <gtest/gtest.h>

#include "storage/memory_store.h"

#include <chrono>
#include <thread>

TEST(MemoryStoreTest, PutGetExistsAndDelete)
{
    MemoryStore store;

    EXPECT_TRUE(store.put("name", "alice"));
    EXPECT_TRUE(store.exists("name"));

    auto value = store.get("name");
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(*value, "alice");

    EXPECT_TRUE(store.del("name"));
    EXPECT_FALSE(store.exists("name"));
    EXPECT_FALSE(store.get("name").has_value());
}

TEST(MemoryStoreTest, RejectsEmptyKey)
{
    MemoryStore store;

    EXPECT_FALSE(store.put("", "value"));
    EXPECT_FALSE(store.exists(""));
}

TEST(MemoryStoreTest, ExpiresKeysWithTtl)
{
    MemoryStore store;

    EXPECT_TRUE(store.put_ttl("session", "alice", 1));
    EXPECT_TRUE(store.exists("session"));
    EXPECT_GE(store.ttl("session"), 0);

    for (int i = 0; i < 30 && store.exists("session"); ++i)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_FALSE(store.exists("session"));
    EXPECT_EQ(store.ttl("session"), -2);
}

TEST(MemoryStoreTest, EvictsLeastRecentlyUsedKey)
{
    MemoryStore store;
    store.set_max_keys(2);

    EXPECT_TRUE(store.put("a", "1"));
    EXPECT_TRUE(store.put("b", "2"));
    EXPECT_EQ(store.get("a"), "1");
    EXPECT_TRUE(store.put("c", "3"));

    EXPECT_TRUE(store.exists("a"));
    EXPECT_FALSE(store.exists("b"));
    EXPECT_TRUE(store.exists("c"));
    EXPECT_EQ(store.size(), 2u);
}
