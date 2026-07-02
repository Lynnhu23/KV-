#include <gtest/gtest.h>

#include "storage/memory_store.h"

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
