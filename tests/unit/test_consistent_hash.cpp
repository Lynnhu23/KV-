#include <gtest/gtest.h>

#include "cluster/consistent_hash.h"

TEST(ConsistentHashTest, ReturnsStableOwnerForKey)
{
    ConsistentHash hash;
    hash.build({
        {"node-1", "127.0.0.1", 19021},
        {"node-2", "127.0.0.1", 19022},
    });

    auto first = hash.owner_for("user:1");
    auto second = hash.owner_for("user:1");

    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(first->id, second->id);
}

TEST(ConsistentHashTest, EmptyRingHasNoOwner)
{
    ConsistentHash hash;

    EXPECT_FALSE(hash.owner_for("key").has_value());
}
