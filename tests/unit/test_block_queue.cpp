#include <gtest/gtest.h>
#include <thread>
#include <future>
#include <chrono>
#include "log/block_queue.h"

TEST(BlockQueueTest, PushPop)
{
    block_queue<int> q(10);
    EXPECT_TRUE(q.empty());
    EXPECT_TRUE(q.push(42));
    EXPECT_FALSE(q.empty());
    EXPECT_EQ(q.size(), 1);
    int val;
    EXPECT_TRUE(q.pop(val));
    EXPECT_EQ(val, 42);
    EXPECT_TRUE(q.empty());
}

TEST(BlockQueueTest, FullQueue)
{
    block_queue<int> q(3);
    EXPECT_TRUE(q.push(1));
    EXPECT_TRUE(q.push(2));
    EXPECT_TRUE(q.push(3));
    EXPECT_FALSE(q.push(4));
    EXPECT_EQ(q.size(), 3);
}

TEST(BlockQueueTest, FrontBack)
{
    block_queue<int> q(5);
    q.push(10);
    q.push(20);
    int val;
    EXPECT_TRUE(q.front(val));
    EXPECT_EQ(val, 10);
    EXPECT_TRUE(q.back(val));
    EXPECT_EQ(val, 20);
}

TEST(BlockQueueTest, Clear)
{
    block_queue<int> q(10);
    q.push(1);
    q.push(2);
    q.clear();
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0);
}

TEST(BlockQueueTest, ThreadSafety)
{
    block_queue<int> q(100);
    std::atomic<int> sum{0};
    std::atomic<bool> done{false};

    std::jthread producer([&q, &done] {
        for (int i = 0; i < 1000; ++i)
        {
            while (!q.push(i)) { std::this_thread::yield(); }
        }
        done = true;
    });

    std::jthread consumer([&q, &sum, &done] {
        int consumed = 0;
        while (consumed < 1000)
        {
            int val;
            if (q.pop(val, 100))
            {
                sum += val;
                ++consumed;
            }
            else if (done && q.empty())
            {
                break;
            }
        }
    });

    producer.join();
    consumer.join();
    EXPECT_EQ(sum.load(), 499500);  // sum of 0..999
}

TEST(BlockQueueTest, TimeoutPop)
{
    block_queue<int> q(10);
    int val;
    auto start = std::chrono::steady_clock::now();
    EXPECT_FALSE(q.pop(val, 100));
    auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 90);
    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 200);
}
