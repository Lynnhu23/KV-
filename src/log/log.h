#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <iostream>
#include <string>
#include <string_view>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include "block_queue.h"

using namespace std;

class Log
{
public:
    // C++11以后,使用局部变量懒汉不用加锁
    static Log *get_instance()
    {
        static Log instance;
        return &instance;
    }

    // 可选择的参数有日志文件、日志缓冲区大小、最大行数以及最长日志条队列
    bool init(const char *file_name, int close_log, int log_buf_size = 8192,
              int split_lines = 5000000, int max_queue_size = 0);

    void write_log(int level, const char *format, ...);

    void flush(void);

private:
    Log();
    virtual ~Log();

    void async_write_log()
    {
        string single_log;
        // 从阻塞队列中取出一个日志string，写入文件
        while (m_log_queue->pop(single_log))
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_fp != NULL)
                fputs(single_log.c_str(), m_fp);
        }
    }

private:
    string dir_name;     // 路径名
    string log_name;     // log文件名
    int m_split_lines;   // 日志最大行数
    int m_log_buf_size;  // 日志缓冲区大小
    size_t m_count;      // 日志行数记录
    int m_today;         // 因为按天分类,记录当前时间是那一天
    FILE *m_fp;          // 打开log的文件指针
    bool m_initialized;  // 日志文件是否已成功初始化
    vector<char> m_buf;  // 日志缓冲区 (RAII)
    unique_ptr<block_queue<string>> m_log_queue; // 阻塞队列 (RAII)
    bool m_is_async;     // 是否同步标志位
    std::mutex m_mutex;
    std::jthread m_async_thread; // 异步写日志线程 (C++20)
    int m_close_log;     // 关闭日志
};

// 简化宏: write_log 内部检查 m_close_log
#define LOG_DEBUG(format, ...) Log::get_instance()->write_log(0, format, ##__VA_ARGS__)
#define LOG_INFO(format, ...)  Log::get_instance()->write_log(1, format, ##__VA_ARGS__)
#define LOG_WARN(format, ...)  Log::get_instance()->write_log(2, format, ##__VA_ARGS__)
#define LOG_ERROR(format, ...) Log::get_instance()->write_log(3, format, ##__VA_ARGS__)

#endif
