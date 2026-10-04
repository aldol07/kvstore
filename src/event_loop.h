// Single-threaded epoll event loop (edge-triggered, non-blocking sockets).
#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "aof.h"
#include "store.h"

namespace kv {

struct Connection {
    explicit Connection(int f) : fd(f) {}
    int fd;
    std::string rbuf;       // bytes received but not yet parsed
    std::string wbuf;       // replies not yet written
    size_t wpos = 0;        // how much of wbuf has been written
    bool want_write = false;  // EPOLLOUT currently registered
    bool closing = false;     // close once wbuf is flushed (QUIT / protocol error)
};

struct Stats {
    uint64_t connections_total = 0;
    uint64_t commands_total = 0;
};

class EventLoop {
public:
    // `aof` may be null (persistence off).
    EventLoop(Store& store, Aof* aof, int port, const std::string& bind = "0.0.0.0");
    ~EventLoop();
    void run();    // blocks until SIGINT/SIGTERM arrives (or stop() is called)
    void stop() { running_ = false; }
    const Stats& stats() const { return stats_; }

private:
    void accept_all();
    void on_signal();
    void on_readable(Connection& c);
    void on_writable(Connection& c);
    void process(Connection& c);
    void flush(Connection& c);
    void update_interest(Connection& c);
    void close_conn(int fd);

    Store& store_;
    Aof* aof_;
    int listen_fd_ = -1;
    int epfd_ = -1;
    int sigfd_ = -1;  // signalfd: SIGINT/SIGTERM delivered as readable bytes, not async handlers
    bool running_ = true;
    std::unordered_map<int, std::unique_ptr<Connection>> conns_;
    Stats stats_;
};

}  // namespace kv
