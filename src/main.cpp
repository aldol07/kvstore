// kvstore: a Redis-compatible in-memory key-value store.
//   kvstore [--port 6379] [--bind 0.0.0.0] [--aof FILE] [--appendfsync always|everysec|no]
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>

#include "aof.h"
#include "event_loop.h"
#include "store.h"

static int usage(const char* prog) {
    std::fprintf(stderr,
                 "usage: %s [--port N] [--bind ADDR] [--aof FILE] [--appendfsync always|everysec|no]\n",
                 prog);
    return 2;
}

int main(int argc, char** argv) {
    int port = 6379;
    std::string bind = "0.0.0.0";
    std::string aof_path;  // empty = persistence off
    kv::FsyncPolicy policy = kv::FsyncPolicy::EverySec;
    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--port") && i + 1 < argc) port = std::atoi(argv[++i]);
        else if (!strcmp(argv[i], "--bind") && i + 1 < argc) bind = argv[++i];
        else if (!strcmp(argv[i], "--aof") && i + 1 < argc) aof_path = argv[++i];
        else if (!strcmp(argv[i], "--appendfsync") && i + 1 < argc) {
            if (!kv::parse_fsync_policy(argv[++i], policy)) return usage(argv[0]);
        } else return usage(argv[0]);
    }
    std::signal(SIGPIPE, SIG_IGN);  // a client vanishing mid-write must not kill the server
    try {
        kv::Store store;
        std::unique_ptr<kv::Aof> aof;
        if (!aof_path.empty()) {
            aof = std::make_unique<kv::Aof>(aof_path, policy);
            auto t0 = std::chrono::steady_clock::now();
            size_t n = aof->load(store);
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - t0).count();
            std::printf("aof: replayed %zu commands from %s in %lld ms, %zu keys\n", n,
                        aof_path.c_str(), (long long)ms, store.size());
        }
        kv::EventLoop loop(store, aof.get(), port, bind);  // also takes over SIGINT/SIGTERM via signalfd
        std::printf("kvstore ready on %s:%d (epoll, single-threaded)\n", bind.c_str(), port);
        std::fflush(stdout);
        loop.run();
        std::printf("bye: %llu connections, %llu commands\n",
                    (unsigned long long)loop.stats().connections_total,
                    (unsigned long long)loop.stats().commands_total);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "fatal: %s\n", e.what());
        return 1;
    }
    return 0;
}
