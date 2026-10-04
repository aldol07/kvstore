// Append-only file: every write command is appended in RESP form; on startup the file is
// replayed to rebuild the keyspace. BGREWRITEAOF compacts it using fork().
#pragma once
#include <sys/types.h>

#include <cstdint>
#include <string>

#include "resp.h"
#include "store.h"

namespace kv {

// When to call fdatasync(). write() only copies bytes into the kernel's page cache;
// they reach the disk later unless we force it.
enum class FsyncPolicy {
    Always,    // sync before replying: an OK is on disk. Slowest, loses nothing.
    EverySec,  // sync at most once a second: can lose ~1s of writes on power loss.
    No,        // never sync; the kernel flushes when it likes (typically ~30s).
};

bool parse_fsync_policy(const std::string& s, FsyncPolicy& out);

class Aof {
public:
    Aof(std::string path, FsyncPolicy policy);
    ~Aof();
    Aof(const Aof&) = delete;
    Aof& operator=(const Aof&) = delete;

    // Replays the file into `store`. Returns the number of commands applied.
    // A half-written last command (crash mid-write) is cut off with ftruncate().
    size_t load(Store& store);

    void append(const Args& args);  // buffer in memory (cheap)
    void write_pending();           // write() the buffer; fdatasync() too if policy is Always
    void tick(int64_t now_ms);      // called every loop iteration; EverySec syncs here

    // Background rewrite (compaction). fork() a child that writes one SET per live key to a
    // temp file from its copy-on-write view of memory, while this process keeps serving.
    // Returns "" on success or a Redis-style error message.
    std::string start_rewrite(const Store& store);
    // Call on SIGCHLD. If the child has exited: append the writes that happened meanwhile,
    // then rename() the temp file over the AOF. `block` waits for the child (tests).
    void reap_child(bool block = false);
    bool rewriting() const { return child_pid_ > 0; }

private:
    void sync();
    void finish_rewrite(bool child_ok);

    std::string path_;
    std::string tmp_path_;      // where the child writes the compacted file
    FsyncPolicy policy_;
    int fd_ = -1;
    std::string buf_;           // appended but not yet written
    bool unsynced_ = false;     // written since the last fdatasync
    int64_t last_sync_ms_ = 0;

    pid_t child_pid_ = -1;
    std::string rewrite_buf_;   // writes made while the child runs; it can't see them
    int64_t rewrite_start_ms_ = 0;
};

}  // namespace kv
