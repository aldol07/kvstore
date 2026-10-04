#include "aof.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <stdexcept>

#include "commands.h"

namespace kv {
namespace {

[[noreturn]] void fail(const std::string& what) {
    throw std::runtime_error("aof " + what + ": " + strerror(errno));
}

// write() may write less than asked; loop until everything is out.
bool write_all(int fd, const char* p, size_t n) {
    while (n > 0) {
        ssize_t w = ::write(fd, p, n);
        if (w > 0) { p += w; n -= static_cast<size_t>(w); }
        else if (w < 0 && errno == EINTR) continue;
        else return false;
    }
    return true;
}

std::string read_file(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) fail("open " + path);
    std::string data;
    char chunk[64 * 1024];
    for (;;) {
        ssize_t n = ::read(fd, chunk, sizeof chunk);
        if (n > 0) data.append(chunk, static_cast<size_t>(n));
        else if (n == 0) break;
        else if (errno != EINTR) { ::close(fd); fail("read"); }
    }
    ::close(fd);
    return data;
}

off_t file_size(const std::string& path) {
    struct stat st{};
    return ::stat(path.c_str(), &st) == 0 ? st.st_size : 0;
}

int64_t mono_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

// Runs in the child process. No exceptions escape, and we leave with _exit() so the
// parent's stdio buffers and destructors (sockets, AOF fd) are not run a second time.
[[noreturn]] void child_write_snapshot(const Store& store, const std::string& tmp_path) {
    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) _exit(1);
    std::string out;
    bool ok = true;
    store.for_each([&](const std::string& k, const std::string& v, int64_t expire_at) {
        resp::array_header(out, expire_at ? 5 : 3);
        resp::bulk(out, "SET");
        resp::bulk(out, k);
        resp::bulk(out, v);
        if (expire_at) { resp::bulk(out, "PXAT"); resp::bulk(out, std::to_string(expire_at)); }
        if (out.size() >= 1 << 20) { ok = ok && write_all(fd, out.data(), out.size()); out.clear(); }
    });
    ok = ok && write_all(fd, out.data(), out.size()) && ::fsync(fd) == 0;
    ::close(fd);
    _exit(ok ? 0 : 1);
}

}  // namespace

bool parse_fsync_policy(const std::string& s, FsyncPolicy& out) {
    if (s == "always") out = FsyncPolicy::Always;
    else if (s == "everysec") out = FsyncPolicy::EverySec;
    else if (s == "no") out = FsyncPolicy::No;
    else return false;
    return true;
}

Aof::Aof(std::string path, FsyncPolicy policy)
    : path_(std::move(path)), tmp_path_(path_ + ".rewrite.tmp"), policy_(policy) {
    // O_APPEND: the kernel moves the offset to end-of-file before every write().
    fd_ = ::open(path_.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
    if (fd_ < 0) fail("open " + path_);
}

Aof::~Aof() {
    if (child_pid_ > 0) {  // shutting down mid-rewrite: abandon it, the old AOF is still complete
        ::kill(child_pid_, SIGKILL);
        ::waitpid(child_pid_, nullptr, 0);
        ::unlink(tmp_path_.c_str());
    }
    try {
        write_pending();
        if (unsynced_) sync();  // clean shutdown: nothing is lost whatever the policy
    } catch (const std::exception& e) {
        std::fprintf(stderr, "%s\n", e.what());
    }
    ::close(fd_);
}

size_t Aof::load(Store& store) {
    std::string data = read_file(path_);
    std::string_view view(data);
    Context ctx{store};
    std::string reply;  // replies are discarded
    size_t off = 0, applied = 0;
    while (off < view.size()) {
        ParseResult r = parse_command(view.substr(off));
        if (r.status == ParseStatus::Error)
            throw std::runtime_error("aof is corrupt at byte " + std::to_string(off) + ": " + r.error);
        if (r.status == ParseStatus::NeedMore) {
            // The server died in the middle of a write(). Everything before `off` is whole
            // commands, so drop the partial tail; the client never got an OK for it anyway.
            std::fprintf(stderr, "aof: dropping %zu bytes of incomplete command at the end\n",
                         view.size() - off);
            if (::ftruncate(fd_, static_cast<off_t>(off)) < 0) fail("ftruncate");
            break;
        }
        off += r.consumed;
        if (r.args.empty()) continue;
        dispatch(ctx, r.args, reply);
        reply.clear();
        ++applied;
    }
    return applied;
}

void Aof::append(const Args& args) {
    size_t start = buf_.size();
    resp::array_header(buf_, args.size());
    for (const auto& a : args) resp::bulk(buf_, a);
    // The child's snapshot is frozen at fork() time, so remember newer writes for it.
    if (child_pid_ > 0) rewrite_buf_.append(buf_, start, std::string::npos);
}

void Aof::write_pending() {
    if (!write_all(fd_, buf_.data(), buf_.size()))
        fail("write");  // disk full etc.: stop rather than silently lose writes
    if (!buf_.empty()) unsynced_ = true;
    buf_.clear();
    if (policy_ == FsyncPolicy::Always && unsynced_) sync();
}

void Aof::tick(int64_t now_ms) {
    if (policy_ != FsyncPolicy::EverySec || !unsynced_ || now_ms - last_sync_ms_ < 1000) return;
    sync();
    last_sync_ms_ = now_ms;
}

void Aof::sync() {
    // fdatasync flushes the file's data (and size) to disk, skipping metadata like mtime.
    if (::fdatasync(fd_) < 0) fail("fdatasync");
    unsynced_ = false;
}

std::string Aof::start_rewrite(const Store& store) {
    if (child_pid_ > 0) return "ERR Background append only file rewriting already in progress";
    write_pending();
    rewrite_buf_.clear();
    rewrite_start_ms_ = mono_ms();

    // fork() duplicates this process. The child gets the same memory *contents*, but the
    // kernel doesn't copy any pages yet: both processes share them read-only, and a page is
    // copied only when one side writes to it (copy-on-write). So fork is fast, and the
    // child sees the keyspace exactly as it was at this instant.
    auto t0 = std::chrono::steady_clock::now();
    pid_t pid = ::fork();
    if (pid < 0) return std::string("ERR fork failed: ") + strerror(errno);
    if (pid == 0) child_write_snapshot(store, tmp_path_);  // never returns

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0).count();
    std::printf("aof rewrite: child pid %d started (fork took %lld us)\n", pid, (long long)us);
    std::fflush(stdout);
    child_pid_ = pid;
    return "";
}

void Aof::reap_child(bool block) {
    if (child_pid_ <= 0) return;
    int status = 0;
    // waitpid collects the child's exit status; until we do, it stays a "zombie" process.
    pid_t r = ::waitpid(child_pid_, &status, block ? 0 : WNOHANG);
    if (r == 0) return;  // still running
    child_pid_ = -1;
    finish_rewrite(r > 0 && WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

void Aof::finish_rewrite(bool child_ok) {
    if (!child_ok) {
        std::fprintf(stderr, "aof rewrite: child failed, keeping the old file\n");
        ::unlink(tmp_path_.c_str());
        rewrite_buf_.clear();
        return;
    }
    write_pending();  // old AOF is complete up to now, in case anything below fails
    off_t old_size = file_size(path_);

    int fd = ::open(tmp_path_.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC);
    bool ok = fd >= 0 && write_all(fd, rewrite_buf_.data(), rewrite_buf_.size()) &&
              ::fsync(fd) == 0;
    if (fd >= 0) ::close(fd);
    // rename() replaces the file atomically: a crash leaves either the old AOF or the new
    // one, never a mix.
    if (!ok || ::rename(tmp_path_.c_str(), path_.c_str()) < 0) {
        std::fprintf(stderr, "aof rewrite: finishing failed (%s), keeping the old file\n",
                     strerror(errno));
        ::unlink(tmp_path_.c_str());
        rewrite_buf_.clear();
        return;
    }
    // Our fd_ still points at the old, now-unlinked file; switch to the new one.
    ::close(fd_);
    fd_ = ::open(path_.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC);
    if (fd_ < 0) fail("reopen " + path_);
    unsynced_ = false;
    std::printf("aof rewrite: done in %lld ms, %lld -> %lld bytes (+%zu bytes written meanwhile)\n",
                (long long)(mono_ms() - rewrite_start_ms_), (long long)old_size,
                (long long)file_size(path_), rewrite_buf_.size());
    std::fflush(stdout);
    rewrite_buf_.clear();
}

}  // namespace kv
