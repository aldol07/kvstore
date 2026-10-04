#include "event_loop.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "commands.h"
#include "resp.h"

namespace kv {
namespace {

constexpr size_t kReadChunk = 16 * 1024;
constexpr size_t kMaxQueryBuf = kMaxBulkLen + 1024 * 1024;  // per-client input cap
constexpr int kMaxEvents = 256;

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        throw std::runtime_error(std::string("fcntl: ") + strerror(errno));
}

}  // namespace

EventLoop::EventLoop(Store& store, Aof* aof, int port, const std::string& bind_addr)
    : store_(store), aof_(aof) {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) throw std::runtime_error(std::string("socket: ") + strerror(errno));
    int yes = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, bind_addr.c_str(), &addr.sin_addr) != 1)
        throw std::runtime_error("invalid bind address: " + bind_addr);
    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0)
        throw std::runtime_error(std::string("bind: ") + strerror(errno));
    if (listen(listen_fd_, SOMAXCONN) < 0) throw std::runtime_error(std::string("listen: ") + strerror(errno));
    set_nonblocking(listen_fd_);

    epfd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epfd_ < 0) throw std::runtime_error(std::string("epoll_create1: ") + strerror(errno));
    epoll_event ev{};
    ev.events = EPOLLIN;  // level-triggered for the listener; we accept in a loop anyway
    ev.data.fd = listen_fd_;
    epoll_ctl(epfd_, EPOLL_CTL_ADD, listen_fd_, &ev);

    // Signals normally interrupt the program at any instruction and run an async handler,
    // where almost nothing is safe to call. Instead: block SIGINT/SIGTERM so the kernel keeps
    // them pending, and ask for them as a file descriptor. epoll then reports "a signal
    // arrived" like any other event, and we handle it in normal code on the loop thread.
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    sigaddset(&mask, SIGCHLD);  // a BGREWRITEAOF child process exited
    if (sigprocmask(SIG_BLOCK, &mask, nullptr) < 0)
        throw std::runtime_error(std::string("sigprocmask: ") + strerror(errno));
    sigfd_ = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (sigfd_ < 0) throw std::runtime_error(std::string("signalfd: ") + strerror(errno));
    ev.events = EPOLLIN;
    ev.data.fd = sigfd_;
    epoll_ctl(epfd_, EPOLL_CTL_ADD, sigfd_, &ev);
}

EventLoop::~EventLoop() {
    for (auto& [fd, _] : conns_) ::close(fd);
    if (sigfd_ >= 0) ::close(sigfd_);
    if (epfd_ >= 0) ::close(epfd_);
    if (listen_fd_ >= 0) ::close(listen_fd_);
}

void EventLoop::run() {
    std::vector<epoll_event> events(kMaxEvents);
    while (running_) {
        int n = epoll_wait(epfd_, events.data(), kMaxEvents, 100 /* ms; phase 2: expiry timer */);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(std::string("epoll_wait: ") + strerror(errno));
        }
        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            uint32_t e = events[i].events;
            if (fd == listen_fd_) { accept_all(); continue; }
            if (fd == sigfd_) { on_signal(); continue; }
            auto it = conns_.find(fd);
            if (it == conns_.end()) continue;
            Connection& c = *it->second;
            if (e & (EPOLLERR | EPOLLHUP)) { close_conn(fd); continue; }
            if (e & EPOLLIN) on_readable(c);
            if (conns_.count(fd) && (e & EPOLLOUT)) on_writable(c);
        }
        if (aof_) aof_->tick(store_.now());  // epoll_wait wakes at least every 100ms
    }
}

void EventLoop::accept_all() {
    for (;;) {
        int fd = accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            if (errno == EINTR || errno == ECONNABORTED) continue;
            perror("accept4");
            return;
        }
        int yes = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof yes);
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
        ev.data.fd = fd;
        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0) { ::close(fd); continue; }
        conns_[fd] = std::make_unique<Connection>(fd);
        ++stats_.connections_total;
    }
}

void EventLoop::on_signal() {
    signalfd_siginfo info;
    while (::read(sigfd_, &info, sizeof info) == static_cast<ssize_t>(sizeof info)) {
        int sig = static_cast<int>(info.ssi_signo);
        if (sig == SIGCHLD) {  // a forked child exited; collect it and finish its work
            if (aof_) aof_->reap_child();
            continue;
        }
        std::printf("received %s, shutting down\n", strsignal(sig));
        running_ = false;  // finish this batch of events, then run() returns
    }
}

void EventLoop::on_readable(Connection& c) {
    // Edge-triggered: we must drain the socket until EAGAIN or we'll never be woken again.
    char buf[kReadChunk];
    bool eof = false;
    for (;;) {
        ssize_t n = ::read(c.fd, buf, sizeof buf);
        if (n > 0) {
            c.rbuf.append(buf, static_cast<size_t>(n));
            if (c.rbuf.size() > kMaxQueryBuf) { close_conn(c.fd); return; }
        } else if (n == 0) { eof = true; break; }
        else if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        else if (errno == EINTR) continue;
        else { close_conn(c.fd); return; }
    }
    int fd = c.fd;
    process(c);
    if (!conns_.count(fd)) return;
    if (eof) { close_conn(fd); return; }
}

void EventLoop::process(Connection& c) {
    size_t off = 0;
    Context ctx{store_, aof_};
    while (off < c.rbuf.size() && !c.closing) {
        ParseResult r = parse_command(std::string_view(c.rbuf).substr(off));
        if (r.status == ParseStatus::NeedMore) break;
        if (r.status == ParseStatus::Error) {
            resp::error(c.wbuf, "ERR " + r.error);
            c.closing = true;  // like Redis: reply, then drop the connection
            break;
        }
        off += r.consumed;
        if (r.args.empty()) continue;
        dispatch(ctx, r.args, c.wbuf);
        ++stats_.commands_total;
        if (ctx.close_after_reply) c.closing = true;
    }
    c.rbuf.erase(0, off);
    if (aof_ && !ctx.to_log.empty()) {
        for (const auto& entry : ctx.to_log) aof_->append(entry);
        aof_->write_pending();  // before replying: with fsync=always, OK means on disk
    }
    flush(c);
}

void EventLoop::on_writable(Connection& c) { flush(c); }

void EventLoop::flush(Connection& c) {
    while (c.wpos < c.wbuf.size()) {
        ssize_t n = ::write(c.fd, c.wbuf.data() + c.wpos, c.wbuf.size() - c.wpos);
        if (n > 0) { c.wpos += static_cast<size_t>(n); continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;  // kernel buffer full
        close_conn(c.fd);
        return;
    }
    if (c.wpos == c.wbuf.size()) {
        c.wbuf.clear();
        c.wpos = 0;
        if (c.closing) { close_conn(c.fd); return; }
    }
    update_interest(c);
}

void EventLoop::update_interest(Connection& c) {
    bool need = !c.wbuf.empty();
    if (need == c.want_write) return;
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP | (need ? EPOLLOUT : 0u);
    ev.data.fd = c.fd;
    epoll_ctl(epfd_, EPOLL_CTL_MOD, c.fd, &ev);
    c.want_write = need;
}

void EventLoop::close_conn(int fd) {
    epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
    ::close(fd);
    conns_.erase(fd);
}

}  // namespace kv
