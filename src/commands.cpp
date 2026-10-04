#include "commands.h"

#include "aof.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <unordered_map>

namespace kv {
namespace {

using Handler = void (*)(Context&, const Args&, std::string&);

struct Command {
    Handler fn;
    int arity;  // Redis convention: >0 exact argc, <0 minimum argc (both include the name)
};

std::string lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

bool to_int(const std::string& s, int64_t& out) {
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc() && p == s.data() + s.size();
}

void ping(Context&, const Args& a, std::string& out) {
    if (a.size() == 1) resp::simple(out, "PONG");
    else if (a.size() == 2) resp::bulk(out, a[1]);
    else resp::error(out, "ERR wrong number of arguments for 'ping' command");
}

void echo(Context&, const Args& a, std::string& out) { resp::bulk(out, a[1]); }

void get(Context& c, const Args& a, std::string& out) {
    auto v = c.store.get(a[1]);
    v ? resp::bulk(out, *v) : resp::null_bulk(out);
}

// SET key value [EX seconds | PX milliseconds | EXAT unix-s | PXAT unix-ms] [NX | XX] [GET]
void set(Context& c, const Args& a, std::string& out) {
    int64_t expire_at = 0;
    bool nx = false, xx = false, want_get = false;
    for (size_t i = 3; i < a.size(); ++i) {
        std::string opt = lower(a[i]);
        if ((opt == "ex" || opt == "px" || opt == "exat" || opt == "pxat") && i + 1 < a.size() &&
            expire_at == 0) {
            int64_t n = 0;
            if (!to_int(a[i + 1], n)) return resp::error(out, "ERR value is not an integer or out of range");
            if (n <= 0) return resp::error(out, "ERR invalid expire time in 'set' command");
            if (opt == "ex") expire_at = c.store.now() + n * 1000;
            else if (opt == "px") expire_at = c.store.now() + n;
            else if (opt == "exat") expire_at = n * 1000;
            else expire_at = n;
            ++i;
        } else if (opt == "nx" && !xx) nx = true;
        else if (opt == "xx" && !nx) xx = true;
        else if (opt == "get") want_get = true;
        else return resp::error(out, "ERR syntax error");
    }
    auto old = want_get ? c.store.get(a[1]) : std::nullopt;
    bool present = want_get ? old.has_value() : c.store.exists(a[1]);
    bool skip = (nx && present) || (xx && !present);
    if (!skip) {
        c.store.set(a[1], a[2], expire_at);
        Args entry{"SET", a[1], a[2]};
        if (expire_at) entry.insert(entry.end(), {"PXAT", std::to_string(expire_at)});
        c.to_log.push_back(std::move(entry));
    }
    if (want_get) old ? resp::bulk(out, *old) : resp::null_bulk(out);
    else if (skip) resp::null_bulk(out);
    else resp::simple(out, "OK");
}

void del(Context& c, const Args& a, std::string& out) {
    int64_t n = 0;
    for (size_t i = 1; i < a.size(); ++i) n += c.store.del(a[i]);
    if (n > 0) c.to_log.push_back(a);
    resp::integer(out, n);
}

void exists(Context& c, const Args& a, std::string& out) {
    int64_t n = 0;
    for (size_t i = 1; i < a.size(); ++i) n += c.store.exists(a[i]);
    resp::integer(out, n);
}

void keys(Context& c, const Args& a, std::string& out) {
    auto ks = c.store.keys(a[1]);
    resp::array_header(out, ks.size());
    for (auto& k : ks) resp::bulk(out, k);
}

void dbsize(Context& c, const Args&, std::string& out) { resp::integer(out, static_cast<int64_t>(c.store.size())); }
void flushall(Context& c, const Args&, std::string& out) {
    c.store.clear();
    c.to_log.push_back({"FLUSHALL"});
    resp::simple(out, "OK");
}

// All four EXPIRE variants are logged as PEXPIREAT with an absolute time, so replaying the
// AOF after a restart doesn't restart the countdown.
void expire_generic(Context& c, const Args& a, std::string& out, int64_t unit_ms, bool absolute) {
    int64_t n = 0;
    if (!to_int(a[2], n)) return resp::error(out, "ERR value is not an integer or out of range");
    int64_t at = (absolute ? 0 : c.store.now()) + n * unit_ms;
    bool ok = c.store.expire_at(a[1], at);
    if (ok) c.to_log.push_back({"PEXPIREAT", a[1], std::to_string(at)});
    resp::integer(out, ok ? 1 : 0);
}
void expire(Context& c, const Args& a, std::string& out) { expire_generic(c, a, out, 1000, false); }
void pexpire(Context& c, const Args& a, std::string& out) { expire_generic(c, a, out, 1, false); }
void expireat(Context& c, const Args& a, std::string& out) { expire_generic(c, a, out, 1000, true); }
void pexpireat(Context& c, const Args& a, std::string& out) { expire_generic(c, a, out, 1, true); }
void persist(Context& c, const Args& a, std::string& out) {
    bool ok = c.store.persist(a[1]);
    if (ok) c.to_log.push_back(a);
    resp::integer(out, ok ? 1 : 0);
}

void pttl(Context& c, const Args& a, std::string& out) { resp::integer(out, c.store.pttl(a[1])); }
void ttl(Context& c, const Args& a, std::string& out) {
    int64_t ms = c.store.pttl(a[1]);
    resp::integer(out, ms < 0 ? ms : (ms + 500) / 1000);
}

// Compatibility stubs so redis-cli / redis-benchmark start cleanly.
void command(Context&, const Args&, std::string& out) { resp::array_header(out, 0); }
// CONFIG GET answers the two settings redis-benchmark asks about; anything else is empty.
void config(Context& c, const Args& a, std::string& out) {
    if (lower(a[1]) != "get") return resp::simple(out, "OK");
    std::string param = a.size() > 2 ? lower(a[2]) : "";
    if (param == "save" || param == "appendonly") {
        resp::array_header(out, 2);
        resp::bulk(out, param);
        resp::bulk(out, param == "save" ? "" : (c.aof ? "yes" : "no"));
    } else {
        resp::array_header(out, 0);
    }
}
void client(Context&, const Args&, std::string& out) { resp::simple(out, "OK"); }
void select(Context&, const Args& a, std::string& out) {
    a[1] == "0" ? resp::simple(out, "OK") : resp::error(out, "ERR DB index is out of range");
}
void info(Context& c, const Args&, std::string& out) {
    resp::bulk(out, "# Server\r\nkvstore_version:0.1.0\r\nredis_version:7.0.0-kvstore\r\n"
                    "# Keyspace\r\ndb0:keys=" + std::to_string(c.store.size()) + "\r\n");
}
void bgrewriteaof(Context& c, const Args&, std::string& out) {
    if (!c.aof) return resp::error(out, "ERR AOF is disabled (start the server with --aof FILE)");
    std::string err = c.aof->start_rewrite(c.store);
    err.empty() ? resp::simple(out, "Background append only file rewriting started")
                : resp::error(out, err);
}

void quit(Context& c, const Args&, std::string& out) { c.close_after_reply = true; resp::simple(out, "OK"); }

const std::unordered_map<std::string, Command>& table() {
    static const std::unordered_map<std::string, Command> t = {
        {"ping", {ping, -1}},       {"echo", {echo, 2}},         {"get", {get, 2}},
        {"set", {set, -3}},         {"del", {del, -2}},          {"exists", {exists, -2}},
        {"keys", {keys, 2}},        {"dbsize", {dbsize, 1}},     {"flushall", {flushall, -1}},
        {"flushdb", {flushall, -1}}, {"expire", {expire, 3}},    {"pexpire", {pexpire, 3}},
        {"expireat", {expireat, 3}}, {"pexpireat", {pexpireat, 3}},
        {"persist", {persist, 2}},  {"ttl", {ttl, 2}},           {"pttl", {pttl, 2}},
        {"command", {command, -1}}, {"config", {config, -2}},    {"client", {client, -2}},
        {"select", {select, 2}},    {"info", {info, -1}},        {"quit", {quit, 1}},
        {"bgrewriteaof", {bgrewriteaof, 1}},
    };
    return t;
}

}  // namespace

void dispatch(Context& ctx, const Args& args, std::string& out) {
    std::string name = lower(args[0]);
    auto it = table().find(name);
    if (it == table().end()) {
        std::string preview = args[0].substr(0, 64);
        return resp::error(out, "ERR unknown command '" + preview + "'");
    }
    const Command& cmd = it->second;
    int argc = static_cast<int>(args.size());
    if ((cmd.arity > 0 && argc != cmd.arity) || (cmd.arity < 0 && argc < -cmd.arity))
        return resp::error(out, "ERR wrong number of arguments for '" + name + "' command");
    cmd.fn(ctx, args, out);
}

}  // namespace kv
