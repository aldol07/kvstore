#include "resp.h"

#include <charconv>

namespace kv {
namespace {

// Find "\r\n" starting at pos; returns npos if absent.
size_t find_crlf(std::string_view buf, size_t pos) {
    size_t i = buf.find('\r', pos);
    while (i != std::string_view::npos) {
        if (i + 1 >= buf.size()) return std::string_view::npos;  // need the '\n'
        if (buf[i + 1] == '\n') return i;
        i = buf.find('\r', i + 1);
    }
    return std::string_view::npos;
}

bool parse_int(std::string_view s, int64_t& out) {
    if (s.empty()) return false;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc() && p == s.data() + s.size();
}

ParseResult err(std::string msg) {
    ParseResult r;
    r.status = ParseStatus::Error;
    r.error = std::move(msg);
    return r;
}

ParseResult parse_inline(std::string_view buf) {
    size_t nl = buf.find('\n');
    if (nl == std::string_view::npos) {
        if (buf.size() > kMaxInlineLen) return err("Protocol error: too big inline request");
        return {};
    }
    std::string_view line = buf.substr(0, nl);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    ParseResult r;
    size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) ++i;
        size_t start = i;
        while (i < line.size() && line[i] != ' ' && line[i] != '\t') ++i;
        if (i > start) r.args.emplace_back(line.substr(start, i - start));
    }
    r.status = ParseStatus::Ok;
    r.consumed = nl + 1;
    return r;  // empty args (blank line) is Ok; caller ignores it
}

}  // namespace

ParseResult parse_command(std::string_view buf) {
    if (buf.empty()) return {};
    if (buf[0] != '*') return parse_inline(buf);

    size_t eol = find_crlf(buf, 1);
    if (eol == std::string_view::npos) return {};
    int64_t n = 0;
    if (!parse_int(buf.substr(1, eol - 1), n) || n > static_cast<int64_t>(kMaxArrayLen))
        return err("Protocol error: invalid multibulk length");

    ParseResult r;
    size_t pos = eol + 2;
    if (n <= 0) {  // "*0" or "*-1": nothing to execute
        r.status = ParseStatus::Ok;
        r.consumed = pos;
        return r;
    }
    r.args.reserve(static_cast<size_t>(n));
    for (int64_t i = 0; i < n; ++i) {
        if (pos >= buf.size()) return {};
        if (buf[pos] != '$')
            return err(std::string("Protocol error: expected '$', got '") + buf[pos] + "'");
        eol = find_crlf(buf, pos + 1);
        if (eol == std::string_view::npos) return {};
        int64_t len = 0;
        if (!parse_int(buf.substr(pos + 1, eol - pos - 1), len) || len < 0 ||
            len > static_cast<int64_t>(kMaxBulkLen))
            return err("Protocol error: invalid bulk length");
        size_t start = eol + 2;
        if (buf.size() < start + static_cast<size_t>(len) + 2) return {};
        if (buf[start + len] != '\r' || buf[start + len + 1] != '\n')
            return err("Protocol error: bulk string not terminated by CRLF");
        r.args.emplace_back(buf.substr(start, static_cast<size_t>(len)));
        pos = start + static_cast<size_t>(len) + 2;
    }
    r.status = ParseStatus::Ok;
    r.consumed = pos;
    return r;
}

namespace resp {
void simple(std::string& out, std::string_view s) { out += '+'; out += s; out += "\r\n"; }
void error(std::string& out, std::string_view m) { out += '-'; out += m; out += "\r\n"; }
void integer(std::string& out, int64_t n) { out += ':'; out += std::to_string(n); out += "\r\n"; }
void bulk(std::string& out, std::string_view s) {
    out += '$'; out += std::to_string(s.size()); out += "\r\n"; out += s; out += "\r\n";
}
void null_bulk(std::string& out) { out += "$-1\r\n"; }
void array_header(std::string& out, size_t n) { out += '*'; out += std::to_string(n); out += "\r\n"; }
}  // namespace resp

}  // namespace kv
