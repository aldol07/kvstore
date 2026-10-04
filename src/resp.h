// RESP2 wire protocol: incremental request parser + reply encoders.
#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace kv {

using Args = std::vector<std::string>;

enum class ParseStatus { Ok, NeedMore, Error };

struct ParseResult {
    ParseStatus status = ParseStatus::NeedMore;
    Args args;            // command + arguments (valid when Ok)
    size_t consumed = 0;  // bytes consumed from the buffer (valid when Ok)
    std::string error;    // protocol error message (valid when Error)
};

// Limits protect the server from a single client exhausting memory.
inline constexpr size_t kMaxBulkLen = 64 * 1024 * 1024;  // 64 MB per argument
inline constexpr size_t kMaxArrayLen = 1024 * 1024;      // 1M arguments
inline constexpr size_t kMaxInlineLen = 64 * 1024;       // inline command line

// Parse one command from the front of `buf`. Clients send arrays of bulk strings
// ("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"); telnet-style inline commands ("GET k\r\n") also work.
// Returns NeedMore when the frame is incomplete, so partial reads and pipelining both work.
ParseResult parse_command(std::string_view buf);

namespace resp {
void simple(std::string& out, std::string_view s);       // +OK
void error(std::string& out, std::string_view msg);      // -ERR ...
void integer(std::string& out, int64_t n);               // :1
void bulk(std::string& out, std::string_view s);         // $3\r\nfoo
void null_bulk(std::string& out);                        // $-1
void array_header(std::string& out, size_t n);           // *n
}  // namespace resp

}  // namespace kv
