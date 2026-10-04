// Command table + dispatch. Handlers append a RESP reply to `out`.
#pragma once
#include <string>
#include <vector>

#include "resp.h"
#include "store.h"

namespace kv {

class Aof;

struct Context {
    explicit Context(Store& s, Aof* a = nullptr) : store(s), aof(a) {}
    Store& store;
    Aof* aof;                        // null when persistence is off (and during AOF replay)
    bool close_after_reply = false;  // set by QUIT
    // Write commands that changed the keyspace push a replay-safe form of themselves here
    // (relative TTLs become absolute timestamps), which the event loop appends to the AOF.
    std::vector<Args> to_log;
};

// Executes args[0] with args[1..]; always appends exactly one reply.
void dispatch(Context& ctx, const Args& args, std::string& out);

}  // namespace kv
