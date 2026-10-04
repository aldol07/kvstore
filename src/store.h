// Keyspace. MVP: std::unordered_map + lazy expiry.
// Phase 2 swaps in a custom dict with incremental rehashing and adds active expiry.
#pragma once
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace kv {

using Clock = std::function<int64_t()>;  // milliseconds; injectable for tests
int64_t now_ms();

class Store {
public:
    explicit Store(Clock clock = now_ms) : clock_(std::move(clock)) {}

    std::optional<std::string> get(const std::string& key);
    // expire_at_ms == 0 means "no expiry"
    void set(const std::string& key, std::string value, int64_t expire_at_ms = 0);
    bool exists(const std::string& key);
    bool del(const std::string& key);
    // -2 = no key, -1 = no expiry, else remaining ms
    int64_t pttl(const std::string& key);
    bool expire_at(const std::string& key, int64_t at_ms);
    bool persist(const std::string& key);
    std::vector<std::string> keys(std::string_view pattern);
    size_t size();
    void clear() { map_.clear(); }
    int64_t now() const { return clock_(); }

    // Visit every live key: fn(key, value, expire_at_ms). Read-only, so it is safe to call
    // from a fork()ed child that must not change the parent's view.
    template <class Fn>
    void for_each(Fn fn) const {
        int64_t t = clock_();
        for (const auto& [k, e] : map_)
            if (e.expire_at_ms == 0 || e.expire_at_ms > t) fn(k, e.value, e.expire_at_ms);
    }

private:
    struct Entry {
        std::string value;
        int64_t expire_at_ms = 0;
    };
    // Lazy expiry: called on every access; deletes the key if its TTL passed.
    Entry* lookup(const std::string& key);

    std::unordered_map<std::string, Entry> map_;
    Clock clock_;
};

// Redis-style glob: * ? [abc] [a-z] [^a] and \x escapes.
bool glob_match(std::string_view pattern, std::string_view str);

}  // namespace kv
