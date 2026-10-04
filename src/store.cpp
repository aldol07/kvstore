#include "store.h"

#include <chrono>

namespace kv {

// Unix time, not steady_clock: expiry timestamps are written to the AOF and must still mean
// the same moment after a restart. (Trade-off: a wall-clock jump shifts TTLs, as in Redis.)
int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

Store::Entry* Store::lookup(const std::string& key) {
    auto it = map_.find(key);
    if (it == map_.end()) return nullptr;
    if (it->second.expire_at_ms != 0 && it->second.expire_at_ms <= clock_()) {
        map_.erase(it);
        return nullptr;
    }
    return &it->second;
}

std::optional<std::string> Store::get(const std::string& key) {
    Entry* e = lookup(key);
    if (!e) return std::nullopt;
    return e->value;
}

void Store::set(const std::string& key, std::string value, int64_t expire_at_ms) {
    map_[key] = Entry{std::move(value), expire_at_ms};
}

bool Store::exists(const std::string& key) { return lookup(key) != nullptr; }

bool Store::del(const std::string& key) {
    if (!lookup(key)) return false;
    map_.erase(key);
    return true;
}

int64_t Store::pttl(const std::string& key) {
    Entry* e = lookup(key);
    if (!e) return -2;
    if (e->expire_at_ms == 0) return -1;
    return e->expire_at_ms - clock_();
}

bool Store::expire_at(const std::string& key, int64_t at_ms) {
    Entry* e = lookup(key);
    if (!e) return false;
    if (at_ms <= clock_()) { map_.erase(key); return true; }
    e->expire_at_ms = at_ms;
    return true;
}

bool Store::persist(const std::string& key) {
    Entry* e = lookup(key);
    if (!e || e->expire_at_ms == 0) return false;
    e->expire_at_ms = 0;
    return true;
}

std::vector<std::string> Store::keys(std::string_view pattern) {
    std::vector<std::string> out;
    int64_t t = clock_();
    for (auto it = map_.begin(); it != map_.end();) {
        if (it->second.expire_at_ms != 0 && it->second.expire_at_ms <= t) { it = map_.erase(it); continue; }
        if (glob_match(pattern, it->first)) out.push_back(it->first);
        ++it;
    }
    return out;
}

size_t Store::size() {
    keys("*");  // purge expired so DBSIZE is accurate (O(n); fine for MVP)
    return map_.size();
}

bool glob_match(std::string_view p, std::string_view s) {
    size_t pi = 0, si = 0, star_p = std::string_view::npos, star_s = 0;
    while (si < s.size()) {
        if (pi < p.size()) {
            char c = p[pi];
            if (c == '*') { star_p = pi++; star_s = si; continue; }
            if (c == '?') { ++pi; ++si; continue; }
            if (c == '[') {
                size_t j = pi + 1;
                bool neg = j < p.size() && p[j] == '^';
                if (neg) ++j;
                bool match = false;
                while (j < p.size() && p[j] != ']') {
                    if (p[j] == '\\' && j + 1 < p.size()) { match |= p[j + 1] == s[si]; j += 2; }
                    else if (j + 2 < p.size() && p[j + 1] == '-' && p[j + 2] != ']') {
                        char lo = p[j], hi = p[j + 2];
                        if (lo > hi) std::swap(lo, hi);
                        match |= s[si] >= lo && s[si] <= hi;
                        j += 3;
                    } else { match |= p[j] == s[si]; ++j; }
                }
                if (j < p.size() && match != neg) { pi = j + 1; ++si; continue; }
            } else {
                if (c == '\\' && pi + 1 < p.size()) c = p[++pi];
                if (c == s[si]) { ++pi; ++si; continue; }
            }
        }
        if (star_p == std::string_view::npos) return false;
        pi = star_p + 1;
        si = ++star_s;
    }
    while (pi < p.size() && p[pi] == '*') ++pi;
    return pi == p.size();
}

}  // namespace kv
