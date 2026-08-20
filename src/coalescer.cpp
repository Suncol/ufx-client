#include "ufx/coalescer.h"

namespace ufx {

RefreshCoalescer::RefreshCoalescer(int window_ms) : window_ms_(window_ms) {
    if (window_ms_ < 0) {
        window_ms_ = 0;
    }
}

void RefreshCoalescer::Request(const std::string& key, int64_t now_ms) {
    Slot& slot = slots_[key];
    if (slot.outstanding != 0) {
        slot.dirty = true;
        return;
    }
    if (!slot.pending) {
        slot.pending = true;
        slot.due_ms = now_ms + window_ms_;
    }
}

void RefreshCoalescer::RequestNow(const std::string& key, int64_t now_ms) {
    Slot& slot = slots_[key];
    if (slot.outstanding != 0) {
        slot.dirty = true;
        return;
    }
    slot.pending = true;
    slot.due_ms = now_ms;
}

std::vector<std::string> RefreshCoalescer::Due(int64_t now_ms) {
    std::vector<std::string> ready;
    for (std::map<std::string, Slot>::iterator it = slots_.begin(); it != slots_.end();
         ++it) {
        if (it->second.pending && it->second.outstanding == 0 &&
            now_ms >= it->second.due_ms) {
            ready.push_back(it->first);
        }
    }
    return ready;
}

void RefreshCoalescer::MarkInFlight(const std::string& key, size_t operation_count) {
    Slot& slot = slots_[key];
    slot.pending = false;
    slot.outstanding = operation_count;
}

void RefreshCoalescer::MarkDone(const std::string& key, int64_t now_ms) {
    std::map<std::string, Slot>::iterator it = slots_.find(key);
    if (it == slots_.end()) {
        return;
    }
    if (it->second.outstanding == 0) {
        return;
    }
    --it->second.outstanding;
    if (it->second.outstanding != 0) {
        return;
    }
    if (it->second.dirty) {
        it->second.dirty = false;
        it->second.pending = true;
        it->second.due_ms = now_ms + window_ms_;
    }
}

bool RefreshCoalescer::HasInFlight(const std::string& key) const {
    std::map<std::string, Slot>::const_iterator it = slots_.find(key);
    return it != slots_.end() && it->second.outstanding != 0;
}

bool RefreshCoalescer::HasPending(const std::string& key) const {
    std::map<std::string, Slot>::const_iterator it = slots_.find(key);
    return it != slots_.end() && it->second.pending;
}

bool RefreshCoalescer::NextDueMs(int64_t* due_ms) const {
    if (due_ms == NULL) {
        return false;
    }
    bool found = false;
    int64_t earliest = 0;
    for (std::map<std::string, Slot>::const_iterator it = slots_.begin();
         it != slots_.end(); ++it) {
        if (!it->second.pending || it->second.outstanding != 0) {
            continue;
        }
        if (!found || it->second.due_ms < earliest) {
            found = true;
            earliest = it->second.due_ms;
        }
    }
    if (found) {
        *due_ms = earliest;
    }
    return found;
}

}  // namespace ufx
