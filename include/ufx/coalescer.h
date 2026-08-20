#ifndef UFX_COALESCER_H
#define UFX_COALESCER_H

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace ufx {

class RefreshCoalescer {
public:
    explicit RefreshCoalescer(int window_ms = 100);

    void Request(const std::string& key, int64_t now_ms);
    void RequestNow(const std::string& key, int64_t now_ms);
    std::vector<std::string> Due(int64_t now_ms);
    void MarkInFlight(const std::string& key, size_t operation_count);
    void MarkDone(const std::string& key, int64_t now_ms);
    bool HasPending(const std::string& key) const;
    bool HasInFlight(const std::string& key) const;
    bool NextDueMs(int64_t* due_ms) const;

private:
    struct Slot {
        int64_t due_ms;
        bool pending;
        size_t outstanding;
        bool dirty;
        Slot() : due_ms(0), pending(false), outstanding(0), dirty(false) {}
    };

    int window_ms_;
    std::map<std::string, Slot> slots_;
};

}  // namespace ufx

#endif
