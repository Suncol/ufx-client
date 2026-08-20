#ifndef UFX_LISTENER_GUARD_H
#define UFX_LISTENER_GUARD_H

#include <exception>
#include <new>

namespace ufx {
namespace detail {

enum ListenerCallResult {
    kListenerCallSucceeded,
    kListenerCallThrewBadAlloc,
    kListenerCallThrewStdException,
    kListenerCallThrewUnknownException
};

template <typename Callback>
ListenerCallResult InvokeListenerNoThrow(const Callback& callback) noexcept {
    try {
        callback();
        return kListenerCallSucceeded;
    } catch (const std::bad_alloc&) {
        return kListenerCallThrewBadAlloc;
    } catch (const std::exception&) {
        return kListenerCallThrewStdException;
    } catch (...) {
        return kListenerCallThrewUnknownException;
    }
}

}  // namespace detail
}  // namespace ufx

#endif
