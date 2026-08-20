#ifndef UFX_LISTENER_H
#define UFX_LISTENER_H

#include "ufx/types.h"

namespace ufx {

enum class SnapshotKind {
    kOrders,
    kPositions,
    kAccounts
};

class IMarketListener {
public:
    virtual ~IMarketListener() {}

    // Exceptions do not cross Session thread/API boundaries. A non-allocation
    // callback exception stops a running Session with code -1009; std::bad_alloc
    // retains the existing allocation-failure code -1006.
    virtual void OnSnapshotBegin(SnapshotKind kind) = 0;
    virtual void OnOrderUpdate(const OrderView& order) = 0;
    virtual void OnOrderRemoved(const OrderView& order) = 0;
    virtual void OnPositionUpdate(const PositionView& position) = 0;
    virtual void OnPositionRemoved(const PositionView& position) = 0;
    virtual void OnAccountUpdate(const AccountView& account) = 0;
    virtual void OnAccountRemoved(const AccountView& account) = 0;
    virtual void OnSnapshotEnd(SnapshotKind kind) = 0;
    virtual void OnSessionEvent(int code, const char* msg) = 0;
};

}  // namespace ufx

#endif
