#ifndef UFX_PRINT_LISTENER_H
#define UFX_PRINT_LISTENER_H

#include "ufx/listener.h"

#include <cstdio>

namespace ufx {

class PrintListener : public IMarketListener {
public:
    void OnSnapshotBegin(SnapshotKind kind) {
        std::printf("[snapshot] begin kind=%d\n", static_cast<int>(kind));
    }

    void OnOrderUpdate(const OrderView& o) {
        std::printf("[order] no=%lld type=%s state=%s %s/%s %s %s price=%s "
                    "amt=%lld deal=%lld fill=%lld cancel=%lld\n",
                    static_cast<long long>(o.entrust_no), o.msgtype.c_str(),
                    o.entrust_state.c_str(),
                    o.account_code.c_str(), o.combi_no.c_str(), o.market_no.c_str(),
                    o.stock_code.c_str(), o.entrust_price.ToString().c_str(),
                    static_cast<long long>(o.entrust_amount),
                    static_cast<long long>(o.deal_amount),
                    static_cast<long long>(o.last_fill_amount),
                    static_cast<long long>(o.cancel_amount));
    }

    void OnOrderRemoved(const OrderView& o) {
        std::printf("[order-removed] no=%lld\n", static_cast<long long>(o.entrust_no));
    }

    void OnPositionUpdate(const PositionView& p) {
        std::printf("[position] %s/%s %s.%s current=%lld enable=%lld "
                    "unsellable=%lld pre_buy=%lld pre_sell=%lld today_buy=%lld "
                    "today_sell=%lld\n",
                    p.account_code.c_str(), p.combi_no.c_str(), p.market_no.c_str(),
                    p.stock_code.c_str(), static_cast<long long>(p.current_amount),
                    static_cast<long long>(p.enable_amount),
                    static_cast<long long>(p.unsellable_amount),
                    static_cast<long long>(p.pre_buy_amount),
                    static_cast<long long>(p.pre_sell_amount),
                    static_cast<long long>(p.today_buy_amount),
                    static_cast<long long>(p.today_sell_amount));
    }

    void OnPositionRemoved(const PositionView& p) {
        std::printf("[position-removed] %s/%s %s.%s\n", p.account_code.c_str(),
                    p.combi_no.c_str(), p.market_no.c_str(), p.stock_code.c_str());
    }

    void OnAccountUpdate(const AccountView& a) {
        std::printf("[account] %s/%s t0=%s t1=%s balance=%s\n",
                    a.account_code.c_str(), a.asset_no.c_str(),
                    a.enable_balance_t0.ToString().c_str(),
                    a.enable_balance_t1.ToString().c_str(),
                    a.current_balance.ToString().c_str());
    }

    void OnAccountRemoved(const AccountView& a) {
        std::printf("[account-removed] %s/%s\n", a.account_code.c_str(),
                    a.asset_no.c_str());
    }

    void OnSnapshotEnd(SnapshotKind kind) {
        std::printf("[snapshot] end kind=%d\n", static_cast<int>(kind));
    }

    void OnSessionEvent(int code, const char* msg) {
        std::printf("[session] code=%d msg=%s\n", code, msg ? msg : "");
    }
};

}  // namespace ufx

#endif
