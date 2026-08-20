#ifndef UFX_BOOKS_H
#define UFX_BOOKS_H

#include "ufx/types.h"

#include <map>
#include <set>
#include <string>
#include <vector>

namespace ufx {

template <typename T>
struct BookChanges {
    std::vector<T> updated;
    std::vector<T> removed;
};

typedef std::map<int64_t, OrderView> OrderSnapshot;
typedef std::map<std::string, PositionView> PositionSnapshot;
typedef std::map<std::string, AccountView> AccountSnapshot;

class OrderBook {
public:
    bool UpsertFromPush(const OrderView& incoming, const OrderView** stored);
    const OrderView* Find(int64_t entrust_no) const;
    std::vector<OrderView> All() const;
    BookChanges<OrderView> ReplaceAll(OrderSnapshot orders);
    void Clear();

private:
    OrderSnapshot orders_;
};

class PositionBook {
public:
    PositionView Upsert(PositionView row);
    const PositionView* Find(const std::string& key) const;
    std::vector<PositionView> All() const;
    BookChanges<PositionView> ReplaceScope(const QueryScope& scope,
                                           PositionSnapshot rows);
    void Clear();

private:
    PositionSnapshot positions_;
};

class AccountBook {
public:
    AccountView Upsert(const AccountView& row);
    const AccountView* Find(const std::string& key) const;
    std::vector<AccountView> All() const;
    BookChanges<AccountView> ReplaceScope(const std::string& scope_key,
                                          AccountSnapshot rows);
    void Clear();

private:
    AccountSnapshot accounts_;
    std::map<std::string, std::set<std::string> > scope_keys_;
};

bool IsOrderMessage(const std::string& msgtype);
bool IsCancellationMessage(const std::string& msgtype);
bool ShouldRefreshBooks(const std::string& msgtype);

}  // namespace ufx

#endif
