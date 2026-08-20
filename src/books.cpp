#include "ufx/books.h"

#include <utility>

namespace ufx {

namespace {

bool SameOrder(const OrderView& a, const OrderView& b) {
    return a.business_date == b.business_date && a.entrust_no == b.entrust_no &&
           a.msgtype == b.msgtype && a.entrust_state == b.entrust_state &&
           a.account_code == b.account_code && a.asset_no == b.asset_no &&
           a.combi_no == b.combi_no && a.market_no == b.market_no &&
           a.stock_code == b.stock_code &&
           a.entrust_direction == b.entrust_direction &&
           a.entrust_price == b.entrust_price &&
           a.entrust_amount == b.entrust_amount && a.deal_amount == b.deal_amount &&
           a.last_fill_amount == b.last_fill_amount &&
           a.cancel_amount == b.cancel_amount && a.extsystem_id == b.extsystem_id &&
           a.deal_no == b.deal_no &&
           a.realdeal_serial_no == b.realdeal_serial_no;
}

bool SamePosition(const PositionView& a, const PositionView& b) {
    return a.account_code == b.account_code && a.asset_no == b.asset_no &&
           a.combi_no == b.combi_no && a.market_no == b.market_no &&
           a.stock_code == b.stock_code && a.invest_type == b.invest_type &&
           a.stockholder_id == b.stockholder_id && a.hold_seat == b.hold_seat &&
           a.current_amount == b.current_amount && a.enable_amount == b.enable_amount &&
           a.unsellable_amount == b.unsellable_amount &&
           a.pre_buy_amount == b.pre_buy_amount &&
           a.pre_sell_amount == b.pre_sell_amount &&
           a.today_buy_amount == b.today_buy_amount &&
           a.today_sell_amount == b.today_sell_amount;
}

bool SameAccount(const AccountView& a, const AccountView& b) {
    return a.account_code == b.account_code && a.asset_no == b.asset_no &&
           a.enable_balance_t0 == b.enable_balance_t0 &&
           a.enable_balance_t1 == b.enable_balance_t1 &&
           a.current_balance == b.current_balance;
}

bool CopyIfPresent(const std::string& incoming, std::string* stored) {
    if (!incoming.empty() && incoming != *stored) {
        *stored = incoming;
        return true;
    }
    return false;
}

template <typename T>
bool AssignIfDifferent(const T& incoming, T* stored) {
    if (incoming == *stored) {
        return false;
    }
    *stored = incoming;
    return true;
}

bool IsTerminalState(const std::string& state) {
    return state == "5" || state == "7" || state == "8" || state == "9" ||
           state == "c" || state == "d" || state == "e" || state == "G";
}

bool CanApplyConfirmation(const std::string& state) {
    return state.empty() || state == "1" || state == "2" || state == "3" ||
           state == "4";
}

}  // namespace

bool OrderBook::UpsertFromPush(const OrderView& incoming, const OrderView** stored) {
    if (incoming.entrust_no <= 0) {
        return false;
    }

    OrderSnapshot::iterator it = orders_.find(incoming.entrust_no);
    if (it == orders_.end()) {
        it = orders_.insert(std::make_pair(incoming.entrust_no, incoming)).first;
        if (stored != NULL) {
            *stored = &it->second;
        }
        return true;
    }

    OrderView& current = it->second;
    bool changed = false;
    if (current.business_date == 0 && incoming.business_date != 0) {
        current.business_date = incoming.business_date;
        changed = true;
    }
    changed = CopyIfPresent(incoming.account_code, &current.account_code) || changed;
    changed = CopyIfPresent(incoming.asset_no, &current.asset_no) || changed;
    changed = CopyIfPresent(incoming.combi_no, &current.combi_no) || changed;
    changed = CopyIfPresent(incoming.market_no, &current.market_no) || changed;
    changed = CopyIfPresent(incoming.stock_code, &current.stock_code) || changed;
    changed =
        CopyIfPresent(incoming.entrust_direction, &current.entrust_direction) || changed;
    if (incoming.extsystem_id != 0 && incoming.extsystem_id != current.extsystem_id) {
        current.extsystem_id = incoming.extsystem_id;
        changed = true;
    }

    if (incoming.msgtype == "a") {
        if (current.entrust_state.empty()) {
            changed = AssignIfDifferent(incoming.msgtype, &current.msgtype) || changed;
            changed =
                AssignIfDifferent(incoming.entrust_state, &current.entrust_state) || changed;
            changed =
                AssignIfDifferent(incoming.entrust_price, &current.entrust_price) || changed;
            changed =
                AssignIfDifferent(incoming.entrust_amount, &current.entrust_amount) || changed;
        }
    } else if (incoming.msgtype == "b") {
        if (!IsTerminalState(current.entrust_state) &&
            CanApplyConfirmation(current.entrust_state) && current.deal_amount == 0) {
            changed = AssignIfDifferent(incoming.msgtype, &current.msgtype) || changed;
            changed =
                AssignIfDifferent(incoming.entrust_state, &current.entrust_state) || changed;
            changed =
                AssignIfDifferent(incoming.entrust_price, &current.entrust_price) || changed;
            changed =
                AssignIfDifferent(incoming.entrust_amount, &current.entrust_amount) || changed;
        }
    } else if (incoming.msgtype == "c") {
        if (current.deal_amount == 0) {
            changed = AssignIfDifferent(incoming.msgtype, &current.msgtype) || changed;
            changed =
                AssignIfDifferent(incoming.entrust_state, &current.entrust_state) || changed;
        }
    } else if (incoming.msgtype == "g") {
        const bool advances_total = incoming.deal_amount > current.deal_amount;
        const bool annotates_snapshot = incoming.deal_amount == current.deal_amount &&
                                        current.deal_no.empty() &&
                                        !incoming.deal_no.empty();
        if (advances_total || annotates_snapshot) {
            changed = AssignIfDifferent(incoming.msgtype, &current.msgtype) || changed;
            changed = AssignIfDifferent(incoming.last_fill_amount,
                                        &current.last_fill_amount) ||
                      changed;
            changed = AssignIfDifferent(incoming.deal_no, &current.deal_no) || changed;
            changed = AssignIfDifferent(incoming.realdeal_serial_no,
                                        &current.realdeal_serial_no) ||
                      changed;
            if (advances_total) {
                changed = AssignIfDifferent(incoming.entrust_state,
                                            &current.entrust_state) ||
                          changed;
                changed = AssignIfDifferent(incoming.entrust_amount,
                                            &current.entrust_amount) ||
                          changed;
                changed = AssignIfDifferent(incoming.deal_amount,
                                            &current.deal_amount) ||
                          changed;
                changed = AssignIfDifferent(incoming.cancel_amount,
                                            &current.cancel_amount) ||
                          changed;
            }
        }
    }

    if (stored != NULL) {
        *stored = &current;
    }
    return changed;
}

const OrderView* OrderBook::Find(int64_t entrust_no) const {
    std::map<int64_t, OrderView>::const_iterator it = orders_.find(entrust_no);
    return it == orders_.end() ? NULL : &it->second;
}

std::vector<OrderView> OrderBook::All() const {
    std::vector<OrderView> out;
    out.reserve(orders_.size());
    for (std::map<int64_t, OrderView>::const_iterator it = orders_.begin();
         it != orders_.end(); ++it) {
        out.push_back(it->second);
    }
    return out;
}

BookChanges<OrderView> OrderBook::ReplaceAll(OrderSnapshot orders) {
    BookChanges<OrderView> changes;
    OrderSnapshot::const_iterator previous = orders_.begin();
    OrderSnapshot::iterator current = orders.begin();
    while (previous != orders_.end() || current != orders.end()) {
        if (current == orders.end() ||
            (previous != orders_.end() && previous->first < current->first)) {
            changes.removed.push_back(previous->second);
            ++previous;
            continue;
        }
        if (previous == orders_.end() || current->first < previous->first) {
            changes.updated.push_back(current->second);
            ++current;
            continue;
        }

        if (previous->second.business_date == current->second.business_date &&
            previous->second.deal_amount == current->second.deal_amount) {
            current->second.last_fill_amount = previous->second.last_fill_amount;
            current->second.deal_no = previous->second.deal_no;
            current->second.realdeal_serial_no = previous->second.realdeal_serial_no;
        }
        if (!SameOrder(previous->second, current->second)) {
            changes.updated.push_back(current->second);
        }
        ++previous;
        ++current;
    }
    orders_.swap(orders);
    return changes;
}

void OrderBook::Clear() { orders_.clear(); }

PositionView PositionBook::Upsert(PositionView row) {
    row.unsellable_amount = UnsellableAmount(row.current_amount, row.enable_amount);
    positions_[PositionKey(row)] = row;
    return row;
}

const PositionView* PositionBook::Find(const std::string& key) const {
    std::map<std::string, PositionView>::const_iterator it = positions_.find(key);
    return it == positions_.end() ? NULL : &it->second;
}

std::vector<PositionView> PositionBook::All() const {
    std::vector<PositionView> out;
    out.reserve(positions_.size());
    for (std::map<std::string, PositionView>::const_iterator it = positions_.begin();
         it != positions_.end(); ++it) {
        out.push_back(it->second);
    }
    return out;
}

BookChanges<PositionView> PositionBook::ReplaceScope(
    const QueryScope& scope, PositionSnapshot rows) {
    for (PositionSnapshot::iterator row = rows.begin(); row != rows.end(); ++row) {
        row->second.unsellable_amount =
            UnsellableAmount(row->second.current_amount, row->second.enable_amount);
    }

    BookChanges<PositionView> changes;
    PositionSnapshot::iterator it = positions_.begin();
    while (it != positions_.end()) {
        if (MatchesScope(it->second, scope) && rows.find(it->first) == rows.end()) {
            changes.removed.push_back(it->second);
            positions_.erase(it++);
        } else {
            ++it;
        }
    }
    for (PositionSnapshot::iterator row = rows.begin(); row != rows.end(); ++row) {
        const PositionSnapshot::const_iterator old = positions_.find(row->first);
        if (old == positions_.end() || !SamePosition(old->second, row->second)) {
            changes.updated.push_back(row->second);
        }
        positions_[row->first] = std::move(row->second);
    }
    return changes;
}

void PositionBook::Clear() { positions_.clear(); }

AccountView AccountBook::Upsert(const AccountView& row) {
    accounts_[AccountKey(row)] = row;
    return row;
}

const AccountView* AccountBook::Find(const std::string& key) const {
    std::map<std::string, AccountView>::const_iterator it = accounts_.find(key);
    return it == accounts_.end() ? NULL : &it->second;
}

std::vector<AccountView> AccountBook::All() const {
    std::vector<AccountView> out;
    out.reserve(accounts_.size());
    for (std::map<std::string, AccountView>::const_iterator it = accounts_.begin();
         it != accounts_.end(); ++it) {
        out.push_back(it->second);
    }
    return out;
}

BookChanges<AccountView> AccountBook::ReplaceScope(
    const std::string& scope_key, AccountSnapshot rows) {
    std::set<std::string> next_keys;
    for (AccountSnapshot::const_iterator row = rows.begin(); row != rows.end(); ++row) {
        next_keys.insert(row->first);
    }

    BookChanges<AccountView> changes;
    const std::set<std::string> previous_keys = scope_keys_[scope_key];
    for (std::set<std::string>::const_iterator key = previous_keys.begin();
         key != previous_keys.end(); ++key) {
        if (next_keys.find(*key) != next_keys.end()) {
            continue;
        }
        bool used_elsewhere = false;
        for (std::map<std::string, std::set<std::string> >::const_iterator scope =
                 scope_keys_.begin();
             scope != scope_keys_.end(); ++scope) {
            if (scope->first != scope_key && scope->second.find(*key) != scope->second.end()) {
                used_elsewhere = true;
                break;
            }
        }
        if (!used_elsewhere) {
            const std::map<std::string, AccountView>::iterator old = accounts_.find(*key);
            if (old != accounts_.end()) {
                changes.removed.push_back(old->second);
                accounts_.erase(old);
            }
        }
    }
    for (AccountSnapshot::iterator row = rows.begin(); row != rows.end(); ++row) {
        const AccountSnapshot::const_iterator old = accounts_.find(row->first);
        if (old == accounts_.end() || !SameAccount(old->second, row->second)) {
            changes.updated.push_back(row->second);
        }
        accounts_[row->first] = std::move(row->second);
    }
    scope_keys_[scope_key] = next_keys;
    return changes;
}

void AccountBook::Clear() {
    accounts_.clear();
    scope_keys_.clear();
}

bool IsOrderMessage(const std::string& msgtype) {
    return msgtype.size() == 1 && msgtype[0] >= 'a' && msgtype[0] <= 'g';
}

bool IsCancellationMessage(const std::string& msgtype) {
    return msgtype == "d" || msgtype == "e" || msgtype == "f";
}

bool ShouldRefreshBooks(const std::string& msgtype) {
    return msgtype == "a" || msgtype == "c" || msgtype == "e" || msgtype == "g" ||
           msgtype == "P";
}

}  // namespace ufx
