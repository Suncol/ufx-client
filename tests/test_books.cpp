#include "ufx/books.h"
#include "ufx/coalescer.h"
#include "ufx/session.h"

#include "../src/listener_guard.h"

#include <cstdio>
#include <cstdlib>
#include <limits>
#include <stdexcept>

static int g_failed = 0;

#define CHECK(cond)                                                              \
    do {                                                                         \
        if (!(cond)) {                                                           \
            std::fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, \
                         #cond);                                                 \
            ++g_failed;                                                          \
        }                                                                        \
    } while (0)

static ufx::OrderView MakeOrder(int64_t no, const char* msg, int64_t total_fill) {
    ufx::OrderView order;
    order.business_date = 20260819;
    order.entrust_no = no;
    order.msgtype = msg;
    order.entrust_state = total_fill == 1000 ? "7" : "6";
    order.account_code = "A1";
    order.asset_no = "AS1";
    order.combi_no = "C1";
    order.market_no = "1";
    order.stock_code = "600570";
    order.entrust_direction = "1";
    order.entrust_price = ufx::Price::FromScaled(100000);
    order.entrust_amount = 1000;
    order.deal_amount = total_fill;
    order.last_fill_amount = total_fill;
    order.cancel_amount = 0;
    order.extsystem_id = 1;
    order.deal_no = total_fill == 0 ? "" : "D" + std::to_string(total_fill);
    order.realdeal_serial_no = total_fill;
    return order;
}

static ufx::OrderSnapshot MakeOrderSnapshot(
    const std::vector<ufx::OrderView>& rows) {
    ufx::OrderSnapshot snapshot;
    for (size_t i = 0; i < rows.size(); ++i) {
        snapshot.insert(std::make_pair(rows[i].entrust_no, rows[i]));
    }
    return snapshot;
}

static ufx::PositionSnapshot MakePositionSnapshot(
    const std::vector<ufx::PositionView>& rows) {
    ufx::PositionSnapshot snapshot;
    for (size_t i = 0; i < rows.size(); ++i) {
        snapshot.insert(std::make_pair(ufx::PositionKey(rows[i]), rows[i]));
    }
    return snapshot;
}

static ufx::AccountSnapshot MakeAccountSnapshot(
    const std::vector<ufx::AccountView>& rows) {
    ufx::AccountSnapshot snapshot;
    for (size_t i = 0; i < rows.size(); ++i) {
        snapshot.insert(std::make_pair(ufx::AccountKey(rows[i]), rows[i]));
    }
    return snapshot;
}

static void TestFixedDecimal() {
    ufx::Price price;
    CHECK(ufx::Price::Parse("10.2500", &price));
    CHECK(price.ScaledValue() == 102500);
    CHECK(price.ToString() == "10.2500");
    const std::string price_text("10.2500");
    CHECK(ufx::Price::Parse(price_text, &price));
    CHECK(!ufx::Price::Parse(static_cast<const char*>(NULL), &price));

    ufx::Money money;
    CHECK(ufx::Money::Parse("-0.01", &money));
    CHECK(money.ScaledValue() == -1);
    CHECK(money.ToString() == "-0.01");
    CHECK(ufx::Money::Parse("12", &money));
    CHECK(money.ScaledValue() == 1200);
    CHECK(money.ToString() == "12.00");

    CHECK(!ufx::Money::Parse("1.001", &money));
    CHECK(!ufx::Money::Parse("1e3", &money));
    CHECK(!ufx::Money::Parse(".", &money));
    CHECK(!ufx::Money::Parse("", &money));
    CHECK(!ufx::Money::Parse("92233720368547759.00", &money));
    CHECK(ufx::Money::Parse("92233720368547758.07", &money));
    CHECK(money.ScaledValue() == std::numeric_limits<int64_t>::max());
    CHECK(money.ToString() == "92233720368547758.07");
    CHECK(ufx::Money::Parse("-92233720368547758.08", &money));
    CHECK(money.ScaledValue() == std::numeric_limits<int64_t>::min());
    CHECK(money.ToString() == "-92233720368547758.08");
}

static void TestSessionLimitDefaults() {
    const ufx::SessionConfig config;
    CHECK(config.max_event_queue_size > 0);
    CHECK(config.max_event_queue_bytes > 0);
    CHECK(config.max_dispatch_batch_size <= config.max_event_queue_size);
    CHECK(config.enqueue_timeout_ms > 0);
    CHECK(config.query_chain_timeout_ms >= config.query_timeout_ms);
    CHECK(config.max_query_pages > 0);
    CHECK(config.max_query_rows >= static_cast<size_t>(config.query_page_size));
    CHECK(config.max_query_bytes >= config.max_event_queue_bytes);
    CHECK(config.max_buffered_order_pushes > 0);

    const ufx::SessionStats stats;
    CHECK(stats.queued_events == 0);
    CHECK(stats.query_chains_started == 0);
    CHECK(stats.order_snapshots_started == 0);
}

static void TestListenerExceptionGuard() {
    bool called = false;
    const ufx::detail::ListenerCallResult success =
        ufx::detail::InvokeListenerNoThrow([&called] { called = true; });
    CHECK(called);
    CHECK(success == ufx::detail::kListenerCallSucceeded);

    const ufx::detail::ListenerCallResult allocation_failure =
        ufx::detail::InvokeListenerNoThrow([] { throw std::bad_alloc(); });
    CHECK(allocation_failure == ufx::detail::kListenerCallThrewBadAlloc);

    const ufx::detail::ListenerCallResult standard_exception =
        ufx::detail::InvokeListenerNoThrow([] {
            throw std::runtime_error("listener failure");
        });
    CHECK(standard_exception == ufx::detail::kListenerCallThrewStdException);

    const ufx::detail::ListenerCallResult unknown_exception =
        ufx::detail::InvokeListenerNoThrow([] { throw 7; });
    CHECK(unknown_exception == ufx::detail::kListenerCallThrewUnknownException);
}

static void TestCumulativeDealIsIdempotent() {
    ufx::OrderBook book;
    const ufx::OrderView* stored = NULL;

    ufx::OrderView initial = MakeOrder(101, "a", 0);
    initial.entrust_state = "2";
    CHECK(book.UpsertFromPush(initial, &stored));
    CHECK(stored != NULL && stored->deal_amount == 0);

    ufx::OrderView fill1 = MakeOrder(101, "g", 200);
    fill1.last_fill_amount = 200;
    CHECK(book.UpsertFromPush(fill1, &stored));
    CHECK(stored != NULL && stored->deal_amount == 200);
    CHECK(stored != NULL && stored->last_fill_amount == 200);

    CHECK(!book.UpsertFromPush(fill1, &stored));
    CHECK(stored != NULL && stored->deal_amount == 200);

    ufx::OrderView fill2 = MakeOrder(101, "g", 500);
    fill2.last_fill_amount = 300;
    CHECK(book.UpsertFromPush(fill2, &stored));
    CHECK(stored != NULL && stored->deal_amount == 500);
    CHECK(stored != NULL && stored->last_fill_amount == 300);

    ufx::OrderView stale = MakeOrder(101, "g", 300);
    stale.last_fill_amount = 100;
    CHECK(!book.UpsertFromPush(stale, &stored));
    CHECK(stored != NULL && stored->deal_amount == 500);
    CHECK(stored != NULL && stored->last_fill_amount == 300);
}

static void TestSnapshotReplayDoesNotRegress() {
    ufx::OrderBook book;
    ufx::OrderView snapshot = MakeOrder(101, "", 500);
    snapshot.msgtype.clear();
    snapshot.last_fill_amount = 0;
    snapshot.deal_no.clear();
    snapshot.realdeal_serial_no = 0;
    snapshot.entrust_state = "6";
    std::vector<ufx::OrderView> rows(1, snapshot);
    const ufx::BookChanges<ufx::OrderView> first =
        book.ReplaceAll(MakeOrderSnapshot(rows));
    CHECK(first.updated.size() == 1);
    CHECK(first.removed.empty());

    const ufx::OrderView* stored = NULL;
    ufx::OrderView stale_submit = MakeOrder(101, "a", 0);
    stale_submit.entrust_state = "2";
    CHECK(!book.UpsertFromPush(stale_submit, &stored));
    CHECK(stored != NULL && stored->entrust_state == "6");
    CHECK(stored != NULL && stored->deal_amount == 500);

    ufx::OrderView same_fill = MakeOrder(101, "g", 500);
    same_fill.last_fill_amount = 300;
    CHECK(book.UpsertFromPush(same_fill, &stored));
    CHECK(stored != NULL && stored->deal_amount == 500);
    CHECK(stored != NULL && stored->last_fill_amount == 300);
    CHECK(!book.UpsertFromPush(same_fill, &stored));

    snapshot.entrust_state = "8";
    snapshot.cancel_amount = 500;
    book.ReplaceAll(
        MakeOrderSnapshot(std::vector<ufx::OrderView>(1, snapshot)));
    CHECK(!book.UpsertFromPush(same_fill, &stored));
    CHECK(stored != NULL && stored->entrust_state == "8");
    CHECK(stored != NULL && stored->cancel_amount == 500);
}

static void TestOrderSnapshotChanges() {
    ufx::OrderBook book;
    std::vector<ufx::OrderView> initial;
    initial.push_back(MakeOrder(1, "", 0));
    initial.push_back(MakeOrder(2, "", 0));
    book.ReplaceAll(MakeOrderSnapshot(initial));

    std::vector<ufx::OrderView> next;
    ufx::OrderView changed = MakeOrder(2, "", 100);
    next.push_back(changed);
    next.push_back(MakeOrder(3, "", 0));
    const ufx::BookChanges<ufx::OrderView> changes =
        book.ReplaceAll(MakeOrderSnapshot(next));
    CHECK(changes.removed.size() == 1);
    CHECK(changes.removed[0].entrust_no == 1);
    CHECK(changes.updated.size() == 2);

    const ufx::BookChanges<ufx::OrderView> unchanged =
        book.ReplaceAll(MakeOrderSnapshot(next));
    CHECK(unchanged.removed.empty());
    CHECK(unchanged.updated.empty());
}

static void TestOrderSnapshotOrderedMerge() {
    ufx::OrderBook book;
    std::vector<ufx::OrderView> initial;
    initial.push_back(MakeOrder(1, "", 0));
    initial.push_back(MakeOrder(3, "", 0));
    initial.push_back(MakeOrder(5, "", 0));
    book.ReplaceAll(MakeOrderSnapshot(initial));

    std::vector<ufx::OrderView> next;
    next.push_back(MakeOrder(2, "", 0));
    next.push_back(MakeOrder(3, "", 0));
    next.push_back(MakeOrder(5, "", 200));
    next.push_back(MakeOrder(6, "", 0));
    const ufx::BookChanges<ufx::OrderView> changes =
        book.ReplaceAll(MakeOrderSnapshot(next));
    CHECK(changes.removed.size() == 1);
    CHECK(changes.removed[0].entrust_no == 1);
    CHECK(changes.updated.size() == 3);
    CHECK(changes.updated[0].entrust_no == 2);
    CHECK(changes.updated[1].entrust_no == 5);
    CHECK(changes.updated[2].entrust_no == 6);
}

static void TestLargeUnchangedOrderSnapshot() {
    std::vector<ufx::OrderView> rows;
    rows.reserve(10000);
    for (int64_t no = 1; no <= 10000; ++no) {
        rows.push_back(MakeOrder(no, "", no % 7));
    }
    ufx::OrderBook book;
    CHECK(book.ReplaceAll(MakeOrderSnapshot(rows)).updated.size() == rows.size());
    const ufx::BookChanges<ufx::OrderView> unchanged =
        book.ReplaceAll(MakeOrderSnapshot(rows));
    CHECK(unchanged.updated.empty());
    CHECK(unchanged.removed.empty());
}

static ufx::PositionView MakePosition(const char* account, const char* asset,
                                      const char* combi, const char* stock) {
    ufx::PositionView position;
    position.account_code = account;
    position.asset_no = asset;
    position.combi_no = combi;
    position.market_no = "1";
    position.stock_code = stock;
    position.invest_type = "1";
    position.stockholder_id = "H";
    position.hold_seat = "S";
    position.current_amount = 1000;
    position.enable_amount = 200;
    return position;
}

static void TestPositionScopeReplacement() {
    ufx::PositionBook book;
    const ufx::PositionView a = book.Upsert(MakePosition("A1", "AS1", "C1", "600570"));
    const ufx::PositionView b = book.Upsert(MakePosition("A1", "AS2", "C2", "000001"));
    CHECK(a.unsellable_amount == 800);
    CHECK(b.unsellable_amount == 800);

    ufx::QueryScope scope;
    scope.account_code = "A1";
    scope.asset_no = "AS1";
    std::vector<ufx::PositionView> fresh;
    ufx::PositionView replacement = MakePosition("A1", "AS1", "C1", "600036");
    replacement.current_amount = 10;
    replacement.enable_amount = 10;
    fresh.push_back(replacement);

    const ufx::BookChanges<ufx::PositionView> changes =
        book.ReplaceScope(scope, MakePositionSnapshot(fresh));
    CHECK(changes.removed.size() == 1);
    CHECK(changes.updated.size() == 1);
    CHECK(book.All().size() == 2);
    CHECK(book.Find(ufx::PositionKey(replacement)) != NULL);
    CHECK(book.Find(ufx::PositionKey(b)) != NULL);
    CHECK(changes.updated[0].unsellable_amount == 0);
}

static ufx::AccountView MakeAccount(const char* account, const char* asset,
                                    int64_t balance_cents) {
    ufx::AccountView value;
    value.account_code = account;
    value.asset_no = asset;
    value.enable_balance_t0 = ufx::Money::FromScaled(balance_cents);
    value.enable_balance_t1 = ufx::Money::FromScaled(balance_cents);
    value.current_balance = ufx::Money::FromScaled(balance_cents);
    return value;
}

static void TestAccountScopeReplacement() {
    ufx::AccountBook book;
    std::vector<ufx::AccountView> first;
    first.push_back(MakeAccount("A1", "AS1", 100));
    first.push_back(MakeAccount("A1", "AS2", 200));
    CHECK(book.ReplaceScope("scope", MakeAccountSnapshot(first)).updated.size() == 2);

    std::vector<ufx::AccountView> second;
    second.push_back(MakeAccount("A1", "AS2", 300));
    const ufx::BookChanges<ufx::AccountView> changes =
        book.ReplaceScope("scope", MakeAccountSnapshot(second));
    CHECK(changes.removed.size() == 1);
    CHECK(changes.removed[0].asset_no == "AS1");
    CHECK(changes.updated.size() == 1);
    CHECK(changes.updated[0].current_balance.ToString() == "3.00");
}

static void TestAccountSharedScopeOwnership() {
    ufx::AccountBook book;
    std::vector<ufx::AccountView> shared;
    shared.push_back(MakeAccount("A1", "AS1", 100));
    book.ReplaceScope("scope-1", MakeAccountSnapshot(shared));
    book.ReplaceScope("scope-2", MakeAccountSnapshot(shared));

    const ufx::AccountSnapshot empty;
    CHECK(book.ReplaceScope("scope-1", empty).removed.empty());
    CHECK(book.All().size() == 1);
    const ufx::BookChanges<ufx::AccountView> removed =
        book.ReplaceScope("scope-2", empty);
    CHECK(removed.removed.size() == 1);
    CHECK(book.All().empty());
}

static void TestMessageClassification() {
    CHECK(ufx::IsOrderMessage("a"));
    CHECK(ufx::IsOrderMessage("g"));
    CHECK(!ufx::IsOrderMessage("P"));
    CHECK(!ufx::IsOrderMessage("a0"));
    CHECK(ufx::IsCancellationMessage("d"));
    CHECK(ufx::IsCancellationMessage("e"));
    CHECK(ufx::IsCancellationMessage("f"));
    CHECK(!ufx::IsCancellationMessage("g"));
    CHECK(ufx::ShouldRefreshBooks("a"));
    CHECK(ufx::ShouldRefreshBooks("c"));
    CHECK(ufx::ShouldRefreshBooks("e"));
    CHECK(ufx::ShouldRefreshBooks("g"));
    CHECK(ufx::ShouldRefreshBooks("P"));
    CHECK(!ufx::ShouldRefreshBooks("b"));
    CHECK(!ufx::ShouldRefreshBooks("d"));
    CHECK(!ufx::ShouldRefreshBooks("f"));
}

static void TestCoalescer() {
    ufx::RefreshCoalescer coalescer(100);
    coalescer.Request("scope", 0);
    coalescer.Request("scope", 50);
    CHECK(coalescer.Due(50).empty());
    std::vector<std::string> due = coalescer.Due(100);
    CHECK(due.size() == 1);
    CHECK(due[0] == "scope");

    coalescer.MarkInFlight("scope", 2);
    coalescer.Request("scope", 110);
    CHECK(coalescer.HasInFlight("scope"));
    CHECK(coalescer.Due(200).empty());

    coalescer.MarkDone("scope", 200);
    CHECK(coalescer.HasInFlight("scope"));
    CHECK(coalescer.Due(300).empty());
    coalescer.MarkDone("scope", 200);
    CHECK(!coalescer.HasInFlight("scope"));
    CHECK(coalescer.Due(200).empty());
    due = coalescer.Due(300);
    CHECK(due.size() == 1);

    ufx::RefreshCoalescer immediate(100);
    immediate.RequestNow("orders", 500);
    CHECK(immediate.HasPending("orders"));
    int64_t due_ms = 0;
    CHECK(immediate.NextDueMs(&due_ms));
    CHECK(due_ms == 500);
    due = immediate.Due(500);
    CHECK(due.size() == 1 && due[0] == "orders");
    immediate.MarkInFlight("orders", 1);
    immediate.RequestNow("orders", 501);
    immediate.MarkDone("orders", 510);
    CHECK(immediate.Due(609).empty());
    CHECK(immediate.NextDueMs(&due_ms));
    CHECK(due_ms == 610);
    CHECK(immediate.Due(610).size() == 1);
}

int main() {
    TestFixedDecimal();
    TestSessionLimitDefaults();
    TestListenerExceptionGuard();
    TestCumulativeDealIsIdempotent();
    TestSnapshotReplayDoesNotRegress();
    TestOrderSnapshotChanges();
    TestOrderSnapshotOrderedMerge();
    TestLargeUnchangedOrderSnapshot();
    TestPositionScopeReplacement();
    TestAccountScopeReplacement();
    TestAccountSharedScopeOwnership();
    TestMessageClassification();
    TestCoalescer();
    if (g_failed != 0) {
        std::fprintf(stderr, "%d checks failed\n", g_failed);
        return 1;
    }
    std::printf("all exact core checks passed\n");
    return 0;
}
