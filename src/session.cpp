#include "ufx/session.h"

#include "listener_guard.h"
#include "t2_util.h"
#include "ufx/books.h"
#include "ufx/coalescer.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace ufx {

namespace {

char* Mutable(const std::string& value) {
    return const_cast<char*>(value.c_str());
}

int64_t NowMs() {
    return static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

struct OperationStatus {
    int code;
    std::string message;

    OperationStatus() : code(0) {}
    OperationStatus(int value, const std::string& text) : code(value), message(text) {}
    bool Ok() const { return code == 0; }
};

enum EventKind {
    kEventPush,
    kEventQuery,
    kEventInitialReconcile
};

struct QueuedEvent {
    EventKind kind;
    int func_no;
    int hsend;
    int return_code;
    int error_no;
    int64_t received_ms;
    int64_t enqueued_ms;
    std::string error_info;
    std::vector<char> payload;

    QueuedEvent()
        : kind(kEventPush),
          func_no(0),
          hsend(0),
          return_code(0),
          error_no(0),
          received_ms(0),
          enqueued_ms(0) {}
};

struct QueryChain {
    uint64_t id;
    std::string refresh_key;
    int func_no;
    QueryScope scope;
    std::string position_str;
    std::set<std::string> seen_cursors;
    OrderSnapshot orders;
    PositionSnapshot positions;
    AccountSnapshot accounts;
    std::vector<int64_t> order_row_order;
    std::vector<std::string> position_row_order;
    std::vector<std::string> account_row_order;
    int hsend;
    int64_t started_ms;
    int64_t sent_ms;
    size_t page_count;
    size_t row_count;
    size_t payload_bytes;

    QueryChain()
        : id(0),
          func_no(0),
          hsend(0),
          started_ms(0),
          sent_ms(0),
          page_count(0),
          row_count(0),
          payload_bytes(0) {}
};

bool IsPaginatedFunction(int func_no) {
    return func_no == 31001 || func_no == 32001;
}

const char kOrderSnapshotKey[] = "order_snapshot";
const int kListenerCallbackError = -1009;
const int kDispatcherExceptionError = -1010;

std::string FunctionName(int func_no) {
    std::ostringstream out;
    out << func_no;
    return out.str();
}

template <typename T>
void UpdateAtomicMax(std::atomic<T>* target, T value) {
    T observed = target->load();
    while (observed < value &&
           !target->compare_exchange_weak(observed, value)) {
    }
}

template <size_t N>
struct ColumnLayout {
    int index[N];
};

template <size_t N>
bool ResolveColumns(IF2UnPacker* unpack, const char* const (&names)[N],
                    ColumnLayout<N>* layout, const char* function_name,
                    std::string* error) {
    for (size_t i = 0; i < N; ++i) {
        layout->index[i] = t2::FieldIndex(unpack, names[i]);
        if (layout->index[i] < 0) {
            *error = std::string(function_name) + " response is missing field " + names[i];
            return false;
        }
    }
    return true;
}

enum PositionColumn {
    kPositionAccount,
    kPositionAsset,
    kPositionCombi,
    kPositionMarket,
    kPositionStock,
    kPositionInvestType,
    kPositionStockholder,
    kPositionHoldSeat,
    kPositionCurrentAmount,
    kPositionEnableAmount,
    kPositionPreBuyAmount,
    kPositionPreSellAmount,
    kPositionTodayBuyAmount,
    kPositionTodaySellAmount,
    kPositionColumnCount
};

const char* const kPositionColumnNames[kPositionColumnCount] = {
    "account_code",      "asset_no",        "combi_no",
    "market_no",         "stock_code",      "invest_type",
    "stockholder_id",    "hold_seat",       "current_amount",
    "enable_amount",     "pre_buy_amount",  "pre_sell_amount",
    "today_buy_amount",  "today_sell_amount"};

enum OrderColumn {
    kOrderDate,
    kOrderEntrustNo,
    kOrderState,
    kOrderAccount,
    kOrderAsset,
    kOrderCombi,
    kOrderMarket,
    kOrderStock,
    kOrderDirection,
    kOrderPrice,
    kOrderAmount,
    kOrderDealAmount,
    kOrderWithdrawAmount,
    kOrderExtsystemId,
    kOrderColumnCount
};

const char* const kOrderColumnNames[kOrderColumnCount] = {
    "entrust_date",      "entrust_no",       "entrust_state",
    "account_code",      "asset_no",         "combi_no",
    "market_no",         "stock_code",       "entrust_direction",
    "entrust_price",     "entrust_amount",   "deal_amount",
    "withdraw_amount",   "extsystem_id"};

enum AccountColumn {
    kAccountCode,
    kAccountAsset,
    kAccountEnableT0,
    kAccountEnableT1,
    kAccountCurrent,
    kAccountColumnCount
};

const char* const kAccountColumnNames[kAccountColumnCount] = {
    "account_code", "asset_no", "enable_balance_t0", "enable_balance_t1",
    "current_balance"};

}  // namespace

class Session::Impl {
public:
    explicit Impl(const SessionConfig& config)
        : config_(config),
          session_conn_(NULL),
          query_conn_(NULL),
          heartbeat_conn_(NULL),
          mc_conn_(NULL),
          subscriber_(NULL),
          session_cb_(this, "session", false),
          query_cb_(this, "query", true),
          heartbeat_cb_(this, "heartbeat", false),
          mc_conn_cb_(this, "mc", false),
          mc_cb_(this),
          coalescer_(config.coalesce_window_ms),
          order_coalescer_(config.coalesce_window_ms),
          running_(false),
          accepting_events_(false),
          listener_epoch_(0),
          listener_reconcile_requested_(false),
          lifecycle_state_(kStopped),
          active_workers_(0),
          subscribe_index_(-1),
          next_query_id_(1),
          order_query_id_(0),
          order_snapshot_barrier_(false),
          order_listener_epoch_(0),
          position_listener_epoch_(0),
          account_listener_epoch_(0),
          last_reconcile_ms_(0),
          queued_payload_bytes_(0),
          reserved_event_count_(0),
          reserved_payload_bytes_(0),
          queue_generation_(1),
          peak_queued_events_(0),
          peak_queued_payload_bytes_(0),
          buffered_order_push_count_(0),
          peak_buffered_order_pushes_(0),
          fatal_pending_(false),
          fatal_code_(0),
          stat_enqueued_events_(0),
          stat_enqueue_waits_(0),
          stat_rejected_events_(0),
          stat_query_chains_started_(0),
          stat_query_chains_completed_(0),
          stat_query_chains_failed_(0),
          stat_query_pages_(0),
          stat_query_rows_(0),
          stat_query_payload_bytes_(0),
          stat_order_snapshot_requests_(0),
          stat_order_snapshot_coalesced_(0),
          stat_order_snapshots_started_(0),
          stat_max_event_queue_delay_ms_(0),
          stat_max_listener_callback_ms_(0) {}

    ~Impl() { Stop(); }

    void SetListener(const std::shared_ptr<IMarketListener>& listener) {
        {
            std::lock_guard<std::mutex> lock(data_mu_);
            listener_ = listener;
            listener_epoch_.fetch_add(1);
        }
        if (listener && running_.load()) {
            listener_reconcile_requested_.store(true);
            queue_cv_.notify_one();
        }
    }

    std::string UserToken() const {
        std::lock_guard<std::mutex> lock(data_mu_);
        return token_;
    }

    SessionStats Stats() const {
        SessionStats stats;
        {
            std::lock_guard<std::mutex> lock(queue_mu_);
            stats.queued_events = queue_.size() + reserved_event_count_;
            stats.queued_payload_bytes =
                queued_payload_bytes_ + reserved_payload_bytes_;
            stats.peak_queued_events = peak_queued_events_;
            stats.peak_queued_payload_bytes = peak_queued_payload_bytes_;
        }
        stats.buffered_order_pushes = buffered_order_push_count_.load();
        stats.peak_buffered_order_pushes = peak_buffered_order_pushes_.load();
        stats.enqueued_events = stat_enqueued_events_.load();
        stats.enqueue_waits = stat_enqueue_waits_.load();
        stats.rejected_events = stat_rejected_events_.load();
        stats.query_chains_started = stat_query_chains_started_.load();
        stats.query_chains_completed = stat_query_chains_completed_.load();
        stats.query_chains_failed = stat_query_chains_failed_.load();
        stats.query_pages = stat_query_pages_.load();
        stats.query_rows = stat_query_rows_.load();
        stats.query_payload_bytes = stat_query_payload_bytes_.load();
        stats.order_snapshot_requests = stat_order_snapshot_requests_.load();
        stats.order_snapshot_coalesced = stat_order_snapshot_coalesced_.load();
        stats.order_snapshots_started = stat_order_snapshots_started_.load();
        stats.max_event_queue_delay_ms = stat_max_event_queue_delay_ms_.load();
        stats.max_listener_callback_ms = stat_max_listener_callback_ms_.load();
        return stats;
    }

    int Start() {
        OperationStatus status;
        bool join_failed_start = false;
        {
            std::lock_guard<std::mutex> operation_lock(operation_mu_);
            {
                std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mu_);
                if (lifecycle_state_ != kStopped) {
                    status =
                        OperationStatus(-1002, "session is already started or stopping");
                }
            }
            if (status.Ok()) {
                std::lock_guard<std::mutex> join_lock(join_mu_);
                JoinWorkers();
            }

            std::unique_lock<std::mutex> lifecycle_lock(lifecycle_mu_);
            if (status.Ok()) {
                lifecycle_state_ = kStarting;
                join_failed_start = true;
                status = ValidateConfig();
                if (status.Ok()) {
                    status = ConnectPair(&session_conn_, config_.t2sdk_ini, &session_cb_);
                }
                if (status.Ok()) {
                    status = ConnectPair(&query_conn_, config_.t2sdk_ini, &query_cb_);
                }
                if (status.Ok()) {
                    status =
                        ConnectPair(&heartbeat_conn_, config_.t2sdk_ini, &heartbeat_cb_);
                }
                if (status.Ok()) {
                    status = ConnectPair(&mc_conn_, config_.subscriber_ini, &mc_conn_cb_);
                }
                if (status.Ok()) {
                    status = Login();
                }
                if (status.Ok()) {
                    accepting_events_.store(true);
                    status = Subscribe();
                }

                if (status.Ok()) {
                    order_snapshot_barrier_ = true;
                    listener_epoch_.fetch_add(1);
                    running_.store(true);
                    last_reconcile_ms_ = NowMs();
                    lifecycle_state_ = kRunning;
                    active_workers_ = 2;
                    try {
                        dispatcher_ = std::thread(&Impl::DispatchLoop, this);
                        heartbeat_ = std::thread(&Impl::HeartbeatLoop, this);
                    } catch (...) {
                        running_.store(false);
                        accepting_events_.store(false);
                        lifecycle_state_ = kStopping;
                        if (!dispatcher_.joinable()) {
                            active_workers_ = 0;
                            cleanup_status_ = CleanupResourcesLocked();
                            lifecycle_state_ = kStopped;
                        } else {
                            active_workers_ = 1;
                        }
                        status = OperationStatus(-1003, "failed to start worker threads");
                    }
                } else {
                    accepting_events_.store(false);
                    running_.store(false);
                    cleanup_status_ = CleanupResourcesLocked();
                    lifecycle_state_ = kStopped;
                }
            }
            if (status.Ok()) {
                lifecycle_lock.unlock();
                QueuedEvent initial;
                initial.kind = kEventInitialReconcile;
                if (!Enqueue(std::move(initial))) {
                    status = OperationStatus(-1004, "session stopped during startup");
                }
            }
        }

        if (!status.Ok()) {
            queue_cv_.notify_all();
            queue_space_cv_.notify_all();
            stop_cv_.notify_all();
            if (join_failed_start) {
                std::lock_guard<std::mutex> join_lock(join_mu_);
                JoinWorkers();
            }
            NotifySession(status.code, status.message.c_str());
            return status.code;
        }

        const detail::ListenerCallResult start_notification =
            NotifySession(0, "session started");
        if (start_notification != detail::kListenerCallSucceeded) {
            Stop();
            return start_notification == detail::kListenerCallThrewBadAlloc
                       ? -1006
                       : kListenerCallbackError;
        }
        return 0;
    }

    void Stop() {
        OperationStatus cleanup_status;
        bool called_from_worker = false;
        {
            std::lock_guard<std::mutex> operation_lock(operation_mu_);
            {
                std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mu_);
                if (lifecycle_state_ == kRunning) {
                    lifecycle_state_ = kStopping;
                    running_.store(false);
                    accepting_events_.store(false);
                } else if (lifecycle_state_ == kStarting) {
                    lifecycle_state_ = kStopping;
                    running_.store(false);
                    accepting_events_.store(false);
                }
                called_from_worker = IsWorkerThread();
                if (active_workers_ == 0 && lifecycle_state_ == kStopping) {
                    cleanup_status_ = CleanupResourcesLocked();
                    lifecycle_state_ = kStopped;
                }
            }

        }
        queue_cv_.notify_all();
        queue_space_cv_.notify_all();
        stop_cv_.notify_all();
        if (called_from_worker) {
            return;
        }
        {
            std::lock_guard<std::mutex> join_lock(join_mu_);
            JoinWorkers();
        }
        {
            std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mu_);
            cleanup_status = cleanup_status_;
            cleanup_status_ = OperationStatus();
        }
        if (!cleanup_status.Ok()) {
            NotifySession(cleanup_status.code, cleanup_status.message.c_str());
        }
    }

    void OnQueryMsg(int hsend, IBizMessage* msg) {
        if (msg == NULL || !accepting_events_.load()) {
            return;
        }
        const int64_t received_ms = NowMs();
        int len = 0;
        const void* content = msg->GetContent(len);
        const size_t payload_bytes =
            content != NULL && len > 0 ? static_cast<size_t>(len) : 0U;
        uint64_t reservation_generation = 0;
        if (!ReserveEvent(payload_bytes, hsend, received_ms,
                          &reservation_generation)) {
            return;
        }
        QueuedEvent event;
        try {
            event.kind = kEventQuery;
            event.func_no = msg->GetFunction();
            event.hsend = hsend;
            event.return_code = msg->GetReturnCode();
            event.error_no = msg->GetErrorNo();
            event.received_ms = received_ms;
            event.error_info = t2::CStr(msg->GetErrorInfo());
            event.payload = t2::CopyBuffer(content, len);
        } catch (const std::bad_alloc&) {
            if (CancelReservation(payload_bytes, hsend, reservation_generation)) {
                stat_rejected_events_.fetch_add(1);
                SignalFatal(-1006, "failed to allocate query event payload");
            }
            return;
        }
        CommitReserved(std::move(event), payload_bytes, reservation_generation);
    }

    void OnPush(const void* data, int len) {
        if (!accepting_events_.load()) {
            return;
        }
        const size_t payload_bytes =
            data != NULL && len > 0 ? static_cast<size_t>(len) : 0U;
        uint64_t reservation_generation = 0;
        if (!ReserveEvent(payload_bytes, 0, 0, &reservation_generation)) {
            return;
        }
        QueuedEvent event;
        try {
            event.kind = kEventPush;
            event.payload = t2::CopyBuffer(data, len);
        } catch (const std::bad_alloc&) {
            if (CancelReservation(payload_bytes, 0, reservation_generation)) {
                stat_rejected_events_.fetch_add(1);
                SignalFatal(-1006, "failed to allocate push event payload");
            }
            return;
        }
        CommitReserved(std::move(event), payload_bytes, reservation_generation);
    }

    void OnLinkClosed(const std::string& which) {
        if (!running_.load()) {
            return;
        }
        SignalFatal(-1, which + " connection closed");
    }

    void OnSubscriptionFailure(int subscribe_index, const char* message) {
        std::ostringstream out;
        out << "subscription " << subscribe_index << " terminated";
        if (message != NULL && *message != '\0') {
            out << ": " << message;
        }
        SignalFatal(-2, out.str());
    }

private:
    enum LifecycleState {
        kStopped,
        kStarting,
        kRunning,
        kStopping
    };

    class ConnectionCallback : public CCallbackInterface {
    public:
        ConnectionCallback(Impl* owner, const char* name, bool receives_queries)
            : owner_(owner), name_(name), receives_queries_(receives_queries) {}
        unsigned long FUNCTION_CALL_MODE QueryInterface(const char*, IKnown**) { return 0; }
        unsigned long FUNCTION_CALL_MODE AddRef() { return 1; }
        unsigned long FUNCTION_CALL_MODE Release() { return 1; }
        void FUNCTION_CALL_MODE OnConnect(CConnectionInterface*) {}
        void FUNCTION_CALL_MODE OnSafeConnect(CConnectionInterface*) {}
        void FUNCTION_CALL_MODE OnRegister(CConnectionInterface*) {}
        void FUNCTION_CALL_MODE OnClose(CConnectionInterface*) { owner_->OnLinkClosed(name_); }
        void FUNCTION_CALL_MODE OnSent(CConnectionInterface*, int, void*, void*, int) {}
        void FUNCTION_CALL_MODE Reserved1(void*, void*, void*, void*) {}
        void FUNCTION_CALL_MODE Reserved2(void*, void*, void*, void*) {}
        int FUNCTION_CALL_MODE Reserved3() { return 0; }
        void FUNCTION_CALL_MODE Reserved4() {}
        void FUNCTION_CALL_MODE Reserved5() {}
        void FUNCTION_CALL_MODE Reserved6() {}
        void FUNCTION_CALL_MODE Reserved7() {}
        void FUNCTION_CALL_MODE OnReceivedBiz(CConnectionInterface*, int, const void*, int) {}
        void FUNCTION_CALL_MODE OnReceivedBizEx(CConnectionInterface*, int, LPRET_DATA,
                                                const void*, int) {}
        void FUNCTION_CALL_MODE OnReceivedBizMsg(CConnectionInterface*, int hsend,
                                                 IBizMessage* msg) {
            if (receives_queries_) {
                owner_->OnQueryMsg(hsend, msg);
            }
        }

    private:
        Impl* owner_;
        std::string name_;
        bool receives_queries_;
    };

    class McCallback : public CSubCallbackInterface {
    public:
        explicit McCallback(Impl* owner) : owner_(owner) {}
        unsigned long FUNCTION_CALL_MODE QueryInterface(const char*, IKnown**) { return 0; }
        unsigned long FUNCTION_CALL_MODE AddRef() { return 1; }
        unsigned long FUNCTION_CALL_MODE Release() { return 1; }
        void FUNCTION_CALL_MODE OnReceived(CSubscribeInterface*, int, const void* data,
                                           int len, LPSUBSCRIBE_RECVDATA) {
            owner_->OnPush(data, len);
        }
        void FUNCTION_CALL_MODE OnRecvTickMsg(CSubscribeInterface*, int index,
                                              const char* message) {
            owner_->OnSubscriptionFailure(index, message);
        }

    private:
        Impl* owner_;
    };

    OperationStatus ValidateConfig() const {
        if (config_.t2sdk_ini.empty() || config_.subscriber_ini.empty()) {
            return OperationStatus(-1001, "t2sdk_ini and subscriber_ini are required");
        }
        if (config_.operator_no.empty()) {
            return OperationStatus(-1001, "operator_no is required");
        }
        if (config_.asset_no.empty() == config_.combi_no.empty()) {
            return OperationStatus(-1001,
                                   "exactly one of asset_no and combi_no is required");
        }
        if (config_.heartbeat_interval_ms <= 0 || config_.reconcile_interval_ms <= 0 ||
            config_.connect_timeout_ms <= 0 || config_.query_timeout_ms <= 0 ||
            config_.enqueue_timeout_ms <= 0 ||
            config_.query_chain_timeout_ms < config_.query_timeout_ms ||
            config_.coalesce_window_ms < 0) {
            return OperationStatus(-1001, "session timeouts and intervals are invalid");
        }
        if (config_.query_page_size <= 0 || config_.query_page_size > 10000) {
            return OperationStatus(-1001, "query_page_size must be between 1 and 10000");
        }
        if (config_.max_event_queue_size == 0 || config_.max_event_queue_bytes == 0 ||
            config_.max_dispatch_batch_size == 0 ||
            config_.max_dispatch_batch_size > config_.max_event_queue_size) {
            return OperationStatus(-1001, "event queue limits must be positive");
        }
        if (config_.max_query_pages == 0 ||
            config_.max_query_rows < static_cast<size_t>(config_.query_page_size) ||
            config_.max_query_bytes == 0 || config_.max_buffered_order_pushes == 0) {
            return OperationStatus(-1001, "query and replay limits are invalid");
        }
        return OperationStatus();
    }

    OperationStatus ConnectPair(CConnectionInterface** connection,
                                const std::string& ini,
                                CCallbackInterface* callback) {
        CConfigInterface* config = NewConfig();
        if (config == NULL) {
            return OperationStatus(-1, "NewConfig failed");
        }
        config->AddRef();
        const int load_rc = config->Load(ini.c_str());
        if (load_rc != 0) {
            config->Release();
            return OperationStatus(load_rc, "load T2SDK config failed: " + ini);
        }
        *connection = NewConnection(config);
        config->Release();
        if (*connection == NULL) {
            return OperationStatus(-1, "NewConnection failed: " + ini);
        }
        (*connection)->AddRef();
        int rc = (*connection)->Create2BizMsg(callback);
        if (rc != 0) {
            return OperationStatus(rc, t2::CStr((*connection)->GetErrorMsg(rc)));
        }
        rc = (*connection)->Connect(static_cast<unsigned int>(config_.connect_timeout_ms));
        if (rc != 0) {
            return OperationStatus(rc, t2::CStr((*connection)->GetErrorMsg(rc)));
        }
        return OperationStatus();
    }

    static void ReleaseConn(CConnectionInterface** connection) {
        if (*connection == NULL) {
            return;
        }
        (*connection)->Close();
        (*connection)->Release();
        *connection = NULL;
    }

    OperationStatus Login() {
        IF2Packer* pack = NewPacker(2);
        if (pack == NULL) {
            return OperationStatus(-1, "NewPacker failed for login");
        }
        pack->AddRef();
        pack->BeginPack();
        pack->AddField("operator_no", 'S', 16, 0);
        pack->AddField("password", 'S', 32, 0);
        pack->AddField("mac_address", 'S', 255, 0);
        pack->AddField("op_station", 'S', 255, 0);
        pack->AddField("ip_address", 'S', 36, 0);
        pack->AddField("authorization_id", 'S', 64, 0);
        pack->AddStr(config_.operator_no.c_str());
        pack->AddStr(config_.password.c_str());
        pack->AddStr(t2::CStr(session_conn_->GetSelfMac()).c_str());
        pack->AddStr("ufx_client");
        std::string ip = t2::CStr(session_conn_->GetSelfAddress());
        const std::string::size_type colon = ip.find(':');
        if (colon != std::string::npos) {
            ip = ip.substr(0, colon);
        }
        pack->AddStr(ip.c_str());
        pack->AddStr(config_.authorization_id.c_str());
        pack->EndPack();

        IBizMessage* message = NewBizMessage();
        if (message == NULL) {
            t2::ReleasePacker(pack);
            return OperationStatus(-1, "NewBizMessage failed for login");
        }
        message->AddRef();
        message->SetFunction(10001);
        message->SetPacketType(REQUEST_PACKET);
        message->SetContent(pack->GetPackBuf(), pack->GetPackLen());
        const int hsend = session_conn_->SendBizMsg(message, 0);
        t2::ReleasePacker(pack);
        message->Release();
        if (hsend <= 0) {
            return OperationStatus(hsend, t2::CStr(session_conn_->GetErrorMsg(hsend)));
        }

        IBizMessage* response = NULL;
        const int receive_rc = session_conn_->RecvBizMsg(
            hsend, &response, static_cast<unsigned int>(config_.query_timeout_ms), 0);
        if (receive_rc != 0 || response == NULL) {
            return OperationStatus(receive_rc != 0 ? receive_rc : -1,
                                   t2::CStr(session_conn_->GetErrorMsg(receive_rc)));
        }
        if (response->GetReturnCode() != 0) {
            const int code = response->GetErrorNo() != 0 ? response->GetErrorNo()
                                                         : response->GetReturnCode();
            return OperationStatus(code, t2::CStr(response->GetErrorInfo()));
        }

        int len = 0;
        const void* content = response->GetContent(len);
        if (content == NULL || len <= 0) {
            return OperationStatus(-1, "login response has no content");
        }
        IF2UnPacker* unpack = NewUnPacker(const_cast<void*>(content),
                                          static_cast<unsigned int>(len));
        if (unpack == NULL) {
            return OperationStatus(-1, "login unpack failed");
        }
        unpack->AddRef();
        const t2::ErrorHead head = t2::ReadHead(unpack);
        if (!head.valid || head.error_code != 0 || unpack->GetDatasetCount() < 2) {
            const int code = head.error_code != 0 ? head.error_code : -1;
            const std::string text = head.error_msg.empty() ? "invalid login response"
                                                             : head.error_msg;
            unpack->Release();
            return OperationStatus(code, text);
        }
        unpack->SetCurrentDatasetByIndex(1);
        if (unpack->GetRowCount() != 1U) {
            unpack->Release();
            return OperationStatus(-1, "login response must contain exactly one row");
        }
        unpack->First();
        std::string token;
        const bool token_ok = t2::FieldStr(unpack, "user_token", &token);
        unpack->Release();
        if (!token_ok || token.empty()) {
            return OperationStatus(-1, "login response contains no user_token");
        }
        {
            std::lock_guard<std::mutex> lock(data_mu_);
            token_ = token;
        }
        return OperationStatus();
    }

    OperationStatus Subscribe() {
        CConfigInterface* config = NewConfig();
        if (config == NULL) {
            return OperationStatus(-1, "NewConfig failed for subscriber");
        }
        config->AddRef();
        const int load_rc = config->Load(config_.subscriber_ini.c_str());
        if (load_rc != 0) {
            config->Release();
            return OperationStatus(load_rc, "load subscriber config failed");
        }
        const std::string biz_name =
            t2::CStr(config->GetString("subcribe", "biz_name", "ufx_client"));
        const std::string topic =
            t2::CStr(config->GetString("subcribe", "topic_name", "ufx_topic"));
        config->Release();

        subscriber_ = mc_conn_->NewSubscriber(&mc_cb_, Mutable(biz_name), 5000);
        if (subscriber_ == NULL) {
            return OperationStatus(-1, t2::CStr(mc_conn_->GetMCLastError()));
        }
        subscriber_->AddRef();

        CSubscribeParamInterface* param = NewSubscribeParam();
        IF2Packer* check = NewPacker(2);
        if (param == NULL || check == NULL) {
            if (param != NULL) {
                param->AddRef();
                param->Release();
            }
            if (check != NULL) {
                check->AddRef();
                t2::ReleasePacker(check);
            }
            return OperationStatus(-1, "failed to allocate subscription parameters");
        }
        param->AddRef();
        param->SetTopicName(Mutable(topic));
        param->SetFromNow(true);
        param->SetReplace(false);
        param->SetFilter(const_cast<char*>("operator_no"), Mutable(config_.operator_no));

        check->AddRef();
        check->BeginPack();
        check->AddField("login_operator_no", 'S', 32, 0);
        check->AddField("password", 'S', 16, 0);
        check->AddField("mac_address", 'S', 32, 0);
        check->AddField("hd_volserial", 'S', 32, 0);
        check->AddField("ip_address", 'S', 32, 0);
        check->AddStr(config_.operator_no.c_str());
        check->AddStr(config_.password.c_str());
        check->AddStr(t2::CStr(mc_conn_->GetSelfMac()).c_str());
        check->AddStr("vol");
        check->AddStr("0.0.0.0");
        check->EndPack();

        IF2UnPacker* response = NULL;
        subscribe_index_ = subscriber_->SubscribeTopic(param, 5000, &response, check);
        t2::ReleasePacker(check);
        param->Release();

        OperationStatus status;
        if (subscribe_index_ <= 0) {
            if (response != NULL) {
                const t2::ErrorHead head = t2::ReadHead(response);
                status = OperationStatus(head.error_code != 0 ? head.error_code
                                                              : subscribe_index_,
                                         head.error_msg.empty() ? "subscription rejected"
                                                                : head.error_msg);
            } else {
                status = OperationStatus(subscribe_index_,
                                         t2::CStr(mc_conn_->GetErrorMsg(subscribe_index_)));
            }
        }
        if (response != NULL) {
            response->Release();
        }
        return status;
    }

    OperationStatus Logout() {
        std::string token;
        {
            std::lock_guard<std::mutex> lock(data_mu_);
            token = token_;
        }
        if (session_conn_ == NULL || token.empty()) {
            return OperationStatus();
        }

        IF2Packer* pack = NewPacker(2);
        IBizMessage* message = NewBizMessage();
        if (pack == NULL || message == NULL) {
            if (pack != NULL) {
                pack->AddRef();
                t2::ReleasePacker(pack);
            }
            if (message != NULL) {
                message->AddRef();
                message->Release();
            }
            return OperationStatus(-1, "failed to allocate logout request");
        }
        pack->AddRef();
        pack->BeginPack();
        pack->AddField("user_token", 'S', 512, 0);
        pack->AddStr(token.c_str());
        pack->EndPack();
        message->AddRef();
        message->SetFunction(10002);
        message->SetPacketType(REQUEST_PACKET);
        message->SetContent(pack->GetPackBuf(), pack->GetPackLen());
        const int hsend = session_conn_->SendBizMsg(message, 0);
        t2::ReleasePacker(pack);
        message->Release();
        if (hsend <= 0) {
            return OperationStatus(hsend, "logout send failed");
        }
        IBizMessage* response = NULL;
        const int rc = session_conn_->RecvBizMsg(
            hsend, &response, static_cast<unsigned int>(config_.query_timeout_ms), 0);
        if (rc != 0 || response == NULL) {
            return OperationStatus(rc != 0 ? rc : -1, "logout receive failed");
        }
        if (response->GetReturnCode() != 0) {
            const int code = response->GetErrorNo() != 0 ? response->GetErrorNo()
                                                         : response->GetReturnCode();
            return OperationStatus(code, t2::CStr(response->GetErrorInfo()));
        }
        int len = 0;
        const void* content = response->GetContent(len);
        if (content == NULL || len <= 0) {
            return OperationStatus(-1, "logout response has no content");
        }
        IF2UnPacker* unpack = NewUnPacker(const_cast<void*>(content),
                                          static_cast<unsigned int>(len));
        if (unpack == NULL) {
            return OperationStatus(-1, "logout unpack failed");
        }
        unpack->AddRef();
        const t2::ErrorHead head = t2::ReadHead(unpack);
        unpack->Release();
        if (!head.valid || head.error_code != 0) {
            return OperationStatus(head.error_code != 0 ? head.error_code : -1,
                                   head.error_msg.empty() ? "invalid logout response"
                                                          : head.error_msg);
        }
        return OperationStatus();
    }

    OperationStatus CleanupResourcesLocked() {
        OperationStatus status;
        if (subscriber_ != NULL) {
            if (subscribe_index_ > 0) {
                subscriber_->CancelSubscribeTopic(subscribe_index_);
            }
            subscriber_->Release();
            subscriber_ = NULL;
        }
        subscribe_index_ = -1;

        status = Logout();
        ReleaseConn(&mc_conn_);
        ReleaseConn(&query_conn_);
        ReleaseConn(&heartbeat_conn_);
        ReleaseConn(&session_conn_);

        {
            std::lock_guard<std::mutex> data_lock(data_mu_);
            token_.clear();
        }
        {
            std::lock_guard<std::mutex> queue_lock(queue_mu_);
            std::queue<QueuedEvent> empty;
            queue_.swap(empty);
            queued_payload_bytes_ = 0;
            reserved_event_count_ = 0;
            reserved_payload_bytes_ = 0;
            received_query_times_.clear();
            ++queue_generation_;
            fatal_pending_ = false;
            fatal_code_ = 0;
            fatal_message_.clear();
        }
        queries_.clear();
        pending_handles_.clear();
        refresh_scopes_.clear();
        coalescer_ = RefreshCoalescer(config_.coalesce_window_ms);
        order_coalescer_ = RefreshCoalescer(config_.coalesce_window_ms);
        std::vector<OrderView>().swap(buffered_order_pushes_);
        order_query_id_ = 0;
        order_snapshot_barrier_ = false;
        listener_reconcile_requested_.store(false);
        buffered_order_push_count_.store(0);
        next_query_id_ = 1;
        last_reconcile_ms_ = 0;
        return status;
    }

    bool IsWorkerThread() const {
        const std::thread::id current = std::this_thread::get_id();
        return dispatcher_id_ == current || heartbeat_id_ == current;
    }

    void JoinWorkers() {
        const std::thread::id current = std::this_thread::get_id();
        if (heartbeat_.joinable() && heartbeat_.get_id() != current) {
            heartbeat_.join();
        }
        if (dispatcher_.joinable() && dispatcher_.get_id() != current) {
            dispatcher_.join();
        }
    }

    void WorkerStarted(bool dispatcher) {
        std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mu_);
        if (dispatcher) {
            dispatcher_id_ = std::this_thread::get_id();
        } else {
            heartbeat_id_ = std::this_thread::get_id();
        }
    }

    void WorkerExited(bool dispatcher) {
        std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mu_);
        if (dispatcher) {
            dispatcher_id_ = std::thread::id();
        } else {
            heartbeat_id_ = std::thread::id();
        }
        if (active_workers_ > 0) {
            --active_workers_;
        }
        if (active_workers_ == 0 && lifecycle_state_ == kStopping) {
            cleanup_status_ = CleanupResourcesLocked();
            lifecycle_state_ = kStopped;
        }
    }

    bool QueueHasCapacity(size_t payload_bytes) const {
        const size_t event_count = queue_.size() + reserved_event_count_;
        const size_t used_bytes = queued_payload_bytes_ + reserved_payload_bytes_;
        return event_count < config_.max_event_queue_size &&
               used_bytes <= config_.max_event_queue_bytes &&
               payload_bytes <= config_.max_event_queue_bytes - used_bytes;
    }

    bool ReserveEvent(size_t payload_bytes, int received_query_handle,
                      int64_t received_ms, uint64_t* generation) {
        if (generation == NULL) {
            return false;
        }
        if (payload_bytes > config_.max_event_queue_bytes) {
            stat_rejected_events_.fetch_add(1);
            SignalFatal(-1005, "event payload exceeds max_event_queue_bytes");
            return false;
        }

        std::unique_lock<std::mutex> lock(queue_mu_);
        if (!accepting_events_.load()) {
            return false;
        }
        if (received_query_handle > 0) {
            try {
                received_query_times_.insert(
                    std::make_pair(received_query_handle, received_ms));
            } catch (const std::bad_alloc&) {
                lock.unlock();
                stat_rejected_events_.fetch_add(1);
                SignalFatal(-1006, "failed to track a received query response");
                return false;
            }
        }
        if (accepting_events_.load() && !QueueHasCapacity(payload_bytes)) {
            stat_enqueue_waits_.fetch_add(1);
        }
        const bool ready = queue_space_cv_.wait_for(
            lock, std::chrono::milliseconds(config_.enqueue_timeout_ms),
            [this, payload_bytes, received_query_handle] {
                return !accepting_events_.load() ||
                       (received_query_handle > 0 &&
                        received_query_times_.find(received_query_handle) ==
                            received_query_times_.end()) ||
                       QueueHasCapacity(payload_bytes);
            });
        if (!ready) {
            if (received_query_handle > 0) {
                received_query_times_.erase(received_query_handle);
            }
            lock.unlock();
            stat_rejected_events_.fetch_add(1);
            SignalFatal(-1005, "event queue capacity wait timed out");
            return false;
        }
        if (!accepting_events_.load()) {
            if (received_query_handle > 0) {
                received_query_times_.erase(received_query_handle);
            }
            return false;
        }
        if (received_query_handle > 0 &&
            received_query_times_.find(received_query_handle) ==
                received_query_times_.end()) {
            return false;
        }

        ++reserved_event_count_;
        reserved_payload_bytes_ += payload_bytes;
        *generation = queue_generation_;
        const size_t event_count = queue_.size() + reserved_event_count_;
        const size_t used_bytes = queued_payload_bytes_ + reserved_payload_bytes_;
        if (event_count > peak_queued_events_) {
            peak_queued_events_ = event_count;
        }
        if (used_bytes > peak_queued_payload_bytes_) {
            peak_queued_payload_bytes_ = used_bytes;
        }
        return true;
    }

    bool CancelReservation(size_t payload_bytes, int received_query_handle,
                           uint64_t generation) {
        {
            std::lock_guard<std::mutex> lock(queue_mu_);
            if (generation != queue_generation_) {
                return false;
            }
            if (received_query_handle > 0) {
                received_query_times_.erase(received_query_handle);
            }
            if (reserved_event_count_ > 0) {
                --reserved_event_count_;
            }
            if (reserved_payload_bytes_ >= payload_bytes) {
                reserved_payload_bytes_ -= payload_bytes;
            } else {
                reserved_payload_bytes_ = 0;
            }
        }
        queue_space_cv_.notify_all();
        return true;
    }

    bool CommitReserved(QueuedEvent event, size_t payload_bytes,
                        uint64_t generation) {
        bool allocation_failed = false;
        const int received_query_handle =
            event.kind == kEventQuery ? event.hsend : 0;
        {
            std::lock_guard<std::mutex> lock(queue_mu_);
            if (generation != queue_generation_ || reserved_event_count_ == 0 ||
                reserved_payload_bytes_ < payload_bytes) {
                return false;
            }
            --reserved_event_count_;
            reserved_payload_bytes_ -= payload_bytes;
            if (received_query_handle > 0 &&
                received_query_times_.find(received_query_handle) ==
                    received_query_times_.end()) {
                queue_space_cv_.notify_all();
                return false;
            }
            if (!accepting_events_.load()) {
                if (received_query_handle > 0) {
                    received_query_times_.erase(received_query_handle);
                }
                queue_space_cv_.notify_all();
                return false;
            }
            try {
                event.enqueued_ms = NowMs();
                queue_.push(std::move(event));
                queued_payload_bytes_ += payload_bytes;
                stat_enqueued_events_.fetch_add(1);
            } catch (const std::bad_alloc&) {
                if (received_query_handle > 0) {
                    received_query_times_.erase(received_query_handle);
                }
                allocation_failed = true;
            }
        }
        if (allocation_failed) {
            stat_rejected_events_.fetch_add(1);
            SignalFatal(-1006, "failed to allocate event queue storage");
            return false;
        }
        queue_cv_.notify_one();
        return true;
    }

    bool Enqueue(QueuedEvent event) {
        const size_t payload_bytes = event.payload.size();
        uint64_t reservation_generation = 0;
        return ReserveEvent(payload_bytes, 0, 0, &reservation_generation) &&
               CommitReserved(std::move(event), payload_bytes,
                              reservation_generation);
    }

    bool QueryResponseReceivedBefore(int hsend, int64_t deadline_ms) const {
        std::lock_guard<std::mutex> lock(queue_mu_);
        const std::map<int, int64_t>::const_iterator received =
            received_query_times_.find(hsend);
        return received != received_query_times_.end() &&
               received->second < deadline_ms;
    }

    void ClearReceivedQueryHandle(int hsend) {
        if (hsend <= 0) {
            return;
        }
        bool erased = false;
        {
            std::lock_guard<std::mutex> lock(queue_mu_);
            erased = received_query_times_.erase(hsend) != 0U;
        }
        if (erased) {
            queue_space_cv_.notify_all();
        }
    }

    void SignalFatal(int code, const std::string& message) {
        {
            std::lock_guard<std::mutex> lock(queue_mu_);
            if (!fatal_pending_) {
                fatal_pending_ = true;
                fatal_code_ = code;
                fatal_message_ = message;
            }
            accepting_events_.store(false);
        }
        queue_cv_.notify_all();
        queue_space_cv_.notify_all();
        stop_cv_.notify_all();
    }

    std::shared_ptr<IMarketListener> Listener(uint64_t* epoch) const {
        std::lock_guard<std::mutex> lock(data_mu_);
        if (epoch != NULL) {
            *epoch = listener_epoch_.load();
        }
        return listener_;
    }

    void RecordListenerDuration(int64_t started_ms) {
        const int64_t elapsed = NowMs() - started_ms;
        if (elapsed > 0) {
            UpdateAtomicMax(&stat_max_listener_callback_ms_,
                            static_cast<uint64_t>(elapsed));
        }
    }

    template <typename Callback>
    detail::ListenerCallResult InvokeListener(const char* callback_name,
                                              const Callback& callback) {
        const detail::ListenerCallResult result =
            detail::InvokeListenerNoThrow(callback);
        if (result == detail::kListenerCallSucceeded) {
            return result;
        }
        if (running_.load()) {
            if (result == detail::kListenerCallThrewBadAlloc) {
                SignalFatal(-1006, "listener callback failed to allocate memory");
            } else {
                std::ostringstream out;
                out << "listener " << callback_name << " threw ";
                if (result == detail::kListenerCallThrewStdException) {
                    out << "std::exception";
                } else {
                    out << "an unknown exception";
                }
                SignalFatal(kListenerCallbackError, out.str());
            }
        }
        return result;
    }

    detail::ListenerCallResult NotifySession(int code, const char* message) {
        const std::shared_ptr<IMarketListener> listener = Listener(NULL);
        if (!listener) {
            return detail::kListenerCallSucceeded;
        }
        const int64_t started_ms = NowMs();
        const detail::ListenerCallResult result =
            InvokeListener("OnSessionEvent", [=] {
                listener->OnSessionEvent(code, message);
            });
        RecordListenerDuration(started_ms);
        return result;
    }

    QueryScope ConfiguredScope() const {
        QueryScope scope;
        scope.account_code = config_.account_code;
        scope.asset_no = config_.asset_no;
        scope.combi_no = config_.combi_no;
        return scope;
    }

    void BeginRuntimeFailure(int code, const std::string& message) {
        {
            std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mu_);
            if (lifecycle_state_ == kRunning) {
                lifecycle_state_ = kStopping;
            }
            running_.store(false);
            accepting_events_.store(false);
        }
        queue_cv_.notify_all();
        queue_space_cv_.notify_all();
        stop_cv_.notify_all();
        NotifySession(code, message.c_str());
    }

    void HeartbeatLoop() {
        WorkerStarted(false);
        while (running_.load()) {
            {
                std::unique_lock<std::mutex> lock(stop_mu_);
                if (stop_cv_.wait_for(
                        lock, std::chrono::milliseconds(config_.heartbeat_interval_ms),
                        [this] { return !running_.load(); })) {
                    break;
                }
            }
            const OperationStatus status = SendHeartbeat();
            if (!status.Ok()) {
                SignalFatal(status.code, status.message);
                break;
            }
        }
        WorkerExited(false);
    }

    OperationStatus SendHeartbeat() {
        std::string token;
        {
            std::lock_guard<std::mutex> lock(data_mu_);
            token = token_;
        }
        if (token.empty() || heartbeat_conn_ == NULL) {
            return OperationStatus(-1, "heartbeat connection or token is unavailable");
        }

        IF2Packer* pack = NewPacker(2);
        IBizMessage* message = NewBizMessage();
        if (pack == NULL || message == NULL) {
            if (pack != NULL) {
                pack->AddRef();
                t2::ReleasePacker(pack);
            }
            if (message != NULL) {
                message->AddRef();
                message->Release();
            }
            return OperationStatus(-1, "failed to allocate heartbeat request");
        }
        pack->AddRef();
        pack->BeginPack();
        pack->AddField("user_token", 'S', 512, 0);
        pack->AddStr(token.c_str());
        pack->EndPack();
        message->AddRef();
        message->SetFunction(10000);
        message->SetPacketType(REQUEST_PACKET);
        message->SetContent(pack->GetPackBuf(), pack->GetPackLen());
        const int hsend = heartbeat_conn_->SendBizMsg(message, 0);
        t2::ReleasePacker(pack);
        message->Release();
        if (hsend <= 0) {
            return OperationStatus(hsend, "heartbeat send failed");
        }

        IBizMessage* response = NULL;
        const int rc = heartbeat_conn_->RecvBizMsg(hsend, &response, 3000, 0);
        if (rc != 0 || response == NULL) {
            return OperationStatus(rc != 0 ? rc : -1, "heartbeat receive failed");
        }
        if (response->GetReturnCode() != 0) {
            const int code = response->GetErrorNo() != 0 ? response->GetErrorNo()
                                                         : response->GetReturnCode();
            return OperationStatus(code, t2::CStr(response->GetErrorInfo()));
        }
        int len = 0;
        const void* content = response->GetContent(len);
        if (content == NULL || len <= 0) {
            return OperationStatus(-1, "heartbeat response has no content");
        }
        IF2UnPacker* unpack = NewUnPacker(const_cast<void*>(content),
                                          static_cast<unsigned int>(len));
        if (unpack == NULL) {
            return OperationStatus(-1, "heartbeat unpack failed");
        }
        unpack->AddRef();
        const t2::ErrorHead head = t2::ReadHead(unpack);
        unpack->Release();
        if (!head.valid || head.error_code != 0) {
            return OperationStatus(head.error_code != 0 ? head.error_code : -1,
                                   head.error_msg.empty() ? "invalid heartbeat response"
                                                          : head.error_msg);
        }
        return OperationStatus();
    }

    int64_t DispatchWaitMs(int64_t now) const {
        int64_t next_deadline = last_reconcile_ms_ + config_.reconcile_interval_ms;
        int64_t due = 0;
        if (coalescer_.NextDueMs(&due) && due < next_deadline) {
            next_deadline = due;
        }
        if (order_coalescer_.NextDueMs(&due) && due < next_deadline) {
            next_deadline = due;
        }
        for (std::map<int, uint64_t>::const_iterator handle = pending_handles_.begin();
             handle != pending_handles_.end(); ++handle) {
            const std::map<uint64_t, QueryChain>::const_iterator chain =
                queries_.find(handle->second);
            if (chain == queries_.end()) {
                continue;
            }
            const int64_t page_deadline =
                chain->second.sent_ms + config_.query_timeout_ms;
            const int64_t chain_deadline =
                chain->second.started_ms + config_.query_chain_timeout_ms;
            if (!QueryResponseReceivedBefore(handle->first, page_deadline) &&
                page_deadline < next_deadline) {
                next_deadline = page_deadline;
            }
            if (chain_deadline < next_deadline) {
                next_deadline = chain_deadline;
            }
        }
        return next_deadline > now ? next_deadline - now : 0;
    }

    void DispatchLoop() {
        WorkerStarted(true);
        std::vector<QueuedEvent> batch;
        try {
            batch.reserve(config_.max_dispatch_batch_size);
        } catch (const std::bad_alloc&) {
            BeginRuntimeFailure(-1006, "failed to allocate dispatcher batch");
            WorkerExited(true);
            return;
        }
        while (running_.load()) {
            batch.clear();
            bool has_fatal = false;
            int fatal_code = 0;
            std::string fatal_message;
            {
                const int64_t wait_ms = DispatchWaitMs(NowMs());
                std::unique_lock<std::mutex> lock(queue_mu_);
                queue_cv_.wait_for(lock, std::chrono::milliseconds(wait_ms), [this] {
                    return !running_.load() || fatal_pending_ || !queue_.empty() ||
                           listener_reconcile_requested_.load();
                });
                if (fatal_pending_) {
                    has_fatal = true;
                    fatal_code = fatal_code_;
                    fatal_message.swap(fatal_message_);
                    fatal_pending_ = false;
                } else {
                    while (!queue_.empty() &&
                           batch.size() < config_.max_dispatch_batch_size) {
                        const size_t payload_bytes = queue_.front().payload.size();
                        batch.push_back(std::move(queue_.front()));
                        queue_.pop();
                        if (queued_payload_bytes_ >= payload_bytes) {
                            queued_payload_bytes_ -= payload_bytes;
                        } else {
                            queued_payload_bytes_ = 0;
                        }
                    }
                }
            }
            queue_space_cv_.notify_all();

            if (has_fatal) {
                BeginRuntimeFailure(fatal_code, fatal_message);
                break;
            }

            for (size_t i = 0;
                 i < batch.size() && running_.load() && accepting_events_.load();
                 ++i) {
                const int64_t queue_delay = NowMs() - batch[i].enqueued_ms;
                if (queue_delay > 0) {
                    UpdateAtomicMax(
                        &stat_max_event_queue_delay_ms_,
                        static_cast<uint64_t>(queue_delay));
                }
                try {
                    if (batch[i].kind == kEventPush) {
                        HandlePush(batch[i]);
                    } else if (batch[i].kind == kEventQuery) {
                        HandleQuery(batch[i]);
                    } else if (batch[i].kind == kEventInitialReconcile) {
                        RequestFullReconcile(true);
                    }
                } catch (const std::bad_alloc&) {
                    SignalFatal(-1006, "failed to allocate while dispatching an event");
                } catch (const std::exception&) {
                    SignalFatal(kDispatcherExceptionError,
                                "unhandled std::exception while dispatching an event");
                } catch (...) {
                    SignalFatal(kDispatcherExceptionError,
                                "unhandled unknown exception while dispatching an event");
                }
                std::vector<char>().swap(batch[i].payload);
                std::string().swap(batch[i].error_info);
            }
            if (!running_.load()) {
                break;
            }
            if (!accepting_events_.load()) {
                continue;
            }

            try {
                if (listener_reconcile_requested_.exchange(false)) {
                    RequestFullReconcile();
                }

                const int64_t now = NowMs();
                FlushOrderSnapshot(now);
                FlushDueQueries(now);
                ExpirePending(now);
                if (now - last_reconcile_ms_ >= config_.reconcile_interval_ms) {
                    RequestFullReconcile();
                    last_reconcile_ms_ = now;
                }
            } catch (const std::bad_alloc&) {
                SignalFatal(-1006,
                            "failed to allocate while scheduling reconciliation");
            } catch (const std::exception&) {
                SignalFatal(
                    kDispatcherExceptionError,
                    "unhandled std::exception while scheduling reconciliation");
            } catch (...) {
                SignalFatal(
                    kDispatcherExceptionError,
                    "unhandled unknown exception while scheduling reconciliation");
            }
        }
        WorkerExited(true);
    }

    void RequestFullReconcile(bool immediate_order = false) {
        RequestRefresh(ConfiguredScope());
        RequestOrderSnapshot(immediate_order);
    }

    void RequestRefresh(const QueryScope& scope) {
        const std::string key = RefreshKey(scope);
        refresh_scopes_[key] = scope;
        coalescer_.Request(key, NowMs());
    }

    void FlushDueQueries(int64_t now) {
        const std::vector<std::string> due = coalescer_.Due(now);
        for (size_t i = 0; i < due.size(); ++i) {
            const std::map<std::string, QueryScope>::const_iterator scope =
                refresh_scopes_.find(due[i]);
            if (scope == refresh_scopes_.end()) {
                NotifySession(-1, "missing refresh scope");
                continue;
            }
            coalescer_.MarkInFlight(due[i], 2);
            StartRefreshBatch(due[i], scope->second);
        }
    }

    void StartRefreshBatch(const std::string& key, const QueryScope& scope) {
        if (CreateQuery(31001, scope, key) == 0) {
            coalescer_.MarkDone(key, NowMs());
        }
        if (CreateQuery(34001, scope, key) == 0) {
            coalescer_.MarkDone(key, NowMs());
        }
    }

    void RequestOrderSnapshot(bool immediate = false) {
        stat_order_snapshot_requests_.fetch_add(1);
        if (order_coalescer_.HasPending(kOrderSnapshotKey) ||
            order_coalescer_.HasInFlight(kOrderSnapshotKey)) {
            stat_order_snapshot_coalesced_.fetch_add(1);
        }
        const int64_t now = NowMs();
        if (immediate) {
            order_coalescer_.RequestNow(kOrderSnapshotKey, now);
        } else {
            order_coalescer_.Request(kOrderSnapshotKey, now);
        }
    }

    void FlushOrderSnapshot(int64_t now) {
        const std::vector<std::string> due = order_coalescer_.Due(now);
        if (due.empty()) {
            return;
        }
        order_coalescer_.MarkInFlight(kOrderSnapshotKey, 1);
        order_snapshot_barrier_ = true;
        const uint64_t query_id =
            CreateQuery(32001, ConfiguredScope(), std::string());
        if (query_id == 0) {
            order_snapshot_barrier_ = false;
            ReplayBufferedOrderPushes();
            order_coalescer_.MarkDone(kOrderSnapshotKey, NowMs());
            NotifySession(-1, "failed to start 32001 order snapshot");
            return;
        }
        order_query_id_ = query_id;
        stat_order_snapshots_started_.fetch_add(1);
    }

    uint64_t CreateQuery(int func_no, const QueryScope& scope,
                         const std::string& refresh_key) {
        QueryChain chain;
        chain.id = next_query_id_++;
        chain.refresh_key = refresh_key;
        chain.func_no = func_no;
        chain.scope = scope;
        chain.started_ms = NowMs();
        const uint64_t id = chain.id;
        queries_[id] = std::move(chain);
        stat_query_chains_started_.fetch_add(1);
        if (!SendQueryPage(id)) {
            queries_.erase(id);
            stat_query_chains_failed_.fetch_add(1);
            return 0;
        }
        return id;
    }

    bool SendQueryPage(uint64_t query_id) {
        std::map<uint64_t, QueryChain>::iterator chain = queries_.find(query_id);
        if (chain == queries_.end() || query_conn_ == NULL) {
            return false;
        }
        std::string token;
        {
            std::lock_guard<std::mutex> lock(data_mu_);
            token = token_;
        }
        if (token.empty()) {
            return false;
        }

        IF2Packer* pack = NewPacker(2);
        IBizMessage* message = NewBizMessage();
        if (pack == NULL || message == NULL) {
            if (pack != NULL) {
                pack->AddRef();
                t2::ReleasePacker(pack);
            }
            if (message != NULL) {
                message->AddRef();
                message->Release();
            }
            return false;
        }
        pack->AddRef();
        pack->BeginPack();
        pack->AddField("user_token", 'S', 512, 0);
        if (!chain->second.scope.account_code.empty()) {
            pack->AddField("account_code", 'S', 32, 0);
        }
        if (!chain->second.scope.asset_no.empty()) {
            pack->AddField("asset_no", 'S', 32, 0);
        }
        if (!chain->second.scope.combi_no.empty()) {
            pack->AddField("combi_no", 'S', 16, 0);
        }
        if (IsPaginatedFunction(chain->second.func_no) &&
            !chain->second.position_str.empty()) {
            pack->AddField("position_str", 'S', 128, 0);
        }
        if (IsPaginatedFunction(chain->second.func_no)) {
            pack->AddField("request_num", 'I', 8, 0);
        }

        pack->AddStr(token.c_str());
        if (!chain->second.scope.account_code.empty()) {
            pack->AddStr(chain->second.scope.account_code.c_str());
        }
        if (!chain->second.scope.asset_no.empty()) {
            pack->AddStr(chain->second.scope.asset_no.c_str());
        }
        if (!chain->second.scope.combi_no.empty()) {
            pack->AddStr(chain->second.scope.combi_no.c_str());
        }
        if (IsPaginatedFunction(chain->second.func_no) &&
            !chain->second.position_str.empty()) {
            pack->AddStr(chain->second.position_str.c_str());
        }
        if (IsPaginatedFunction(chain->second.func_no)) {
            pack->AddInt(config_.query_page_size);
        }
        pack->EndPack();

        message->AddRef();
        message->SetFunction(chain->second.func_no);
        message->SetPacketType(REQUEST_PACKET);
        message->SetContent(pack->GetPackBuf(), pack->GetPackLen());
        const int hsend = query_conn_->SendBizMsg(message, 1);
        t2::ReleasePacker(pack);
        message->Release();
        if (hsend <= 0) {
            std::ostringstream out;
            out << "send query " << chain->second.func_no << " failed: " << hsend;
            NotifySession(hsend, out.str().c_str());
            return false;
        }
        if (pending_handles_.find(hsend) != pending_handles_.end()) {
            SignalFatal(-1008, "T2SDK reused an in-flight send handle");
            return false;
        }
        chain->second.hsend = hsend;
        chain->second.sent_ms = NowMs();
        pending_handles_[hsend] = query_id;
        return true;
    }

    void HandleQuery(const QueuedEvent& event) {
        ClearReceivedQueryHandle(event.hsend);
        const std::map<int, uint64_t>::iterator pending =
            pending_handles_.find(event.hsend);
        if (pending == pending_handles_.end()) {
            return;
        }
        const uint64_t query_id = pending->second;
        pending_handles_.erase(pending);
        std::map<uint64_t, QueryChain>::iterator chain = queries_.find(query_id);
        if (chain == queries_.end()) {
            return;
        }
        chain->second.hsend = 0;

        if (event.received_ms - chain->second.sent_ms >=
            config_.query_timeout_ms) {
            NotifySession(2001, "query page timeout");
            CompleteQuery(query_id, false);
            return;
        }
        if (NowMs() - chain->second.started_ms >= config_.query_chain_timeout_ms) {
            NotifySession(2001, "query chain timeout");
            CompleteQuery(query_id, false);
            return;
        }

        const size_t payload_bytes = event.payload.size();
        if (chain->second.page_count >= config_.max_query_pages ||
            chain->second.payload_bytes > config_.max_query_bytes ||
            payload_bytes > config_.max_query_bytes - chain->second.payload_bytes) {
            NotifySession(-1, "query chain exceeds configured page or byte limit");
            CompleteQuery(query_id, false);
            return;
        }
        ++chain->second.page_count;
        chain->second.payload_bytes += payload_bytes;
        stat_query_pages_.fetch_add(1);
        stat_query_payload_bytes_.fetch_add(static_cast<uint64_t>(payload_bytes));

        if (event.return_code != 0) {
            const int code = event.error_no != 0 ? event.error_no : event.return_code;
            const std::string text = event.error_info.empty()
                                         ? "T2SDK query transport error"
                                         : event.error_info;
            NotifySession(code, text.c_str());
            CompleteQuery(query_id, false);
            return;
        }
        if (event.func_no != chain->second.func_no) {
            NotifySession(-1, "query response function does not match request");
            CompleteQuery(query_id, false);
            return;
        }
        if (event.payload.empty()) {
            NotifySession(-1, "query response has no content");
            CompleteQuery(query_id, false);
            return;
        }

        IF2UnPacker* unpack = NewUnPacker(
            const_cast<char*>(&event.payload[0]),
            static_cast<unsigned int>(event.payload.size()));
        if (unpack == NULL) {
            NotifySession(-1, "query unpack failed");
            CompleteQuery(query_id, false);
            return;
        }
        unpack->AddRef();
        const t2::ErrorHead head = t2::ReadHead(unpack);
        if (!head.valid || head.error_code != 0) {
            const int code = head.error_code != 0 ? head.error_code : -1;
            const std::string text = head.error_msg.empty() ? "invalid query response"
                                                             : head.error_msg;
            unpack->Release();
            NotifySession(code, text.c_str());
            CompleteQuery(query_id, false);
            return;
        }

        bool has_more = false;
        std::string parse_error;
        const bool parsed = ParseQueryPage(&chain->second, unpack, head, &has_more,
                                           &parse_error);
        unpack->Release();
        if (!parsed) {
            NotifySession(-1, parse_error.c_str());
            CompleteQuery(query_id, false);
            return;
        }
        if (has_more) {
            if (chain->second.page_count >= config_.max_query_pages) {
                NotifySession(-1, "query chain exceeds configured page limit");
                CompleteQuery(query_id, false);
                return;
            }
            if (NowMs() - chain->second.started_ms >=
                config_.query_chain_timeout_ms) {
                NotifySession(2001, "query chain timeout");
                CompleteQuery(query_id, false);
                return;
            }
            if (!SendQueryPage(query_id)) {
                NotifySession(-1, "failed to send the next query page");
                CompleteQuery(query_id, false);
            }
            return;
        }
        CompleteQuery(query_id, true);
    }

    bool ParseQueryPage(QueryChain* chain, IF2UnPacker* unpack,
                        const t2::ErrorHead& head, bool* has_more,
                        std::string* error) {
        if (chain == NULL || has_more == NULL || error == NULL) {
            return false;
        }
        *has_more = false;
        if (unpack->GetDatasetCount() <= 1) {
            if (head.data_count == 0) {
                return true;
            }
            *error = "query header reports rows but data set is missing";
            return false;
        }

        unpack->SetCurrentDatasetByIndex(1);
        const unsigned int row_count = unpack->GetRowCount();
        if (head.data_count < 0 || static_cast<unsigned int>(head.data_count) != row_count) {
            *error = "query DataCount does not match data-set row count";
            return false;
        }
        const size_t page_rows = static_cast<size_t>(row_count);
        if (chain->row_count > config_.max_query_rows ||
            page_rows > config_.max_query_rows - chain->row_count) {
            *error = "query chain exceeds configured row limit";
            return false;
        }
        chain->row_count += page_rows;
        stat_query_rows_.fetch_add(static_cast<uint64_t>(page_rows));

        ColumnLayout<kPositionColumnCount> position_columns;
        ColumnLayout<kOrderColumnCount> order_columns;
        ColumnLayout<kAccountColumnCount> account_columns;
        int cursor_index = -1;
        if (row_count > 0U) {
            if (chain->func_no == 31001) {
                if (!ResolveColumns(unpack, kPositionColumnNames, &position_columns,
                                    "31001", error)) {
                    return false;
                }
            } else if (chain->func_no == 32001) {
                if (!ResolveColumns(unpack, kOrderColumnNames, &order_columns,
                                    "32001", error)) {
                    return false;
                }
            } else if (chain->func_no == 34001) {
                if (!ResolveColumns(unpack, kAccountColumnNames, &account_columns,
                                    "34001", error)) {
                    return false;
                }
            } else {
                *error = "unsupported query function " + FunctionName(chain->func_no);
                return false;
            }
            if (IsPaginatedFunction(chain->func_no)) {
                cursor_index = t2::FieldIndex(unpack, "position_str");
                if (cursor_index < 0) {
                    *error = "query page is missing position_str";
                    return false;
                }
            }
        }
        unpack->First();
        std::string last_cursor;
        for (unsigned int row_index = 0; row_index < row_count; ++row_index) {
            bool ok = false;
            if (chain->func_no == 31001) {
                ok = ParsePositionRow(unpack, position_columns, chain, error);
            } else if (chain->func_no == 32001) {
                ok = ParseOrderRow(unpack, order_columns, chain, error);
            } else if (chain->func_no == 34001) {
                ok = ParseAccountRow(unpack, account_columns, chain, error);
            } else {
                *error = "unsupported query function " + FunctionName(chain->func_no);
                return false;
            }
            if (!ok) {
                return false;
            }
            if (IsPaginatedFunction(chain->func_no) &&
                !t2::FieldStrByIndex(unpack, cursor_index, &last_cursor)) {
                *error = "query page is missing position_str";
                return false;
            }
            if (row_index + 1U < row_count) {
                unpack->Next();
            }
        }

        if (IsPaginatedFunction(chain->func_no) &&
            row_count >= static_cast<unsigned int>(config_.query_page_size)) {
            if (last_cursor.empty() || last_cursor == chain->position_str ||
                !chain->seen_cursors.insert(last_cursor).second) {
                *error = "query pagination cursor is empty or repeated";
                return false;
            }
            chain->position_str = last_cursor;
            *has_more = true;
        }
        return true;
    }

    bool ParsePositionRow(IF2UnPacker* unpack,
                          const ColumnLayout<kPositionColumnCount>& columns,
                          QueryChain* chain,
                          std::string* error) {
        PositionView row;
        if (!t2::FieldStrByIndex(unpack, columns.index[kPositionAccount],
                                 &row.account_code) ||
            !t2::FieldStrByIndex(unpack, columns.index[kPositionAsset],
                                 &row.asset_no) ||
            !t2::FieldStrByIndex(unpack, columns.index[kPositionCombi],
                                 &row.combi_no) ||
            !t2::FieldStrByIndex(unpack, columns.index[kPositionMarket],
                                 &row.market_no) ||
            !t2::FieldStrByIndex(unpack, columns.index[kPositionStock],
                                 &row.stock_code) ||
            !t2::FieldStrByIndex(unpack, columns.index[kPositionInvestType],
                                 &row.invest_type) ||
            !t2::FieldStrByIndex(unpack, columns.index[kPositionStockholder],
                                 &row.stockholder_id) ||
            !t2::FieldStrByIndex(unpack, columns.index[kPositionHoldSeat],
                                 &row.hold_seat) ||
            !t2::FieldInt64ByIndex(unpack, columns.index[kPositionCurrentAmount],
                                   &row.current_amount) ||
            !t2::FieldInt64ByIndex(unpack, columns.index[kPositionEnableAmount],
                                   &row.enable_amount) ||
            !t2::FieldInt64ByIndex(unpack, columns.index[kPositionPreBuyAmount],
                                   &row.pre_buy_amount) ||
            !t2::FieldInt64ByIndex(unpack, columns.index[kPositionPreSellAmount],
                                   &row.pre_sell_amount) ||
            !t2::FieldInt64ByIndex(unpack, columns.index[kPositionTodayBuyAmount],
                                   &row.today_buy_amount) ||
            !t2::FieldInt64ByIndex(unpack, columns.index[kPositionTodaySellAmount],
                                   &row.today_sell_amount)) {
            *error = "31001 response contains a missing or invalid field";
            return false;
        }
        if (!MatchesScope(row, chain->scope)) {
            *error = "31001 response contains a row outside the requested scope";
            return false;
        }
        row.unsellable_amount = UnsellableAmount(row.current_amount, row.enable_amount);
        std::string key = PositionKey(row);
        if (!chain->positions.insert(std::make_pair(key, std::move(row))).second) {
            *error = "31001 response contains a duplicate position key";
            return false;
        }
        chain->position_row_order.push_back(std::move(key));
        return true;
    }

    bool ParseOrderRow(IF2UnPacker* unpack,
                       const ColumnLayout<kOrderColumnCount>& columns,
                       QueryChain* chain,
                       std::string* error) {
        OrderView row;
        int64_t business_date = 0;
        if (!t2::FieldInt64ByIndex(unpack, columns.index[kOrderDate],
                                   &business_date) ||
            business_date < 0 || business_date > 99999999 ||
            !t2::FieldInt64ByIndex(unpack, columns.index[kOrderEntrustNo],
                                   &row.entrust_no) ||
            !t2::FieldStrByIndex(unpack, columns.index[kOrderState],
                                 &row.entrust_state) ||
            !t2::FieldStrByIndex(unpack, columns.index[kOrderAccount],
                                 &row.account_code) ||
            !t2::FieldStrByIndex(unpack, columns.index[kOrderAsset],
                                 &row.asset_no) ||
            !t2::FieldStrByIndex(unpack, columns.index[kOrderCombi],
                                 &row.combi_no) ||
            !t2::FieldStrByIndex(unpack, columns.index[kOrderMarket],
                                 &row.market_no) ||
            !t2::FieldStrByIndex(unpack, columns.index[kOrderStock],
                                 &row.stock_code) ||
            !t2::FieldStrByIndex(unpack, columns.index[kOrderDirection],
                                 &row.entrust_direction) ||
            !t2::FieldDecimalByIndex(unpack, columns.index[kOrderPrice],
                                     &row.entrust_price) ||
            !t2::FieldInt64ByIndex(unpack, columns.index[kOrderAmount],
                                   &row.entrust_amount) ||
            !t2::FieldInt64ByIndex(unpack, columns.index[kOrderDealAmount],
                                   &row.deal_amount) ||
            !t2::FieldInt64ByIndex(unpack, columns.index[kOrderWithdrawAmount],
                                   &row.cancel_amount) ||
            !t2::FieldInt64ByIndex(unpack, columns.index[kOrderExtsystemId],
                                   &row.extsystem_id)) {
            *error = "32001 response contains a missing or invalid field";
            return false;
        }
        row.business_date = static_cast<int>(business_date);
        if ((!chain->scope.account_code.empty() &&
              row.account_code != chain->scope.account_code) ||
            (!chain->scope.asset_no.empty() && row.asset_no != chain->scope.asset_no) ||
            (!chain->scope.combi_no.empty() && row.combi_no != chain->scope.combi_no)) {
            *error = "32001 response contains a row outside the requested scope";
            return false;
        }
        const int64_t entrust_no = row.entrust_no;
        if (entrust_no <= 0 ||
            !chain->orders
                 .insert(std::make_pair(entrust_no, std::move(row)))
                 .second) {
            *error = "32001 response contains an invalid or duplicate entrust_no";
            return false;
        }
        chain->order_row_order.push_back(entrust_no);
        return true;
    }

    bool ParseAccountRow(IF2UnPacker* unpack,
                         const ColumnLayout<kAccountColumnCount>& columns,
                         QueryChain* chain,
                         std::string* error) {
        AccountView row;
        if (!t2::FieldStrByIndex(unpack, columns.index[kAccountCode],
                                 &row.account_code) ||
            !t2::FieldStrByIndex(unpack, columns.index[kAccountAsset],
                                 &row.asset_no) ||
            !t2::FieldDecimalByIndex(unpack, columns.index[kAccountEnableT0],
                                     &row.enable_balance_t0) ||
            !t2::FieldDecimalByIndex(unpack, columns.index[kAccountEnableT1],
                                     &row.enable_balance_t1) ||
            !t2::FieldDecimalByIndex(unpack, columns.index[kAccountCurrent],
                                     &row.current_balance)) {
            *error = "34001 response contains a missing or invalid field";
            return false;
        }
        if ((!chain->scope.account_code.empty() &&
             row.account_code != chain->scope.account_code) ||
            (!chain->scope.asset_no.empty() && row.asset_no != chain->scope.asset_no)) {
            *error = "34001 response contains a row outside the requested scope";
            return false;
        }
        std::string key = AccountKey(row);
        if (!chain->accounts.insert(std::make_pair(key, std::move(row))).second) {
            *error = "34001 response contains a duplicate account/asset key";
            return false;
        }
        chain->account_row_order.push_back(std::move(key));
        return true;
    }

    void CompleteQuery(uint64_t query_id, bool success) {
        std::map<uint64_t, QueryChain>::iterator found = queries_.find(query_id);
        if (found == queries_.end()) {
            return;
        }
        QueryChain chain = std::move(found->second);
        queries_.erase(found);

        if (success) {
            if (chain.func_no == 31001) {
                ApplyPositions(chain.scope, std::move(chain.positions),
                               std::move(chain.position_row_order));
            } else if (chain.func_no == 34001) {
                ApplyAccounts(chain.scope, std::move(chain.accounts),
                              std::move(chain.account_row_order));
            } else if (chain.func_no == 32001) {
                ApplyOrders(std::move(chain.orders),
                            std::move(chain.order_row_order));
            }
            stat_query_chains_completed_.fetch_add(1);
        } else {
            stat_query_chains_failed_.fetch_add(1);
        }

        if (chain.func_no == 32001) {
            order_query_id_ = 0;
            order_snapshot_barrier_ = false;
            if (accepting_events_.load()) {
                ReplayBufferedOrderPushes();
            }
            order_coalescer_.MarkDone(kOrderSnapshotKey, NowMs());
        } else if (!chain.refresh_key.empty()) {
            coalescer_.MarkDone(chain.refresh_key, NowMs());
        }
    }

    void ExpirePending(int64_t now) {
        std::set<uint64_t> expired;
        for (std::map<int, uint64_t>::iterator handle = pending_handles_.begin();
             handle != pending_handles_.end();) {
            const std::map<uint64_t, QueryChain>::iterator chain =
                queries_.find(handle->second);
            const bool missing = chain == queries_.end();
            const bool chain_timed_out =
                !missing && now - chain->second.started_ms >=
                                config_.query_chain_timeout_ms;
            const bool page_timed_out =
                !missing &&
                !QueryResponseReceivedBefore(
                    handle->first,
                    chain->second.sent_ms + config_.query_timeout_ms) &&
                now - chain->second.sent_ms >= config_.query_timeout_ms;
            if (missing || chain_timed_out || page_timed_out) {
                expired.insert(handle->second);
                ClearReceivedQueryHandle(handle->first);
                pending_handles_.erase(handle++);
            } else {
                ++handle;
            }
        }
        for (std::set<uint64_t>::const_iterator id = expired.begin(); id != expired.end();
             ++id) {
            NotifySession(2001, "query timeout");
            CompleteQuery(*id, false);
        }
    }

    void ApplyOrders(OrderSnapshot rows, std::vector<int64_t> row_order) {
        const BookChanges<OrderView> changes = orders_.ReplaceAll(std::move(rows));
        uint64_t listener_epoch = 0;
        const std::shared_ptr<IMarketListener> listener = Listener(&listener_epoch);
        if (!listener) {
            return;
        }
        const bool publish_full = listener_epoch != order_listener_epoch_;
        order_listener_epoch_ = listener_epoch;
        const int64_t callback_started_ms = NowMs();
        InvokeListener("orders snapshot", [&] {
            listener->OnSnapshotBegin(SnapshotKind::kOrders);
            for (size_t i = 0; i < changes.removed.size(); ++i) {
                listener->OnOrderRemoved(changes.removed[i]);
            }
            if (publish_full) {
                for (size_t i = 0; i < row_order.size(); ++i) {
                    const OrderView* row = orders_.Find(row_order[i]);
                    if (row != NULL) {
                        listener->OnOrderUpdate(*row);
                    }
                }
            } else {
                for (size_t i = 0; i < changes.updated.size(); ++i) {
                    listener->OnOrderUpdate(changes.updated[i]);
                }
            }
            listener->OnSnapshotEnd(SnapshotKind::kOrders);
        });
        RecordListenerDuration(callback_started_ms);
    }

    void ApplyPositions(const QueryScope& scope,
                        PositionSnapshot rows,
                        std::vector<std::string> row_order) {
        const BookChanges<PositionView> changes =
            positions_.ReplaceScope(scope, std::move(rows));
        uint64_t listener_epoch = 0;
        const std::shared_ptr<IMarketListener> listener = Listener(&listener_epoch);
        if (!listener) {
            return;
        }
        const bool publish_full = listener_epoch != position_listener_epoch_;
        position_listener_epoch_ = listener_epoch;
        const int64_t callback_started_ms = NowMs();
        InvokeListener("positions snapshot", [&] {
            listener->OnSnapshotBegin(SnapshotKind::kPositions);
            for (size_t i = 0; i < changes.removed.size(); ++i) {
                listener->OnPositionRemoved(changes.removed[i]);
            }
            if (publish_full) {
                for (size_t i = 0; i < row_order.size(); ++i) {
                    const PositionView* row = positions_.Find(row_order[i]);
                    if (row != NULL) {
                        listener->OnPositionUpdate(*row);
                    }
                }
            } else {
                for (size_t i = 0; i < changes.updated.size(); ++i) {
                    listener->OnPositionUpdate(changes.updated[i]);
                }
            }
            listener->OnSnapshotEnd(SnapshotKind::kPositions);
        });
        RecordListenerDuration(callback_started_ms);
    }

    void ApplyAccounts(const QueryScope& scope,
                       AccountSnapshot rows,
                       std::vector<std::string> row_order) {
        const BookChanges<AccountView> changes =
            accounts_.ReplaceScope(RefreshKey(scope), std::move(rows));
        uint64_t listener_epoch = 0;
        const std::shared_ptr<IMarketListener> listener = Listener(&listener_epoch);
        if (!listener) {
            return;
        }
        const bool publish_full = listener_epoch != account_listener_epoch_;
        account_listener_epoch_ = listener_epoch;
        const int64_t callback_started_ms = NowMs();
        InvokeListener("accounts snapshot", [&] {
            listener->OnSnapshotBegin(SnapshotKind::kAccounts);
            for (size_t i = 0; i < changes.removed.size(); ++i) {
                listener->OnAccountRemoved(changes.removed[i]);
            }
            if (publish_full) {
                for (size_t i = 0; i < row_order.size(); ++i) {
                    const AccountView* row = accounts_.Find(row_order[i]);
                    if (row != NULL) {
                        listener->OnAccountUpdate(*row);
                    }
                }
            } else {
                for (size_t i = 0; i < changes.updated.size(); ++i) {
                    listener->OnAccountUpdate(changes.updated[i]);
                }
            }
            listener->OnSnapshotEnd(SnapshotKind::kAccounts);
        });
        RecordListenerDuration(callback_started_ms);
    }

    void HandlePush(const QueuedEvent& event) {
        if (event.payload.empty()) {
            NotifySession(-1, "push payload is empty");
            return;
        }
        IF2UnPacker* unpack = NewUnPacker(
            const_cast<char*>(&event.payload[0]),
            static_cast<unsigned int>(event.payload.size()));
        if (unpack == NULL) {
            NotifySession(-1, "push unpack failed");
            return;
        }
        unpack->AddRef();
        if (unpack->GetDatasetCount() <= 0) {
            unpack->Release();
            NotifySession(-1, "push data set is missing");
            return;
        }
        unpack->SetCurrentDatasetByIndex(0);
        if (unpack->GetRowCount() != 1U) {
            unpack->Release();
            NotifySession(-1, "push must contain exactly one row");
            return;
        }
        unpack->First();
        std::string msgtype;
        if (!t2::FieldStr(unpack, "msgtype", &msgtype)) {
            unpack->Release();
            NotifySession(-1, "push is missing msgtype");
            return;
        }

        if (msgtype == "P") {
            unpack->Release();
            RequestFullReconcile();
            return;
        }
        if (!IsOrderMessage(msgtype)) {
            unpack->Release();
            return;
        }

        if (IsCancellationMessage(msgtype)) {
            HandleCancellationPush(unpack, msgtype);
            unpack->Release();
            return;
        }

        OrderView update;
        std::string error;
        const bool parsed = ParseOrderPush(unpack, msgtype, &update, &error);
        unpack->Release();
        if (!parsed) {
            NotifySession(-1, error.c_str());
            RequestOrderSnapshot(true);
            return;
        }

        bool known_scope = false;
        if (!PushMatchesConfiguredScope(update, &known_scope)) {
            return;
        }
        if (!known_scope) {
            RequestOrderSnapshot();
        } else if (order_snapshot_barrier_) {
            if (buffered_order_pushes_.size() >=
                config_.max_buffered_order_pushes) {
                SignalFatal(-1007, "order snapshot replay buffer limit exceeded");
                return;
            }
            buffered_order_pushes_.push_back(update);
            const size_t buffered = buffered_order_pushes_.size();
            buffered_order_push_count_.store(buffered);
            UpdateAtomicMax(&peak_buffered_order_pushes_, buffered);
        } else {
            ApplyOrderPush(update);
        }

        if (ShouldRefreshBooks(msgtype)) {
            RequestRefresh(ConfiguredScope());
        }
    }

    void HandleCancellationPush(IF2UnPacker* unpack, const std::string& msgtype) {
        int64_t original_entrust_no = 0;
        if (!t2::FieldInt64(unpack, "cancel_entrust_no", &original_entrust_no) ||
            original_entrust_no <= 0) {
            NotifySession(-1, "cancellation push is missing cancel_entrust_no");
            RequestOrderSnapshot();
            return;
        }

        OrderView scope_probe;
        scope_probe.entrust_no = original_entrust_no;
        t2::FieldStr(unpack, "account_code", &scope_probe.account_code);
        t2::FieldStr(unpack, "combi_no", &scope_probe.combi_no);
        bool known_scope = false;
        if (!PushMatchesConfiguredScope(scope_probe, &known_scope)) {
            return;
        }
        RequestOrderSnapshot();
        if (msgtype == "e") {
            RequestRefresh(ConfiguredScope());
        }
    }

    bool ParseOrderPush(IF2UnPacker* unpack, const std::string& msgtype,
                        OrderView* update, std::string* error) {
        update->msgtype = msgtype;
        int64_t business_date = 0;
        const char* date_field = msgtype == "g" ? "deal_date" : "business_date";
        if (!t2::FieldInt64(unpack, date_field, &business_date) || business_date < 0 ||
            business_date > 99999999 ||
            !t2::FieldInt64(unpack, "entrust_no", &update->entrust_no) ||
            update->entrust_no <= 0 ||
            !t2::FieldStr(unpack, "entrust_state", &update->entrust_state)) {
            *error = "order push contains an invalid date, entrust_no, or state";
            return false;
        }
        update->business_date = static_cast<int>(business_date);

        if (msgtype == "c") {
            if (!t2::FieldInt64(unpack, "extsystem_id", &update->extsystem_id)) {
                *error = "c push is missing extsystem_id";
                return false;
            }
            return true;
        }

        if (!t2::FieldStr(unpack, "account_code", &update->account_code) ||
            !t2::FieldStr(unpack, "combi_no", &update->combi_no) ||
            !t2::FieldStr(unpack, "market_no", &update->market_no) ||
            !t2::FieldStr(unpack, "stock_code", &update->stock_code) ||
            !t2::FieldStr(unpack, "entrust_direction", &update->entrust_direction) ||
            !t2::FieldInt64(unpack, "entrust_amount", &update->entrust_amount) ||
            !t2::FieldInt64(unpack, "extsystem_id", &update->extsystem_id)) {
            *error = "order push contains a missing or invalid common field";
            return false;
        }

        if (msgtype == "a" || msgtype == "b") {
            if (!t2::FieldDecimal(unpack, "entrust_price", &update->entrust_price)) {
                *error = "order push contains an invalid entrust_price";
                return false;
            }
            return true;
        }

        if (msgtype == "g") {
            if (!t2::FieldInt64(unpack, "deal_amount", &update->last_fill_amount) ||
                !t2::FieldInt64(unpack, "total_deal_amount", &update->deal_amount) ||
                !t2::FieldInt64(unpack, "cancel_amount", &update->cancel_amount) ||
                !t2::FieldStr(unpack, "deal_no", &update->deal_no) ||
                !t2::FieldInt64(unpack, "realdeal_serial_no",
                                &update->realdeal_serial_no) ||
                update->last_fill_amount <= 0 || update->deal_amount <= 0) {
                *error = "g push contains invalid cumulative deal fields";
                return false;
            }
            return true;
        }

        *error = "unsupported direct order push type";
        return false;
    }

    bool PushMatchesConfiguredScope(const OrderView& update, bool* known_scope) const {
        std::string account = update.account_code;
        std::string asset = update.asset_no;
        std::string combi = update.combi_no;
        const OrderView* existing = orders_.Find(update.entrust_no);
        if (existing != NULL) {
            if (account.empty()) {
                account = existing->account_code;
            }
            if (asset.empty()) {
                asset = existing->asset_no;
            }
            if (combi.empty()) {
                combi = existing->combi_no;
            }
        }

        if (!config_.account_code.empty() && !account.empty() &&
            account != config_.account_code) {
            *known_scope = true;
            return false;
        }
        if (!config_.asset_no.empty() && !asset.empty() && asset != config_.asset_no) {
            *known_scope = true;
            return false;
        }
        if (!config_.combi_no.empty() && !combi.empty() && combi != config_.combi_no) {
            *known_scope = true;
            return false;
        }
        *known_scope = (config_.account_code.empty() || !account.empty()) &&
                       (config_.asset_no.empty() || !asset.empty()) &&
                       (config_.combi_no.empty() || !combi.empty());
        return true;
    }

    void ApplyOrderPush(const OrderView& update) {
        const OrderView* stored = NULL;
        if (!orders_.UpsertFromPush(update, &stored)) {
            return;
        }
        const std::shared_ptr<IMarketListener> listener = Listener(NULL);
        if (listener && stored != NULL) {
            const int64_t callback_started_ms = NowMs();
            InvokeListener("OnOrderUpdate", [&] {
                listener->OnOrderUpdate(*stored);
            });
            RecordListenerDuration(callback_started_ms);
        }
    }

    void ReplayBufferedOrderPushes() {
        std::vector<OrderView> buffered;
        buffered.swap(buffered_order_pushes_);
        buffered_order_push_count_.store(0);
        for (size_t i = 0; i < buffered.size(); ++i) {
            ApplyOrderPush(buffered[i]);
        }
    }

    SessionConfig config_;
    std::shared_ptr<IMarketListener> listener_;
    CConnectionInterface* session_conn_;
    CConnectionInterface* query_conn_;
    CConnectionInterface* heartbeat_conn_;
    CConnectionInterface* mc_conn_;
    CSubscribeInterface* subscriber_;
    ConnectionCallback session_cb_;
    ConnectionCallback query_cb_;
    ConnectionCallback heartbeat_cb_;
    ConnectionCallback mc_conn_cb_;
    McCallback mc_cb_;
    OrderBook orders_;
    PositionBook positions_;
    AccountBook accounts_;
    RefreshCoalescer coalescer_;
    RefreshCoalescer order_coalescer_;
    std::map<uint64_t, QueryChain> queries_;
    std::map<int, uint64_t> pending_handles_;
    std::map<std::string, QueryScope> refresh_scopes_;
    std::vector<OrderView> buffered_order_pushes_;
    std::queue<QueuedEvent> queue_;
    mutable std::mutex data_mu_;
    mutable std::mutex queue_mu_;
    std::condition_variable queue_cv_;
    std::condition_variable queue_space_cv_;
    std::mutex stop_mu_;
    std::condition_variable stop_cv_;
    std::mutex operation_mu_;
    std::mutex join_mu_;
    std::mutex lifecycle_mu_;
    std::thread dispatcher_;
    std::thread heartbeat_;
    std::thread::id dispatcher_id_;
    std::thread::id heartbeat_id_;
    std::string token_;
    std::atomic<bool> running_;
    std::atomic<bool> accepting_events_;
    std::atomic<uint64_t> listener_epoch_;
    std::atomic<bool> listener_reconcile_requested_;
    LifecycleState lifecycle_state_;
    int active_workers_;
    OperationStatus cleanup_status_;
    int subscribe_index_;
    uint64_t next_query_id_;
    uint64_t order_query_id_;
    bool order_snapshot_barrier_;
    uint64_t order_listener_epoch_;
    uint64_t position_listener_epoch_;
    uint64_t account_listener_epoch_;
    int64_t last_reconcile_ms_;
    size_t queued_payload_bytes_;
    size_t reserved_event_count_;
    size_t reserved_payload_bytes_;
    // Page timeout is measured at SDK callback arrival; bounded local queueing
    // must not turn an on-time response into a transport timeout.
    std::map<int, int64_t> received_query_times_;
    uint64_t queue_generation_;
    size_t peak_queued_events_;
    size_t peak_queued_payload_bytes_;
    std::atomic<size_t> buffered_order_push_count_;
    std::atomic<size_t> peak_buffered_order_pushes_;
    bool fatal_pending_;
    int fatal_code_;
    std::string fatal_message_;
    std::atomic<uint64_t> stat_enqueued_events_;
    std::atomic<uint64_t> stat_enqueue_waits_;
    std::atomic<uint64_t> stat_rejected_events_;
    std::atomic<uint64_t> stat_query_chains_started_;
    std::atomic<uint64_t> stat_query_chains_completed_;
    std::atomic<uint64_t> stat_query_chains_failed_;
    std::atomic<uint64_t> stat_query_pages_;
    std::atomic<uint64_t> stat_query_rows_;
    std::atomic<uint64_t> stat_query_payload_bytes_;
    std::atomic<uint64_t> stat_order_snapshot_requests_;
    std::atomic<uint64_t> stat_order_snapshot_coalesced_;
    std::atomic<uint64_t> stat_order_snapshots_started_;
    std::atomic<uint64_t> stat_max_event_queue_delay_ms_;
    std::atomic<uint64_t> stat_max_listener_callback_ms_;
};

Session::Session(const SessionConfig& config) : impl_(new Impl(config)) {}

Session::~Session() { delete impl_; }

void Session::SetListener(const std::shared_ptr<IMarketListener>& listener) {
    impl_->SetListener(listener);
}

int Session::Start() { return impl_->Start(); }

void Session::Stop() { impl_->Stop(); }

std::string Session::UserToken() const { return impl_->UserToken(); }

SessionStats Session::Stats() const { return impl_->Stats(); }

}  // namespace ufx
