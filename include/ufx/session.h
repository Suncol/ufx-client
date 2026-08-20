#ifndef UFX_SESSION_H
#define UFX_SESSION_H

#include "ufx/listener.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace ufx {

struct SessionConfig {
    std::string t2sdk_ini;
    std::string subscriber_ini;
    std::string operator_no;
    std::string password;
    std::string authorization_id;
    std::string account_code;
    std::string asset_no;
    std::string combi_no;
    int heartbeat_interval_ms;
    int coalesce_window_ms;
    int reconcile_interval_ms;
    int connect_timeout_ms;
    int query_timeout_ms;
    int query_page_size;
    size_t max_event_queue_size;
    size_t max_event_queue_bytes;
    size_t max_dispatch_batch_size;
    int enqueue_timeout_ms;
    int query_chain_timeout_ms;
    size_t max_query_pages;
    size_t max_query_rows;
    size_t max_query_bytes;
    size_t max_buffered_order_pushes;

    SessionConfig()
        : authorization_id("1"),
          heartbeat_interval_ms(120000),
          coalesce_window_ms(100),
          reconcile_interval_ms(45000),
          connect_timeout_ms(5000),
          query_timeout_ms(10000),
          query_page_size(10000),
          max_event_queue_size(10000),
          max_event_queue_bytes(64U * 1024U * 1024U),
          max_dispatch_batch_size(256),
          enqueue_timeout_ms(10000),
          query_chain_timeout_ms(60000),
          max_query_pages(1000),
          max_query_rows(1000000),
          max_query_bytes(512U * 1024U * 1024U),
          max_buffered_order_pushes(100000) {}
};

struct SessionStats {
    size_t queued_events;
    size_t queued_payload_bytes;
    size_t peak_queued_events;
    size_t peak_queued_payload_bytes;
    size_t buffered_order_pushes;
    size_t peak_buffered_order_pushes;
    uint64_t enqueued_events;
    uint64_t enqueue_waits;
    uint64_t rejected_events;
    uint64_t query_chains_started;
    uint64_t query_chains_completed;
    uint64_t query_chains_failed;
    uint64_t query_pages;
    uint64_t query_rows;
    uint64_t query_payload_bytes;
    uint64_t order_snapshot_requests;
    uint64_t order_snapshot_coalesced;
    uint64_t order_snapshots_started;
    uint64_t max_event_queue_delay_ms;
    uint64_t max_listener_callback_ms;

    SessionStats()
        : queued_events(0),
          queued_payload_bytes(0),
          peak_queued_events(0),
          peak_queued_payload_bytes(0),
          buffered_order_pushes(0),
          peak_buffered_order_pushes(0),
          enqueued_events(0),
          enqueue_waits(0),
          rejected_events(0),
          query_chains_started(0),
          query_chains_completed(0),
          query_chains_failed(0),
          query_pages(0),
          query_rows(0),
          query_payload_bytes(0),
          order_snapshot_requests(0),
          order_snapshot_coalesced(0),
          order_snapshots_started(0),
          max_event_queue_delay_ms(0),
          max_listener_callback_ms(0) {}
};

class Session {
public:
    explicit Session(const SessionConfig& config);
    ~Session();

    // A shared listener remains alive through any callback already in progress.
    // Runtime callbacks are serialized on the dispatcher thread. Start/Stop may
    // report their own synchronous result on the calling thread.
    void SetListener(const std::shared_ptr<IMarketListener>& listener);
    int Start();
    void Stop();
    std::string UserToken() const;
    SessionStats Stats() const;

private:
    Session(const Session&);
    Session& operator=(const Session&);

    class Impl;
    Impl* impl_;
};

}  // namespace ufx

#endif
