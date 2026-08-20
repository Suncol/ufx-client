# ufx_client

C++ 客户端：用回调输出 **订单状态、持仓（可卖/不可卖）、账户资金**。

第一期只覆盖沪深/北交所现货。

## 数据从哪来

客户端从 `ufx_topic` 中只消费委托生命周期消息（`a`–`g`）和清算完成消息
`P`；同一主题里的其他消息会被明确忽略。持仓和可用金仍以查询结果为准。

| 回调 | 触发 | 数字来源 |
|------|------|----------|
| `OnOrderUpdate` | 主推或完整 32001 快照发生实际变化 | `g.total_deal_amount` 是累计成交量；重复/乱序成交不会重复累加 |
| `OnPositionUpdate` | 主推 `a/c/e/g/P` 后约 100ms 合并查询 | **只信 `31001`** |
| `OnAccountUpdate` | 同上 | **只信 `34001`** |

订阅成功后才启动初始快照。32001 快照进行期间，订单主推先缓冲；所有分页完成并
原子替换账本后再重放。31001/32001 都会沿 `position_str` 查询到最后一页，任一页
失败都不会用不完整结果覆盖账本。

撤单消息 `d/e/f` 的 `entrust_no` 是撤单委托号，因此客户端不会把它写到原委托；
它使用 `cancel_entrust_no` 识别范围并触发 32001 对账。32001 的撤单数量读取
`withdraw_amount`。

不是「定时查仓 + 中间用订单自己加减」。30–45 秒全量是对账兜底。

可卖 = `enable_amount`；不可卖 = `current_amount - enable_amount`（T+1 锁定等）。

## 连接

- `t2sdk.ini` → `as_ufx`（登录/心跳/查询）
- `subscriber.ini` → `ar_mc`（订阅）
- 查询走独立连接，异步 `SendBizMsg`，用 `hSend` 和查询代际对回包
- SDK 回调里只拷贝包体入队，不在回调线程调 UFX
- 队列同时限制事件数和 payload 字节；复制 SDK 包体前先预留容量
- 队列满时最多等待 `enqueue_timeout_ms`；超时后会明确停止会话，不静默丢弃
- 查询链限制总页数、总行数、总字节和总时长，超限时保留原账本
- 单页超时按 SDK 回调到达时刻判断；响应准时到达后在本地有界队列中的等待不算网络超时

32001 使用独立 single-flight 合并器。查询期间的新对账请求只记录为 dirty，当前
查询完成后经过 `coalesce_window_ms` 再启动一轮，不会在持续主推下无间隔循环查询。
组合或资产范围能够由现有订单确定时，主推直接更新订单；只有范围未知时才请求
32001 对账。

## 容量与统计

`SessionConfig` 的安全上限均可由部署显式覆盖：

| 配置 | 默认值 | 含义 |
|------|-------:|------|
| `max_event_queue_size` | 10000 | 排队中及正在复制的事件数上限 |
| `max_event_queue_bytes` | 64 MiB | 排队中及正在复制的 payload 字节上限 |
| `enqueue_timeout_ms` | 10000 | SDK 回调等待队列容量的最长时间 |
| `query_chain_timeout_ms` | 60000 | 一条完整分页查询链的总时限 |
| `max_query_pages` | 1000 | 单条查询链总页数上限 |
| `max_query_rows` | 1000000 | 单条查询链总行数上限 |
| `max_query_bytes` | 512 MiB | 单条查询链累计响应 payload 上限 |
| `max_buffered_order_pushes` | 100000 | 订单快照屏障期间的主推重放上限 |

任何上限触发后都不会提交不完整快照。调用 `Session::Stats()` 可以读取队列高水位、
查询页数/行数/字节数、32001 合并比例、主推重放高水位、最大排队延迟和最大
listener 回调耗时；累计计数覆盖该 `Session` 对象的整个生命周期。

## 构建

不连柜台，先跑本地单测（无需 T2SDK）：

```bash
cd cpp/ufx_client
make test
```

连现场（Linux x64，需许可证和两个端口）：

```bash
cp t2sdk.ini.example t2sdk.ini
cp subscriber.ini.example subscriber.ini
# 填 IP/端口/license.dat，subscriber 不要过滤 msgtype

make demo T2SDK_ROOT=../../T2SDK_第三方版本/c++
./ufx_demo --op 1000 --pwd 0 --account 你的账户 --combi 你的组合 --auth 授权码
```

或：

```bash
cmake -S . -B build -DUFX_WITH_T2SDK=ON
cmake --build build
```

## 业务回调

用 `std::shared_ptr` 注册 `ufx::IMarketListener`。快照通过
`OnSnapshotBegin/End` 划定边界，并通过 `OnOrderRemoved`、
`OnPositionRemoved`、`OnAccountRemoved` 显式发布删除。
运行期回调在 dispatcher 线程串行执行；`Start/Stop` 自身的同步事件在调用线程报告。
运行中替换 listener 时，新 listener 在完整订单快照成功完成前不会收到订单增量；
内部账本仍会处理这些增量。
回调里可以调用 `Stop()`，但不能在回调尚未返回时销毁 `Session` 本身。
listener 抛出的异常不会越过 Session 的线程或 API 边界；运行期任一回调抛出普通
异常时，会话会以内部错误码 `-1009` 明确停止，`std::bad_alloc` 仍使用已有的内存
错误码 `-1006`。如果抛出异常的是 `OnSessionEvent` 本身，则该 listener 无法再接收
对应的错误通知。快照回调在异常点立即中断，不再尝试调用同一 listener 的
`OnSnapshotEnd`。

数量字段使用 `int64_t`。价格为四位定点 `ufx::Price`，资金为两位定点
`ufx::Money`；使用 `ScaledValue()` 或 `ToString()`，不再经由二进制浮点数做对账。

34001 返回账户/资产单元资金，不返回组合号，所以 `AccountView` 只包含
`account_code` 和 `asset_no`。查询范围可由 `SessionConfig::asset_no` 或
`SessionConfig::combi_no` 指定，非空的可选字段才会被打包。

下单前精确可卖请另调 `31017`（本阶段未封装）。期货/期权未做。
