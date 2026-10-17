# Storage Lock Prometheus

将 storage-lock 事件流接入 Prometheus，提供不包含 lock ID/owner ID 的低基数指标。

## 快速开始

```go
listener := prometheus_listener.NewPrometheusListener(":9090")
options := storage_lock.NewStorageLockOptionsWithLockId("my-lock")
options.EventListeners = append(options.EventListeners, listener)
lock, _ := storage_lock.NewStorageLockWithOptions(storage, options)
go listener.ListenAndServe()
```

访问 `http://localhost:9090/metrics`。需要优雅退出时调用 `listener.Shutdown(ctx)`；也可以把 `listener.Handler()` 挂载到已有 HTTP server。

## 指标

- `storage_lock_events_total{action}`：动作被归一化为有限集合，不会直接暴露用户自定义 action。
- `storage_lock_operations_total{operation,result,storage}`：操作总数。
- `storage_lock_operation_duration_seconds{operation,result,storage}`：事件持续时间。
- `storage_lock_acquire_total{storage,result}`：获取成功、竞争忙或错误次数。
- `storage_lock_release_total{storage,result}`：释放成功或错误次数。
- `storage_lock_watchdog_renew_total{storage,result}`：续租成功或错误次数。
- `storage_lock_hold_seconds{storage}`：从成功事件到释放事件的近似持有时长（直方图）。
- `storage_lock_hold_seconds_quantile{storage}`：持有时长分位数（Summary，0.5/0.9/0.99）。
- `storage_lock_contention_total{storage,kind}`：争抢失败分类计数，`kind ∈ {busy, timeout, not_your_turn, version_miss, error}`。
- `storage_lock_waiters`：当前所有锁上排队等待者总数（事件推导的近似值，不暴露 lock_id）。
- `storage_lock_contended_locks`：至少有一个排队等待者的锁数量（同上）。

所有 label 都是固定维度或由部署配置产生的 storage 名称；**不会使用 lock_id、owner_id**。
`waiters` / `contended_locks` 两个 gauge 由事件携带的 LockInformation 排队人数做差值聚合，
重复事件幂等；事件指标不能证明当前持有锁数量，因此不提供误导性的"当前持有锁" gauge。

```promql
sum(rate(storage_lock_operations_total{operation="lock",result="busy"}[5m])) by (storage)
sum(rate(storage_lock_contention_total{kind="timeout"}[5m])) by (storage)
sum(rate(storage_lock_watchdog_renew_total{result="error"}[5m])) by (storage)
histogram_quantile(0.95, rate(storage_lock_hold_seconds_bucket[5m]))
```

## Web 看板

`RootHandler()`（或 `ListenAndServe()`）在同一端口提供：

- `/metrics`：Prometheus 抓取端点。
- `/dashboard`：零依赖 HTML 看板，浏览器 fetch 轮询 `/metrics`，实时渲染排队深度、
  争抢中的锁、获取冲突、续租失败、成功操作等关键信号，不需要另起 Grafana。

也可以只挂 metrics：`listener.Handler()` 保持纯指标兼容。

每个 listener 使用独立 Registry，多个 listener 不会因为全局重复注册而 panic。`MustRegister` 仍可显式注册到全局 Registry，但多实例调用前应自行避免重复。
