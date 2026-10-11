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
- `storage_lock_hold_seconds{storage}`：从成功事件到释放事件的近似持有时长。

所有 label 都是固定维度或由部署配置产生的 storage 名称；**不会使用 lock_id、owner_id**。事件指标不能证明当前持有锁数量，因此不提供误导性的当前状态 gauge。

```promql
sum(rate(storage_lock_operations_total{operation="lock",result="busy"}[5m])) by (storage)
sum(rate(storage_lock_watchdog_renew_total{result="error"}[5m])) by (storage)
histogram_quantile(0.95, rate(storage_lock_hold_seconds_bucket[5m]))
```

每个 listener 使用独立 Registry，多个 listener 不会因为全局重复注册而 panic。`MustRegister` 仍可显式注册到全局 Registry，但多实例调用前应自行避免重复。
