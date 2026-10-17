package go_storage_lock_prometheus

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/storage-lock/go-events"
	"github.com/storage-lock/go-storage"
	storage_events "github.com/storage-lock/go-storage-events"
)

// PrometheusListener 把 storage-lock 的事件流翻译成低基数 Prometheus 指标。
// lock_id 和 owner_id 不作为 label，避免业务标识造成无界的指标基数。
type PrometheusListener struct {
	eventCounter *prometheus.CounterVec
	registry     *prometheus.Registry
	addr         string

	acquireCounter       *prometheus.CounterVec
	releaseCounter       *prometheus.CounterVec
	holdTimeHistogram    *prometheus.HistogramVec
	holdQuantileSummary  *prometheus.SummaryVec
	watchdogRenewCounter *prometheus.CounterVec
	operationCounter     *prometheus.CounterVec
	operationDuration    *prometheus.HistogramVec
	contentionCounter    *prometheus.CounterVec
	waitersGauge         prometheus.Gauge
	contendedLocksGauge  prometheus.Gauge

	holdStartTimes sync.Map
	serverMu       sync.Mutex
	server         *http.Server

	// waitersMu 保护 waiterCounts：事件流顺序不确定，按锁 ID 记住上一次看到的
	// 排队人数，用差值更新聚合 gauge，保证重复事件幂等。
	waitersMu    sync.Mutex
	waiterCounts map[string]int
}

var _ events.Listener = &PrometheusListener{}

const PrometheusListenerName = "prometheus-listener"

func NewPrometheusListener(addr string) *PrometheusListener {
	if addr == "" {
		addr = ":9090"
	}
	registry := prometheus.NewRegistry()
	eventCounter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_lock_events_total",
		Help: "The total number of storage-lock events by bounded action name",
	}, []string{"action"})
	acquireCounter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_lock_acquire_total",
		Help: "Total number of lock acquire attempts by result and storage",
	}, []string{"storage", "result"})
	releaseCounter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_lock_release_total",
		Help: "Total number of lock release attempts by result and storage",
	}, []string{"storage", "result"})
	holdTimeHistogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "storage_lock_hold_seconds",
		Help:    "Lock hold time distribution in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"storage"})
	holdQuantileSummary := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "storage_lock_hold_seconds_quantile",
		Help:       "Lock hold time quantiles in seconds (0.5/0.9/0.99)",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"storage"})
	watchdogRenewCounter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_lock_watchdog_renew_total",
		Help: "Total number of watchdog renewal attempts by result and storage",
	}, []string{"storage", "result"})
	operationCounter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_lock_operations_total",
		Help: "Total number of storage-lock operations by operation, result, and storage",
	}, []string{"operation", "result", "storage"})
	operationDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "storage_lock_operation_duration_seconds",
		Help: "Duration of storage-lock operations by operation, result, and storage",
	}, []string{"operation", "result", "storage"})
	contentionCounter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_lock_contention_total",
		Help: "Total number of failed lock attempts by contention kind and storage",
	}, []string{"storage", "kind"})
	waitersGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "storage_lock_waiters",
		Help: "Total number of queued lock waiters across all locks (event-derived approximation)",
	})
	contendedLocksGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "storage_lock_contended_locks",
		Help: "Number of locks that currently have at least one queued waiter (event-derived approximation)",
	})
	registry.MustRegister(eventCounter, acquireCounter, releaseCounter, holdTimeHistogram,
		holdQuantileSummary, watchdogRenewCounter, operationCounter, operationDuration,
		contentionCounter, waitersGauge, contendedLocksGauge)
	return &PrometheusListener{
		eventCounter:         eventCounter,
		registry:             registry,
		addr:                 addr,
		acquireCounter:       acquireCounter,
		releaseCounter:       releaseCounter,
		holdTimeHistogram:    holdTimeHistogram,
		holdQuantileSummary:  holdQuantileSummary,
		watchdogRenewCounter: watchdogRenewCounter,
		operationCounter:     operationCounter,
		operationDuration:    operationDuration,
		contentionCounter:    contentionCounter,
		waitersGauge:         waitersGauge,
		contendedLocksGauge:  contendedLocksGauge,
		waiterCounts:         map[string]int{},
	}
}

func (x *PrometheusListener) Name() string { return PrometheusListenerName }

// On 收集事件。所有用户可控的 action 都先归一化到固定枚举。
func (x *PrometheusListener) On(_ context.Context, e *events.Event) {
	if e == nil {
		return
	}
	storageName := boundedStorage(e.StorageName)
	actions := make([]string, 0, len(e.Actions))
	if len(e.Actions) == 0 {
		actions = append(actions, "unknown")
		x.eventCounter.WithLabelValues("unknown").Inc()
	}
	for _, action := range e.Actions {
		name := "unknown"
		if action != nil && action.Name != "" {
			name = boundedAction(action.Name)
		}
		actions = append(actions, name)
		x.eventCounter.WithLabelValues(name).Inc()
	}
	operation := operationName(e, actions)
	result := resultName(e)
	x.operationCounter.WithLabelValues(operation, result, storageName).Inc()
	if e.StartTime != nil && e.EndTime != nil {
		if duration := e.EndTime.Sub(*e.StartTime); duration >= 0 {
			x.operationDuration.WithLabelValues(operation, result, storageName).Observe(duration.Seconds())
		}
	}

	for _, action := range e.Actions {
		if action == nil {
			continue
		}
		name := "unknown"
		if action.Name != "" {
			name = boundedAction(action.Name)
		}
		x.updateWaiters(storageName, e.LockId, action)
		switch name {
		case "lock-success", "lock":
			x.acquireCounter.WithLabelValues(storageName, "success").Inc()
			if e.LockId != "" {
				x.holdStartTimes.Store(e.LockId, time.Now())
			}
		case "lock-busy":
			x.acquireCounter.WithLabelValues(storageName, "busy").Inc()
			x.contentionCounter.WithLabelValues(storageName, "busy").Inc()
		case "lock-error":
			x.acquireCounter.WithLabelValues(storageName, "error").Inc()
			x.contentionCounter.WithLabelValues(storageName, "error").Inc()
		case "unlock-success", "unlock":
			x.releaseCounter.WithLabelValues(storageName, "success").Inc()
			x.observeHold(e, storageName)
		case "unlock-error":
			x.releaseCounter.WithLabelValues(storageName, "error").Inc()
		case "renew-success":
			x.watchdogRenewCounter.WithLabelValues(storageName, "success").Inc()
		case "renew-error":
			x.watchdogRenewCounter.WithLabelValues(storageName, "error").Inc()
		case "timeout":
			x.contentionCounter.WithLabelValues(storageName, "timeout").Inc()
		case "not-your-turn":
			x.contentionCounter.WithLabelValues(storageName, "not_your_turn").Inc()
		case "version-miss":
			x.contentionCounter.WithLabelValues(storageName, "version_miss").Inc()
		}
	}
}

// updateWaiters 从 action 携带的 LockInformation 提取排队人数，按锁聚合更新
// 两个低基数 gauge（不暴露 lock_id）。同一记录被多个事件重复携带时幂等。
func (x *PrometheusListener) updateWaiters(_ string, lockId string, action *events.Action) {
	if lockId == "" {
		return
	}
	info := lockInfoFromAction(action)
	if info == nil {
		return
	}
	x.waitersMu.Lock()
	defer x.waitersMu.Unlock()
	old := x.waiterCounts[lockId]
	newCount := len(info.Waiters)
	if newCount == old {
		return
	}
	x.waiterCounts[lockId] = newCount
	x.waitersGauge.Add(float64(newCount - old))
	if old == 0 && newCount > 0 {
		x.contendedLocksGauge.Inc()
	} else if old > 0 && newCount == 0 {
		x.contendedLocksGauge.Dec()
	}
}

// lockInfoFromAction 从 action 的 payload 里取出 *storage.LockInformation。
func lockInfoFromAction(action *events.Action) *storage.LockInformation {
	if action == nil || action.PayloadMap == nil {
		return nil
	}
	if v, ok := action.GetPayload(storage_events.PayloadLockInformation); ok {
		if info, ok := v.(*storage.LockInformation); ok {
			return info
		}
	}
	return nil
}

func (x *PrometheusListener) observeHold(e *events.Event, storageName string) {
	if e.LockId == "" {
		return
	}
	if start, ok := x.holdStartTimes.LoadAndDelete(e.LockId); ok {
		duration := time.Since(start.(time.Time)).Seconds()
		x.holdTimeHistogram.WithLabelValues(storageName).Observe(duration)
		x.holdQuantileSummary.WithLabelValues(storageName).Observe(duration)
	}
}

// Handler 返回可挂载到任意 HTTP server 的 metrics handler。
func (x *PrometheusListener) Handler() http.Handler {
	return promhttp.HandlerFor(x.registry, promhttp.HandlerOpts{})
}

func (x *PrometheusListener) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	x.Handler().ServeHTTP(w, r)
}

// ListenAndServe 在 x.addr 暴露 /metrics 与 /dashboard；保留原有 API。
func (x *PrometheusListener) ListenAndServe() error {
	server := &http.Server{Addr: x.addr, Handler: x.RootHandler()}
	x.serverMu.Lock()
	x.server = server
	x.serverMu.Unlock()
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// RootHandler 返回同时挂载 /metrics 与 /dashboard 的 handler。
func (x *PrometheusListener) RootHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", x.Handler())
	mux.Handle("/dashboard", x.DashboardHandler())
	return mux
}

// Shutdown 优雅停止由 ListenAndServe 创建的 HTTP server。
func (x *PrometheusListener) Shutdown(ctx context.Context) error {
	x.serverMu.Lock()
	server := x.server
	x.serverMu.Unlock()
	if server == nil {
		return nil
	}
	return server.Shutdown(ctx)
}

func operationName(e *events.Event, actions []string) string {
	for _, action := range actions {
		switch action {
		case "renew-success", "renew-error":
			return "renew"
		case "unlock-success", "unlock-error":
			return "unlock"
		case "lock-success", "lock-busy", "lock-error":
			return "lock"
		}
	}
	switch e.EventType {
	case events.EventTypeLock:
		return "lock"
	case events.EventTypeUnlock:
		return "unlock"
	case events.EventTypeWatchDog, events.EventTypeRenewed:
		return "renew"
	case events.EventTypeExpired:
		return "expired"
	case events.EventTypeBusy:
		return "busy"
	case events.EventTypeLost:
		return "lost"
	case events.EventTypeWaitChanged:
		return "wait_changed"
	default:
		return "unknown"
	}
}

func resultName(e *events.Event) string {
	if e.Err != nil {
		return "error"
	}
	for _, action := range e.Actions {
		if action != nil && action.Err != nil {
			return "error"
		}
	}
	return "success"
}

func boundedAction(name string) string {
	lower := strings.ToLower(name)
	// 保留最常见的旧 action 名，兼容已有消费者；其余 action 归并到固定枚举。
	if lower == "lock" || lower == "unlock" {
		return lower
	}
	switch {
	case strings.Contains(lower, "renew") || strings.Contains(lower, "refresh"):
		if strings.Contains(lower, "error") {
			return "renew-error"
		}
		return "renew-success"
	case strings.Contains(lower, "unlock"):
		if strings.Contains(lower, "error") {
			return "unlock-error"
		}
		return "unlock-success"
	case strings.Contains(lower, "lock"):
		if strings.Contains(lower, "busy") {
			return "lock-busy"
		}
		if strings.Contains(lower, "error") {
			return "lock-error"
		}
		if strings.Contains(lower, "success") {
			return "lock-success"
		}
		return "lock"
	case strings.Contains(lower, "timeout"):
		return "timeout"
	case strings.Contains(lower, "not-your-turn") || strings.Contains(lower, "notyour-turn"):
		return "not-your-turn"
	case strings.Contains(lower, "versionmiss") || strings.Contains(lower, "version-miss"):
		return "version-miss"
	default:
		return "other"
	}
}

func boundedStorage(name string) string {
	if name == "" {
		return "unknown"
	}
	return name
}

// MustRegister 将指标注册到全局默认 registry；多实例调用可能重复注册。
func (x *PrometheusListener) MustRegister() {
	prometheus.DefaultRegisterer.MustRegister(
		x.eventCounter,
		x.acquireCounter,
		x.releaseCounter,
		x.holdTimeHistogram,
		x.holdQuantileSummary,
		x.watchdogRenewCounter,
		x.operationCounter,
		x.operationDuration,
		x.contentionCounter,
		x.waitersGauge,
		x.contendedLocksGauge,
	)
}
