package go_storage_lock_prometheus

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/storage-lock/go-events"
	"github.com/storage-lock/go-storage"
	storage_events "github.com/storage-lock/go-storage-events"
)

// TestWaitersGauge 验证排队深度聚合：按锁维护上次看到的排队人数，
// 用差值更新总数与争抢锁数量，重复事件幂等。
func TestWaitersGauge(t *testing.T) {
	listener := NewPrometheusListener("")

	info2 := &storage.LockInformation{
		LockId:  "wait-lock",
		Waiters: []storage.LockWaiter{{OwnerId: "a", Mode: "write"}, {OwnerId: "b", Mode: "write"}},
	}
	busy := events.NewEvent("wait-lock").
		AddAction(events.NewAction("StorageLock.Lock.Begin.Busy").AddPayload(storage_events.PayloadLockInformation, info2))

	listener.On(context.Background(), busy)
	if got := testutil.ToFloat64(listener.waitersGauge); got != 2 {
		t.Fatalf("waiters gauge = %v, want 2", got)
	}
	if got := testutil.ToFloat64(listener.contendedLocksGauge); got != 1 {
		t.Fatalf("contended locks gauge = %v, want 1", got)
	}

	// 同一记录再次出现（多次 fork 事件）不应重复计数。
	listener.On(context.Background(), busy)
	if got := testutil.ToFloat64(listener.waitersGauge); got != 2 {
		t.Fatalf("waiters gauge after duplicate = %v, want 2", got)
	}

	// 排队人数下降为 1。
	info1 := &storage.LockInformation{LockId: "wait-lock", Waiters: []storage.LockWaiter{{OwnerId: "b", Mode: "write"}}}
	listener.On(context.Background(), events.NewEvent("wait-lock").
		AddAction(events.NewAction("StorageLock.Lock.Expired").AddPayload(storage_events.PayloadLockInformation, info1)))
	if got := testutil.ToFloat64(listener.waitersGauge); got != 1 {
		t.Fatalf("waiters gauge = %v, want 1", got)
	}
	if got := testutil.ToFloat64(listener.contendedLocksGauge); got != 1 {
		t.Fatalf("contended locks gauge = %v, want 1", got)
	}

	// 无人排队后清空。
	info0 := &storage.LockInformation{LockId: "wait-lock"}
	listener.On(context.Background(), events.NewEvent("wait-lock").
		AddAction(events.NewAction("StorageLock.Unlock.Success").AddPayload(storage_events.PayloadLockInformation, info0)))
	if got := testutil.ToFloat64(listener.waitersGauge); got != 0 {
		t.Fatalf("waiters gauge = %v, want 0", got)
	}
	if got := testutil.ToFloat64(listener.contendedLocksGauge); got != 0 {
		t.Fatalf("contended locks gauge = %v, want 0", got)
	}
}

// TestContentionKinds 验证争抢失败按类别计数。
func TestContentionKinds(t *testing.T) {
	listener := NewPrometheusListener("")
	ctx := context.Background()

	listener.On(ctx, events.NewEvent("a").AddActionByName("StorageLock.Lock.Begin.Busy"))
	listener.On(ctx, events.NewEvent("b").AddActionByName("StorageLock.RLock.Busy"))
	listener.On(ctx, events.NewEvent("c").AddActionByName("timeout"))
	listener.On(ctx, events.NewEvent("d").AddActionByName("not-lock-owner"))

	if got := testutil.ToFloat64(listener.contentionCounter.WithLabelValues("unknown", "busy")); got != 2 {
		t.Fatalf("contention busy = %v, want 2", got)
	}
	if got := testutil.ToFloat64(listener.contentionCounter.WithLabelValues("unknown", "timeout")); got != 1 {
		t.Fatalf("contention timeout = %v, want 1", got)
	}
	// not-lock-owner 不归入争抢失败的已知类别，也不应计入 busy。
	if got := testutil.ToFloat64(listener.contentionCounter.WithLabelValues("unknown", "busy")); got != 2 {
		t.Fatalf("not-lock-owner should not count as busy, got %v", got)
	}
}

// TestHoldQuantile 验证持有时长分位数被观察到。
func TestHoldQuantile(t *testing.T) {
	listener := NewPrometheusListener("")
	ctx := context.Background()

	listener.On(ctx, events.NewEvent("q-lock").AddActionByName("lock"))
	time.Sleep(20 * time.Millisecond)
	listener.On(ctx, events.NewEvent("q-lock").AddActionByName("unlock"))

	// SummaryVec 记录了样本后 count>0，通过抓取文本确认指标出现。
	response := httptest.NewRecorder()
	listener.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(response.Body)
	if !strings.Contains(string(body), "storage_lock_hold_seconds_quantile_count") {
		t.Fatalf("quantile summary not exposed:\n%s", body)
	}
}

// TestDashboardEndpoint 验证 /dashboard 与 /metrics 由 RootHandler 一起提供。
func TestDashboardEndpoint(t *testing.T) {
	listener := NewPrometheusListener("")
	handler := listener.RootHandler()

	metricServer := httptest.NewServer(handler)
	defer metricServer.Close()

	resp, err := metricServer.Client().Get(metricServer.URL + "/dashboard")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "storage-lock 看板") {
		t.Fatalf("dashboard HTML not served")
	}

	resp2, err := metricServer.Client().Get(metricServer.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	body2, _ := io.ReadAll(resp2.Body)
	if !strings.Contains(string(body2), "storage_lock_waiters") {
		t.Fatalf("metrics not served by RootHandler")
	}
}