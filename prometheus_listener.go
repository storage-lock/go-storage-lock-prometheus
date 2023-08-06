package go_storage_lock_prometheus

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/storage-lock/go-events"
)

type PrometheusListener struct {
	countMap map[string]prometheus.Counter
}

var _ events.Listener = &PrometheusListener{}

func (x *PrometheusListener) ListenAndServe() {
	promhttp.Handler()
}

func (x *PrometheusListener) Name() string {
	//TODO implement me
	panic("implement me")

	opsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "myapp_processed_ops_total",
		Help: "The total number of processed events",
	})

	promauto.NewGauge()

}

func (x *PrometheusListener) On(ctx context.Context, e *events.Event) {
	//TODO implement me
	panic("implement me")

}
