package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	DependencyErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "metroride_dependency_errors_total",
		Help: "Total dependency failures by service and dependency.",
	}, []string{"service", "dependency"})

	StreamConsumeErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "metroride_stream_consume_errors_total",
		Help: "Total Redis Stream consume errors by service and stream.",
	}, []string{"service", "stream"})
)

func RegisterCommon() {
	prometheus.MustRegister(DependencyErrors, StreamConsumeErrors)
}
