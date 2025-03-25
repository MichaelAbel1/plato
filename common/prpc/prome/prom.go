package prome

import "github.com/prometheus/client_golang/prometheus"

// NewCounterVec ...
func NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	counterVec := prometheus.NewCounterVec(opts, labelNames)

	prometheus.MustRegister(counterVec) // 自动注册指标，避免了忘记注册指标的错误

	return counterVec
}

// NewHistogramVec ...
func NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec {
	histogramVec := prometheus.NewHistogramVec(opts, labelNames)

	prometheus.MustRegister(histogramVec)

	return histogramVec
}
