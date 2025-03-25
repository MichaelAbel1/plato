package client

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"time"

	"github.com/hardcore-os/plato/common/prpc/prome"
	"github.com/hardcore-os/plato/common/prpc/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const nameSpace = "prpc_client" // 定义 Prometheus 指标的命名空间

var (
	clientHandleCounter = prome.NewCounterVec( // 定义一个计数器向量，用于统计 gRPC 客户端请求的总数
		prometheus.CounterOpts{
			Namespace: nameSpace,
			Subsystem: "req",
			Name:      "client_handle_total",
		},
		[]string{"method", "server", "code", "ip"},
	)

	clientHandleHistogram = prome.NewHistogramVec( // 定义一个直方图向量，用于统计 gRPC 客户端请求的耗时
		prometheus.HistogramOpts{
			Namespace: nameSpace,
			Subsystem: "req",
			Name:      "client_handle_seconds",
		},
		[]string{"method", "server", "ip"},
	)
)

// MetricUnaryClientInterceptor ...
// 返回一个 gRPC 客户端的 unary 拦截器
func MetricUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		beg := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...) // 调用 gRPC 方法

		code := status.Code(err)
		clientHandleCounter.WithLabelValues(method, cc.Target(), code.String(), util.ExternalIP()).Inc()
		clientHandleHistogram.WithLabelValues(method, cc.Target(), util.ExternalIP()).Observe(time.Since(beg).Seconds())

		return err
	}
}
