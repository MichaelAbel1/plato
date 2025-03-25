package client

import (
	"context"
	"time"

	"github.com/bytedance/gopkg/util/logger"
	"github.com/sony/gobreaker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BreakerUnaryClientInterceptor 熔断器，配置其实都可以考虑用option选项模式实现，等待有人缘人优化吧
func BreakerUnaryClientInterceptor(name string, maxRequest uint32, interval, timeout time.Duration, readyToTrip func(counts gobreaker.Counts) bool) grpc.UnaryClientInterceptor {
	// var cb *gobreaker.CircuitBreaker
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        name,        // 熔断器名称
		MaxRequests: maxRequest,  // 允许通过的最大请求数
		Interval:    interval,    // 熔断器重置间隔时间
		ReadyToTrip: readyToTrip, // 触发熔断的条件
		IsSuccessful: func(err error) bool { // 判断请求是否成功的函数，根据 gRPC 状态码判断
			switch status.Code(err) {
			case codes.DeadlineExceeded, codes.Internal, codes.Unavailable, codes.DataLoss, codes.Unimplemented:
				return false
			default:
				return true
			}
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) { // 熔断器状态变化时的回调函数，用于日志记录
			logger.Errorf("name:%s,old state:%s,new state:%s", name, from, to)
		},
	})

	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		_, err := cb.Execute(func() (interface{}, error) { // 执行 gRPC 请求，并使用熔断器进行保护
			err := invoker(ctx, method, req, reply, cc, opts...)
			return nil, err
		})

		return err
	}
}
