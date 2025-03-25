package client

import (
	"context"
	"time"

	"github.com/bytedance/gopkg/util/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// TimeoutUnaryClientInterceptor ...
func TimeoutUnaryClientInterceptor(timeout time.Duration, slowThreshold time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		now := time.Now()
		// 若无自定义超时设置，默认设置超时
		_, ok := ctx.Deadline() // 检查上下文是否已设置超时
		if !ok {                // 如果上下文未设置超时，则设置默认超时
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		var p peer.Peer // 声明一个 peer.Peer 变量，用于获取客户端地址

		err := invoker(ctx, method, req, reply, cc, append(opts, grpc.Peer(&p))...) // 调用 gRPC 方法，并将 grpc.Peer 选项添加到调用选项中，以便获取客户端地址。

		du := time.Since(now) // 计算请求耗时
		remoteIP := ""
		if p.Addr != nil {
			remoteIP = p.Addr.String()
		}

		if slowThreshold > time.Duration(0) && du > slowThreshold { // 如果设置了慢日志阈值，并且请求耗时超过阈值，则记录慢日志
			logger.CtxErrorf(ctx, "grpc slowlog:method%s,tagert:%s,cost:%v,remotIP:%s", method, cc.Target(), du, remoteIP)
		}
		return err
	}
}
