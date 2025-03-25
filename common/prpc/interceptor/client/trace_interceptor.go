package client

import (
	"context"

	ptrace "github.com/hardcore-os/plato/common/prpc/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	gcodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TraceUnaryClientInterceptor trace middleware
func TraceUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		md, ok := metadata.FromOutgoingContext(ctx) // 从上下文中获取 gRPC 元数据
		if !ok {                                    // 如果元数据不存在，则创建一个新的元数据
			md = metadata.MD{}
		}

		tr := otel.GetTracerProvider().Tracer(ptrace.TraceName) // 获取 OpenTelemetry 追踪器
		name, attrs := ptrace.BuildSpan(method, "")             // 构建 Span 的名称和属性
		// span 的作用
		// 记录请求信息（如 HTTP 调用、RPC 调用）
		// 计算执行时间（开始时间、结束时间）
		// 传递上下文信息（用于分布式系统，关联多个服务间的调用）
		// 添加自定义的属性信息（如请求方法、参数、状态码等）
		// 支持分布式追踪（让不同服务的请求链路串联起来）
		ctx, span := tr.Start(ctx, name, trace.WithAttributes(attrs...), trace.WithSpanKind(trace.SpanKindClient)) // 创建一个新的 Span
		defer span.End()                                                                                           // 在函数结束时结束 Span

		ptrace.Inject(ctx, otel.GetTextMapPropagator(), &md) // 将追踪上下文注入到 gRPC 元数据中
		ctx = metadata.NewOutgoingContext(ctx, md)           // 使用新的元数据创建新的上下文

		err := invoker(ctx, method, req, reply, cc, opts...) // 调用 gRPC 方法
		if err != nil {                                      // 如果 gRPC 方法返回错误，则设置 Span 的状态和属性
			s, ok := status.FromError(err) // 从错误中获取 gRPC 状态码
			if ok {                        // 如果错误不是 gRPC 错误，则设置 Span 的状态为错误，并设置错误字符串
				span.SetStatus(codes.Error, s.Message())            // 设置 Span 的状态为错误，并设置错误消息。
				span.SetAttributes(ptrace.StatusCodeAttr(s.Code())) // 设置 Span 的 gRPC 状态码属性。
			} else {
				span.SetStatus(codes.Error, err.Error()) // 如果 gRPC 方法返回错误，则设置 Span 的状态和属性
			}
			return err
		}

		span.SetAttributes(ptrace.StatusCodeAttr(gcodes.OK)) // 如果 gRPC 方法成功，则设置 Span 的 gRPC 状态码属性为 OK。
		return nil
	}
}
