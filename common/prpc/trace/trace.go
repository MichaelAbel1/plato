package trace

import (
	"context"

	sdktrace "go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/metadata"
)

const (
	TraceName = "plato-trace" // 定义追踪名称，用于标识追踪。
)

type metadataSupplier struct {
	metadata *metadata.MD
}

func (s *metadataSupplier) Get(key string) string {
	values := s.metadata.Get(key)
	if len(values) == 0 {
		return ""
	}

	return values[0]
}

func (s *metadataSupplier) Set(key, value string) {
	s.metadata.Set(key, value)
}

// 获取 gRPC 元数据中的所有键
func (s *metadataSupplier) Keys() []string {
	out := make([]string, 0, len(*s.metadata))
	for key := range *s.metadata {
		out = append(out, key)
	}

	return out
}

// Inject set cross-cutting concerns from the Context into the metadata.
// 将 OpenTelemetry 追踪上下文从 ctx 注入到 gRPC 元数据 m 中
func Inject(ctx context.Context, p propagation.TextMapPropagator, m *metadata.MD) {
	p.Inject(ctx, &metadataSupplier{
		metadata: m,
	})
}

// Extract reads cross-cutting concerns from the metadata into a Context.
//
// 从 gRPC 元数据 metadata 中提取 OpenTelemetry 追踪上下文，并将其设置到 ctx 中。
func Extract(ctx context.Context, p propagation.TextMapPropagator, metadata *metadata.MD) sdktrace.SpanContext {
	ctx = p.Extract(ctx, &metadataSupplier{
		metadata: metadata,
	})

	return sdktrace.SpanContextFromContext(ctx) // 从上下文中获取 SpanContext，并返回它
}
