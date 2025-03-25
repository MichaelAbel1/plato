package trace

import (
	"context"
	"sync"

	"github.com/bytedance/gopkg/util/logger"

	"github.com/hardcore-os/plato/common/prpc/config"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

var (
	tp   *tracesdk.TracerProvider // 用于创建追踪器
	once sync.Once                // 用于实现单例模式，确保 StartAgent 函数只被调用一次
)

// StartAgent 开启trace collector
func StartAgent() {
	once.Do(func() { // 确保 StartAgent 函数只被调用一次
		exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(config.GetTraceCollectionUrl()))) // 创建 Jaeger 导出器，并设置 Jaeger Collector 的地址
		if err != nil {
			logger.Errorf("trace start agent err:%s", err.Error())
			return
		}

		tp = tracesdk.NewTracerProvider( // 创建 TracerProvider 实例，并设置采样器、导出器和资源信息
			tracesdk.WithSampler(tracesdk.TraceIDRatioBased(config.GetTraceSampler())), // 设置采样器，根据配置的采样率进行采样
			tracesdk.WithBatcher(exp), // 设置导出器，将追踪数据导出到 Jaeger
			tracesdk.WithResource(resource.NewWithAttributes( // 设置资源信息，包括服务名称等
				semconv.SchemaURL,
				semconv.ServiceNameKey.String(config.GetTraceServiceName()),
			)),
		)

		otel.SetTracerProvider(tp)                                           // 设置全局追踪器提供者
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator( // 设置上下文传播器，用于在不同服务之间传递追踪上下文。
			propagation.TraceContext{}, propagation.Baggage{}))
	})
}

// StopAgent 关闭trace collector,在服务停止时调用StopAgent，不然可能造成trace数据的丢失
func StopAgent() {
	_ = tp.Shutdown(context.TODO())
}
