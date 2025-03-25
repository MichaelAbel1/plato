package prpc

import (
	"context"
	"fmt"
	"time"

	"github.com/hardcore-os/plato/common/prpc/discov/plugin"

	"google.golang.org/grpc/resolver"

	"github.com/hardcore-os/plato/common/prpc/discov"
	clientinterceptor "github.com/hardcore-os/plato/common/prpc/interceptor/client"
	presolver "github.com/hardcore-os/plato/common/prpc/resolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
)

const (
	dialTimeout = 5 * time.Second
)

type PClient struct {
	serviceName  string                        // 服务名称
	d            discov.Discovery              // 服务发现客户端
	interceptors []grpc.UnaryClientInterceptor // 拦截器列表
	conn         *grpc.ClientConn              // gRPC 连接
}

// NewPClient ...
func NewPClient(serviceName string, interceptors ...grpc.UnaryClientInterceptor) (*PClient, error) {
	p := &PClient{
		serviceName:  serviceName,
		interceptors: interceptors,
	}

	if p.d == nil { // 如果未指定服务发现客户端，则从插件中获取默认实例
		dis, err := plugin.GetDiscovInstance()
		if err != nil {
			panic(err)
		}

		p.d = dis
	}

	resolver.Register(presolver.NewDiscovBuilder(p.d)) // 注册自定义的解析器，用于服务发现。

	conn, err := p.dial() // 建立 gRPC 连接
	p.conn = conn

	return p, err
}

// Conn return *grpc.ClientConn
func (p *PClient) Conn() *grpc.ClientConn {
	return p.conn
}

func (p *PClient) dial() (*grpc.ClientConn, error) {
	svcCfg := fmt.Sprintf(`{"loadBalancingPolicy":"%s"}`, roundrobin.Name) // 该字符串表示 gRPC 的服务配置，其中 loadBalancingPolicy 字段被设置为 roundrobin.Name
	balancerOpt := grpc.WithDefaultServiceConfig(svcCfg)                   // 设置轮询负载均衡策略的 gRPC 连接选项

	interceptors := []grpc.UnaryClientInterceptor{
		clientinterceptor.TraceUnaryClientInterceptor(),  // 用于跟踪 gRPC 请求
		clientinterceptor.MetricUnaryClientInterceptor(), // 用于收集 gRPC 请求的指标。
	}
	interceptors = append(interceptors, p.interceptors...) // 将用户自定义拦截器追加到 interceptors 切片中。

	options := []grpc.DialOption{ // 用于存储 gRPC 连接的选项
		balancerOpt, // 之前创建的轮询负载均衡选项
		grpc.WithChainUnaryInterceptor(interceptors...), // 使用 grpc.WithChainUnaryInterceptor 函数创建一个拦截器链，将所有拦截器链接起来。
		grpc.WithInsecure(),                             // 创建一个不安全的连接选项（不使用 TLS）
	}

	ctx, _ := context.WithTimeout(context.Background(), dialTimeout) // 创建一个带有超时时间的 context.Context

	return grpc.DialContext(ctx, fmt.Sprintf("discov:///%v", p.serviceName), options...) // 调用 grpc.DialContext 函数，创建一个 gRPC 连接，并返回该连接和可能的错误。
}

func (p *PClient) DialByEndPoint(adrss string) (*grpc.ClientConn, error) {
	interceptors := []grpc.UnaryClientInterceptor{
		clientinterceptor.TraceUnaryClientInterceptor(),  // 用于跟踪 gRPC 请求
		clientinterceptor.MetricUnaryClientInterceptor(), // 用于收集 gRPC 请求的指标。
	}
	interceptors = append(interceptors, p.interceptors...)

	options := []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(interceptors...),
		grpc.WithInsecure(),
	}

	ctx, _ := context.WithTimeout(context.Background(), dialTimeout)
	return grpc.DialContext(ctx, adrss, options...)
}
