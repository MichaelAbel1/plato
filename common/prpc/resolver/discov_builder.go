package resolver

import (
	"context"
	"fmt"

	"github.com/hardcore-os/plato/common/prpc/discov"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
)

// 封装了服务发现组件
type DiscovBuilder struct {
	discov discov.Discovery // 用来 查询服务列表 并 监听服务变化
}

// NewDiscovBuilder ...
// 工厂函数，用于创建 DiscovBuilder 实例
func NewDiscovBuilder(d discov.Discovery) resolver.Builder {
	return &DiscovBuilder{
		discov: d,
	}
}

// 返回 解析器的 Scheme，它决定 gRPC 客户端如何匹配 URL 前缀
func (d *DiscovBuilder) Scheme() string {
	return DiscovBuilderScheme
}

func (d *DiscovBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	d.discov.GetService(context.TODO(), d.getServiceName(target)) // 解析 gRPC 目标地址
	serviceName := d.getServiceName(target)                       // 提取 target 里的服务名称
	listener := func() {
		service := d.discov.GetService(context.TODO(), serviceName) //  获取服务的可用节点
		var addrs []resolver.Address
		// 读取 IP、Port、Weight（权重）。
		// 构造 resolver.Address，并 附加权重信息（用于负载均衡）
		for _, item := range service.Endpoints {
			attr := attributes.New("weight", item.Weight)
			addr := resolver.Address{
				Addr:       fmt.Sprintf("%s:%d", item.IP, item.Port),
				Attributes: attr,
			}

			addrs = append(addrs, addr)
		}

		cc.UpdateState(resolver.State{ // 让 gRPC 客户端使用新的地址列表
			Addresses: addrs,
		})
	}

	d.discov.AddListener(context.TODO(), listener) // 让 listener() 在服务发现变化时触发让 listener() 在 服务发现变化时触发
	listener()                                     // 立即执行一次，让 gRPC 解析器立刻获取最新的服务地址

	return d, nil
}

// 用于查询服务发现
func (d *DiscovBuilder) getServiceName(target resolver.Target) string {
	return target.Endpoint
}

func (d *DiscovBuilder) Close() {
}

func (d *DiscovBuilder) ResolveNow(options resolver.ResolveNowOptions) {
}
