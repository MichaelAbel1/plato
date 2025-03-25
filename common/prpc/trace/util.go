package trace

import (
	"context"
	"net"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"google.golang.org/grpc/peer"
)

const (
	localhost = "127.0.0.1"
)

// BuildSpan returns the span info.
func BuildSpan(method, peerAddr string) (string, []attribute.KeyValue) {
	attrs := make([]attribute.KeyValue, 0)
	name, mAttrs := parseServiceAndMethod(method) // 解析 gRPC 方法名称，获取服务名和方法名，并返回属性。
	attrs = append(attrs, mAttrs...)
	attrs = append(attrs, peerAttr(peerAddr)...) // 获取客户端地址的属性
	return name, attrs
}

// parseServiceAndMethod ...
func parseServiceAndMethod(fullMethod string) (string, []attribute.KeyValue) {
	name := strings.TrimLeft(fullMethod, "/") // 去除方法名称开头的 /
	parts := strings.SplitN(name, "/", 2)     // 将方法名称按 / 分割成服务名和方法名
	if len(parts) != 2 {
		return name, []attribute.KeyValue(nil)
	}

	var attrs []attribute.KeyValue
	if service := parts[0]; service != "" {
		attrs = append(attrs, semconv.RPCServiceKey.String(service))
	}
	if method := parts[1]; method != "" {
		attrs = append(attrs, semconv.RPCMethodKey.String(method))
	}

	return name, attrs
}

// peerAttr returns the peer attributes.
func peerAttr(addr string) []attribute.KeyValue {
	host, port, err := net.SplitHostPort(addr) // 将地址分割成主机名和端口。
	if err != nil {
		return nil
	}

	if len(host) == 0 {
		host = localhost
	}

	return []attribute.KeyValue{
		semconv.NetPeerIPKey.String(host),
		semconv.NetPeerPortKey.String(port),
	}
}

// PeerFromCtx returns the peer from ctx.
func PeerFromCtx(ctx context.Context) string {
	p, ok := peer.FromContext(ctx) // 从上下文中获取 peer 信息。
	if !ok || p == nil {
		return ""
	}

	return p.Addr.String() // 返回客户端地址的字符串表示
}
