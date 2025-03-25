package prpc

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bytedance/gopkg/util/logger"
	"github.com/hardcore-os/plato/common/prpc/discov"
	"github.com/hardcore-os/plato/common/prpc/discov/plugin"
	serverinterceptor "github.com/hardcore-os/plato/common/prpc/interceptor/server"
	"google.golang.org/grpc"
)

// 实现了一个基于 gRPC 的服务框架 prpc，用于方便地创建和管理 gRPC 服务
// 包括服务注册、拦截器配置、服务发现与注册、以及优雅停机等功能

type RegisterFn func(*grpc.Server)

// 表示一个 gRPC 服务实例，包含服务配置、注册的服务、拦截器等信息
type PServer struct {
	serverOptions                               // 存储服务的配置信息，如服务名、IP、端口、权重等
	registers     []RegisterFn                  // 存储服务注册函数，用于将 gRPC 服务注册到 gRPC 服务器
	interceptors  []grpc.UnaryServerInterceptor // 存储 gRPC 拦截器，用于在请求处理过程中添加自定义逻辑
}

type serverOptions struct {
	serviceName string
	ip          string
	port        int
	weight      int
	health      bool
	d           discov.Discovery
}

// 用于设置 serverOptions 结构体的配置项，采用函数式选项模式，使配置更灵活
// 传统方式：
// 如果使用多个参数的构造函数，调用者可能需要记住参数的顺序和含义，尤其是在参数很多的情况下
// 如果需要添加新的配置选项，可能需要修改构造函数的签名，这会影响所有调用该构造函数的代码。
// 很难处理可选参数和默认值，通常需要使用多个构造函数或复杂的逻辑
// 参数的顺序很重要，如果顺序错误，可能导致配置错误。
type ServerOption func(opts *serverOptions)

// WithServiceName set serviceName
func WithServiceName(serviceName string) ServerOption {
	return func(opts *serverOptions) {
		opts.serviceName = serviceName
	}
}

// WithIP set ip
func WithIP(ip string) ServerOption {
	return func(opts *serverOptions) {
		opts.ip = ip
	}
}

// WithPort set port
func WithPort(port int) ServerOption {
	return func(opts *serverOptions) {
		opts.port = port
	}
}

// WithWeight set weight
func WithWeight(weight int) ServerOption {
	return func(opts *serverOptions) {
		opts.weight = weight
	}
}

// WithHealth set health
func WithHealth(health bool) ServerOption {
	return func(opts *serverOptions) {
		opts.health = health
	}
}

// 创建 PServer 实例，并初始化配置和服务发现客户端
func NewPServer(opts ...ServerOption) *PServer {
	opt := serverOptions{}
	for _, o := range opts { // opts是由 接受一个 *serverOptions 类型的指针作为参数的返回的函数（闭包） 组成的
		o(&opt)
	}

	if opt.d == nil {
		dis, err := plugin.GetDiscovInstance()
		if err != nil {
			panic(err)
		}

		opt.d = dis
	}

	return &PServer{
		opt,
		make([]RegisterFn, 0),
		make([]grpc.UnaryServerInterceptor, 0),
	}
}

// RegisterService ...
// eg :
//
//	p.RegisterService(func(server *grpc.Server) {
//	    test.RegisterGreeterServer(server, &Server{})
//	})

// 用于注册 gRPC 服务，接受一个 RegisterFn 类型的函数，该函数用于将服务注册到 gRPC 服务器。
func (p *PServer) RegisterService(register ...RegisterFn) {
	p.registers = append(p.registers, register...)
}

// 用于注册 gRPC 拦截器，允许添加自定义的拦截器
func (p *PServer) RegisterUnaryServerInterceptor(i grpc.UnaryServerInterceptor) {
	p.interceptors = append(p.interceptors, i)
}

// Start 开启server
// 启动 gRPC 服务器，并处理服务注册、信号处理和优雅停机。
func (p *PServer) Start(ctx context.Context) {
	service := discov.Service{
		Name: p.serviceName,
		Endpoints: []*discov.Endpoint{
			{ // 隐式地获取其指针
				ServerName: p.serviceName,
				IP:         p.ip,
				Port:       p.port,
				Weight:     p.weight,
				Enable:     true,
			},
		},
	}

	// 加载拦截器
	interceptors := []grpc.UnaryServerInterceptor{
		serverinterceptor.RecoveryUnaryServerInterceptor(),            // 用于捕获 panic 并恢复
		serverinterceptor.TraceUnaryServerInterceptor(),               // 用于跟踪请求
		serverinterceptor.MetricUnaryServerInterceptor(p.serviceName), // 用于收集服务指标
	}
	interceptors = append(interceptors, p.interceptors...) // 追加自定义的拦截器

	s := grpc.NewServer(grpc.ChainUnaryInterceptor(interceptors...)) // 创建一个新的 gRPC 服务器实例，并使用拦截器链

	// 注册服务
	for _, register := range p.registers {
		register(s) // 将服务注册到 gRPC 服务器 s
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", p.ip, p.port))
	if err != nil {
		panic(err)
	}

	go func() {
		if err := s.Serve(lis); err != nil { // 启动 gRPC 服务器
			panic(err)
		}
	}()
	// 服务注册
	p.d.Register(ctx, &service) // 将服务注册到服务发现系统 p.d

	logger.Info("start PRCP success")

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT) // 监听系统信号
	for {
		sig := <-c
		switch sig {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			s.Stop()                      // 停止 gRPC 服务器
			p.d.UnRegister(ctx, &service) // 从服务发现系统注销服务
			time.Sleep(time.Second)
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}

}
