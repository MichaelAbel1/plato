package main

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/hardcore-os/plato/common/config"
	"github.com/hardcore-os/plato/common/prpc"
	"github.com/hardcore-os/plato/common/prpc/example/helloservice"
	ptrace "github.com/hardcore-os/plato/common/prpc/trace"
)

func main() {
	config.Init(currentFileDir() + "/prpc_client.yaml") // 从配置文件加载配置信息

	ptrace.StartAgent() // 启动 trace 进行分布式追踪
	defer ptrace.StopAgent()

	pCli, _ := prpc.NewPClient("prpc_server") // 创建一个名为 "prpc_server" 的 gRPC 客户端。

	ctx, _ := context.WithTimeout(context.TODO(), 100*time.Second) // 创建一个带有 100 秒超时的上下文。
	cli := helloservice.NewGreeterClient(pCli.Conn())              // 创建一个Greeter（自定义）用于调用 gRPC 服务的客户端，该客户端使用 pCli.Conn() 获取连接。
	resp, err := cli.SayHello(ctx, &helloservice.HelloRequest{
		Name: "xxxxxx",
	})
	fmt.Println(resp, err) // 打印响应和错误信息
}

func currentFileDir() string {
	_, file, _, ok := runtime.Caller(1) // 获取调用 currentFileDir 函数的文件信息
	parts := strings.Split(file, "/")   // 将文件路径分割成数组

	if !ok {
		return ""
	}

	dir := ""
	for i := 0; i < len(parts)-1; i++ {
		dir += "/" + parts[i]
	}

	return dir[1:] // 返回文件所在目录的路径
}
