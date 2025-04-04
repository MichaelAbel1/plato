package client

import (
	"context"
	"fmt"
	"time"

	"github.com/hardcore-os/plato/common/config"
	"github.com/hardcore-os/plato/common/prpc"
	"github.com/hardcore-os/plato/state/rpc/service"
)

var stateClient service.StateClient

func initStateClient() {
	pCli, err := prpc.NewPClient(config.GetStateServiceName())
	if err != nil {
		panic(err)
	}
	// stateClient = service.NewStateClient(pCli.Conn())  // 通过etcd建立连接
	cli, err := pCli.DialByEndPoint(config.GetGatewayStateServerEndPoint()) // 通过指定地址建立连接 也就是单元化 gateway 与 state server绑定
	if err != nil {
		panic(err)
	}
	stateClient = service.NewStateClient(cli)
}

// 在 RPC 调用中用 connID 替换 fd（文件描述符）的主要原因
// 1. fd（文件描述符）在本地进程中是唯一的，但在分布式系统或不同进程之间可能会重复。connID（连接 ID）通常是全局唯一的，可以跨服务器或进程进行标识
// 2. 在本地进程中，操作系统分配 fd 时，关闭的 fd 可能被重新分配, 如果 RPC 传输 fd，而 fd 被回收再分配，可能导致错误操作。例如：发错联系人
// 3. 在 多进程 / 多服务器 场景下，RPC 需要跨机器正确识别连接，不能依赖 fd 这种本地资源。
func CancelConn(ctx *context.Context, endpoint string, connID uint64, Payload []byte) error {
	rpcCtx, _ := context.WithTimeout(*ctx, 100*time.Millisecond)
	stateClient.CancelConn(rpcCtx, &service.StateRequest{
		Endpoint: endpoint,
		ConnID:   connID,
		Data:     Payload,
	})
	return nil
}

func SendMsg(ctx *context.Context, endpoint string, connID uint64, Payload []byte) error {
	rpcCtx, _ := context.WithTimeout(*ctx, 100*time.Millisecond)
	fmt.Println("sendMsg", connID, string(Payload))
	_, err := stateClient.SendMsg(rpcCtx, &service.StateRequest{
		Endpoint: endpoint,
		ConnID:   connID,
		Data:     Payload,
	})
	if err != nil {
		panic(err)
	}
	return nil
}
