package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/hardcore-os/plato/common/config"
	"github.com/hardcore-os/plato/common/prpc"
	"github.com/hardcore-os/plato/common/tcp"

	"github.com/hardcore-os/plato/gateway/rpc/client"
	"github.com/hardcore-os/plato/gateway/rpc/service"
	"google.golang.org/grpc"
)

var cmdChannel chan *service.CmdContext

// RunMain 启动网关服务
func RunMain(path string) {
	config.Init(path)
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{Port: config.GetGatewayTCPServerPort()}) // 创建一个 TCP 监听器
	if err != nil {
		log.Fatalf("StartTCPEPollServer err:%s", err.Error())
		panic(err)
	}
	initWorkPoll()
	initEpoll(ln, runProc)
	fmt.Println("-------------im gateway stated------------")
	cmdChannel = make(chan *service.CmdContext, config.GetGatewayCmdChannelNum())
	s := prpc.NewPServer(
		prpc.WithServiceName(config.GetGatewayServiceName()),
		prpc.WithIP(config.GetGatewayServiceAddr()),
		prpc.WithPort(config.GetGatewayRPCServerPort()), prpc.WithWeight(config.GetGatewayRPCWeight()))
	fmt.Println(config.GetGatewayServiceName(), config.GetGatewayServiceAddr(), config.GetGatewayRPCServerPort(), config.GetGatewayRPCWeight())
	s.RegisterService(func(server *grpc.Server) {
		service.RegisterGatewayServer(server, &service.Service{CmdChannel: cmdChannel}) // 向 gRPC 服务器 server 注册 GatewayServer 服务，并提供该服务的具体实现 service.Service
	})
	// 启动rpc 客户端
	client.Init()
	// 启动 命令处理写协程
	go cmdHandler()
	// 启动 rpc server
	s.Start(context.TODO())
}

func runProc(c *connection, ep *epoller) {
	ctx := context.Background() // 起始的contenxt
	// step1: 读取一个完整的消息包
	dataBuf, err := tcp.ReadData(c.conn)
	if err != nil {
		// 如果读取conn时发现连接关闭，则直接端口连接
		// 通知 state 清理掉意外退出的 conn的状态信息
		if errors.Is(err, io.EOF) {
			// 这步操作是异步的，不需要等到返回成功在进行，因为消息可靠性的保障是通过协议完成的而非某次cmd
			ep.remove(c)
			client.CancelConn(&ctx, getEndpoint(), c.id, nil)
		}
		return
	}
	err = wPool.Submit(func() { // 利用工作线程池管理并发任务的执行，以提高程序的吞吐量和资源利用率，同时避免创建过多的 goroutine 导致性能下降
		// step2:交给 state server rpc 处理
		client.SendMsg(&ctx, getEndpoint(), c.id, dataBuf)
		// bytes := tcp.DataPgk{
		// 	Len:  uint32(len(dataBuf)),
		// 	Data: dataBuf,
		// }
		// tcp.SendData(c.conn, bytes.Marshal()) // 这里是不通过rpc发送到state server 直接发回去
	})
	if err != nil {
		fmt.Errorf("runProc:err:%+v\n", err.Error())
	}
}

func cmdHandler() {
	for cmd := range cmdChannel {
		// 异步提交到协池中完成发送任务
		switch cmd.Cmd {
		case service.DelConnCmd:
			wPool.Submit(func() { closeConn(cmd) })
		case service.PushCmd:
			fmt.Println("gateway.server.cmdHandler() Submit a msg to sendMsgByCmd()")
			wPool.Submit(func() { sendMsgByCmd(cmd) })
		default:
			panic("command undefined")
		}
	}
}
func closeConn(cmd *service.CmdContext) {
	if connPtr, ok := ep.tables.Load(cmd.ConnID); ok {
		conn, _ := connPtr.(*connection)
		conn.Close()
	}
}
func sendMsgByCmd(cmd *service.CmdContext) {
	fmt.Printf("gateway.server.sendMsgByCmd() reciecve a cmd: %v", cmd)
	connPtr, ok := ep.tables.Load(cmd.ConnID)
	if ok {
		conn, _ := connPtr.(*connection)
		dp := tcp.DataPgk{
			Len:  uint32(len(cmd.Payload)),
			Data: cmd.Payload,
		}
		fmt.Printf("gateway.server.sendMsgByCmd() send Msg.\n")
		tcp.SendData(conn.conn, dp.Marshal())
	}
	fmt.Printf("gateway.server.sendMsgByCmd() ep.tables.Load(cmd.FD):%v, send Msg end.\n", ok)
}

func getEndpoint() string {
	return fmt.Sprintf("%s:%d", config.GetGatewayServiceAddr(), config.GetGatewayRPCServerPort())
}
