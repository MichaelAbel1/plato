package gateway

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/hardcore-os/plato/common/config"
	"github.com/hardcore-os/plato/common/tcp"
)

// var cmdChannel chan *service.CmdContext

// RunMain 启动网关服务
func RunMain(path string) {
	config.Init(path)
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{Port: config.GetGatewayTCPServerPort()})
	if err != nil {
		log.Fatalf("StartTCPEPollServer err:%s", err.Error())
		panic(err)
	}
	initWorkPoll()
	initEpoll(ln, runProc)
	fmt.Println("-------------im gateway stated------------")
	select {}

}

func runProc(c *connection, ep *epoller) {
	// ctx := context.Background() // 起始的contenxt
	// step1: 读取一个完整的消息包
	dataBuf, err := tcp.ReadData(c.conn)
	if err != nil {
		// 如果读取conn时发现连接关闭，则直接端口连接
		// 通知 state 清理掉意外退出的 conn的状态信息
		if errors.Is(err, io.EOF) {
			// 这步操作是异步的，不需要等到返回成功在进行，因为消息可靠性的保障是通过协议完成的而非某次cmd
			ep.remove(c)
			// client.CancelConn(&ctx, getEndpoint(), c.id, nil)
		}
		return
	}
	err = wPool.Submit(func() { // 利用工作线程池管理并发任务的执行，以提高程序的吞吐量和资源利用率，同时避免创建过多的 goroutine 导致性能下降
		// step2:交给 state server rpc 处理
		// client.SendMsg(&ctx, getEndpoint(), c.id, dataBuf)
		bytes := tcp.DataPgk{
			Len:  uint32(len(dataBuf)),
			Data: dataBuf,
		}
		tcp.SendData(c.conn, bytes.Marshal())
	})
	if err != nil {
		fmt.Errorf("runProc:err:%+v\n", err.Error())
	}
}
