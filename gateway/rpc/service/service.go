package service

import (
	context "context"
	"fmt"
)

const (
	DelConnCmd = 1 // DelConn
	PushCmd    = 2 // push
)

type CmdContext struct {
	Ctx      *context.Context
	Cmd      int32
	FD       int
	Playload []byte
}

type Service struct {
	UnimplementedGatewayServer // 嵌入 UnimplementedGatewayServer
	CmdChannel                 chan *CmdContext
}

func (s *Service) DelConn(ctx context.Context, gr *GatewayRequest) (*GatewayResponse, error) {
	c := context.TODO()
	// fmt.Printf("CmdContext: {Ctx: %+v, Cmd: %s, FD: %d}\n", c, "DelConnCmd", int(gr.GetFd()))
	s.CmdChannel <- &CmdContext{
		Ctx: &c,
		Cmd: DelConnCmd,
		FD:  int(gr.GetFd()),
	}
	// fmt.Printf("CmdContext: {Ctx: %+v, Cmd: %s, FD: %d}\n", c, "DelConnCmd", int(gr.GetFd()))
	return &GatewayResponse{
		Code: 0,
		Msg:  "success",
	}, nil
}

func (s *Service) Push(ctx context.Context, gr *GatewayRequest) (*GatewayResponse, error) {
	c := context.TODO()
	fmt.Println("gateway.rpc.service.Push()： push a message")
	s.CmdChannel <- &CmdContext{
		Ctx:      &c,
		Cmd:      PushCmd,
		FD:       int(gr.GetFd()),
		Playload: gr.GetData(),
	}
	// fmt.Printf("CmdContext: {Ctx: %+v, Cmd: %s, FD: %d}\n", c, "DelConnCmd", int(gr.GetFd()))
	return &GatewayResponse{
		Code: 0,
		Msg:  "success",
	}, nil
}
