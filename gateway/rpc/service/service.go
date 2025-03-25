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
	Ctx     *context.Context
	Cmd     int32
	ConnID  uint64
	Payload []byte
}

type Service struct {
	UnimplementedGatewayServer // 嵌入 UnimplementedGatewayServer
	CmdChannel                 chan *CmdContext
}

func (s *Service) DelConn(ctx context.Context, gr *GatewayRequest) (*GatewayResponse, error) {
	c := context.TODO()
	// fmt.Printf("CmdContext: {Ctx: %+v, Cmd: %s, FD: %d}\n", c, "DelConnCmd", int(gr.GetFd()))
	s.CmdChannel <- &CmdContext{
		Ctx:    &c,
		Cmd:    DelConnCmd,
		ConnID: gr.ConnID,
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
		Ctx:     &c,
		Cmd:     PushCmd,
		ConnID:  gr.ConnID,
		Payload: gr.GetData(),
	}
	// fmt.Printf("CmdContext: {Ctx: %+v, Cmd: %s, FD: %d}\n", c, "DelConnCmd", int(gr.GetFd()))
	return &GatewayResponse{
		Code: 0,
		Msg:  "success",
	}, nil
}
