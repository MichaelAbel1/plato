package sdk

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/hardcore-os/plato/common/tcp"
)

// type connect struct {
// 	sendChan, recvChan chan *Message
// 	conn               *net.TCPConn
// 	connID             uint64
// 	ip                 net.IP
// 	port               int
// }

type connect struct {
	conn               *net.TCPConn
	sendChan, recvChan chan *Message
}

// func newConnet(serverAddr string) *connect {
// 	return &connect{
// 		serverAddr: serverAddr,
// 		sendChan:   make(chan *Message),
// 		recvChan:   make(chan *Message),
// 	}
// }

func newConnet(ip net.IP, port int) *connect {
	clientConn := &connect{
		sendChan: make(chan *Message),
		recvChan: make(chan *Message),
	}
	addr := &net.TCPAddr{IP: ip, Port: port}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		fmt.Printf("DialTCP.err=%+v", err)
		return nil
	}
	clientConn.conn = conn
	go func() {
		for {
			data, err := tcp.ReadData(clientConn.conn)
			if err != nil {
				fmt.Printf("ReadData.err=%+v \n", err)
				// clientConn.close()
				// return
			}
			msg := &Message{}
			json.Unmarshal(data, msg)
			clientConn.recvChan <- msg
		}
	}()
	return clientConn
}

func (c *connect) send(data *Message) {
	// 直接发送给接收方
	// c.recvChan <- data
	bytes, _ := json.Marshal(data)
	dataPgk := tcp.DataPgk{
		Data: bytes,
		Len:  uint32(len(bytes)),
	}
	xx := dataPgk.Marshal()
	c.conn.Write(xx)
}

func (c *connect) recv() <-chan *Message {
	return c.recvChan
}

func (c *connect) close() {
	c.conn.Close()
}
