package tcp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

func ReadData(conn *net.TCPConn) ([]byte, error) {
	var dataLen uint32
	dataLenBuf := make([]byte, 4)
	if err := readFixedData(conn, dataLenBuf); err != nil {
		return nil, err
	}
	// fmt.Printf("readFixedData:%+v\n", dataLenBuf)
	buffer := bytes.NewBuffer(dataLenBuf) // 创建一个 bytes.Buffer，并将 dataLenBuf 作为其内容
	// 在TCP/IP协议族中，网络字节序（Network Byte Order）被定义为大端字节序
	if err := binary.Read(buffer, binary.BigEndian, &dataLen); err != nil { // 从 buffer 中读取一个 uint32 类型的值，并将其存储到 dataLen 变量中
		return nil, fmt.Errorf("read headlen error:%s", err.Error())
	}
	if dataLen <= 0 {
		return nil, fmt.Errorf("wrong headlen :%d", dataLen)
	}
	dataBuf := make([]byte, dataLen)
	// fmt.Printf("readFixedData.dataLen:%+v\n", dataLen)
	if err := readFixedData(conn, dataBuf); err != nil {
		return nil, fmt.Errorf("read headlen error:%s", err.Error())
	}
	return dataBuf, nil
}

// 读取固定buf长度的数据
func readFixedData(conn *net.TCPConn, buf []byte) error {
	_ = (*conn).SetReadDeadline(time.Now().Add(time.Duration(120) * time.Second)) // 设置读取120s超时时间, 如果超时，Read 操作将会返回一个超时错误
	var pos int = 0
	var totalSize int = len(buf) // 读取buf长度，读取的总字节数
	for {
		c, err := (*conn).Read(buf[pos:]) // 一次 Read 调用最多可以读取 len(buf[pos:]) 字节的数据
		if err != nil {
			return err
		}
		pos = pos + c // pos 移动到下一次读取的位置
		if pos == totalSize {
			break
		}
	}
	return nil
}
