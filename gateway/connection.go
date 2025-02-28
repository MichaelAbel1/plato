package gateway

import (
	"net"
	"sync"
)

var node *ConnIDGenerater

// 如果想要调整编码，就直接调整这里即可
const (
	version      = uint64(0) // 版本控制
	sequenceBits = uint64(16)

	maxSequence = int64(-1) ^ (int64(-1) << sequenceBits)

	timeLeft    = uint8(16) // timeLeft = sequenceBits // 时间戳向左偏移量
	versionLeft = uint8(63) // 左移动到最高位
	// 2024-05-20 08:00:00 +0800 CST
	twepoch = int64(1589923200000) // 常量时间戳(毫秒)
)

type ConnIDGenerater struct {
	mu        sync.Mutex
	LastStamp int64 // 记录上一次ID的时间戳
	Sequence  int64 // 当前毫秒已经生成的ID序列号(从0 开始累加) 1毫秒内最多生成2^16个ID
}

type connection struct {
	// id   uint64 // 进程级别的生命周期
	fd int
	// e    *epoller
	conn *net.TCPConn
}

func (c *connection) Close() {
	// ep.tables.Delete(c.id)
	// if c.e != nil {
	// 	c.e.fdToConnTable.Delete(c.fd)
	// }
	err := c.conn.Close()
	panic(err)
}

func (c *connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}
