package gateway

import (
	"fmt"
	"log"
	"net"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/hardcore-os/plato/common/config"
	"golang.org/x/sys/unix"
)

// 全局对象
var ep *ePool    // epoll池
var tcpNum int32 // 当前服务允许接入的最大tcp连接数

type ePool struct {
	eChan  chan *connection // 和传递fd类似，只是这里多封装了一层
	tables sync.Map
	eSize  int
	done   chan struct{}

	ln *net.TCPListener
	f  func(c *connection, ep *epoller)
}

func initEpoll(ln *net.TCPListener, f func(c *connection, ep *epoller)) {
	setLimit()
	ep = newEPool(ln, f)
	ep.createAcceptProcess()
	ep.startEPool()
}

func newEPool(ln *net.TCPListener, cb func(c *connection, ep *epoller)) *ePool {
	return &ePool{
		eChan:  make(chan *connection, config.GetGatewayEpollerChanNum()),
		done:   make(chan struct{}),
		eSize:  config.GetGatewayEpollerNum(),
		tables: sync.Map{},
		ln:     ln,
		f:      cb,
	}
}

// 创建一个专门处理 accept 事件的协程，与当前cpu的核数对应，能够发挥最大功效
func (e *ePool) createAcceptProcess() {
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for {
				conn, e := e.ln.AcceptTCP()
				// 限流熔断
				if !checkTcp() {
					_ = conn.Close() // 可以处理的更优雅点 比如返回错误信息，然后让客户端连接其他服务器
					continue
				}
				setTcpConifg(conn)
				if e != nil {
					if ne, ok := e.(net.Error); ok && ne.Temporary() {
						fmt.Errorf("accept temp err: %v", ne)
						continue
					}
					fmt.Errorf("accept err: %v", e)
				}
				c := connection{
					conn: conn,
					fd:   socketFD(conn),
				}
				ep.addTask(&c)
			}
		}()
	}
}

func (e *ePool) startEPool() {
	for i := 0; i < e.eSize; i++ {
		go e.startEProc()
	}
}

// 轮询器池 处理器
func (e *ePool) startEProc() {
	ep, err := newEpoller()
	if err != nil {
		panic(err)
	}
	// 监听连接创建事件
	go func() {
		for {
			select {
			case <-e.done:
				return // 这里可以增加资源回收
			case conn := <-e.eChan:
				addTcpNum()
				fmt.Printf("tcpNum:%d\n", tcpNum)
				if err := ep.add(conn); err != nil {
					fmt.Printf("failed to add connection %v\n", err)
					conn.Close() //登录未成功直接关闭连接
					continue
				}
				fmt.Printf("EpollerPool new connection[%v] tcpSize:%d\n", conn.RemoteAddr(), tcpNum)
			}
		}
	}()
	// 轮询器在这里轮询等待, 当有wait发生时则调用回调函数去处理
	for {
		select {
		case <-e.done:
			return
		default:
			connections, err := ep.wait(200)        // 200ms 一次轮询 防止忙轮询
			if err != nil && err != syscall.EINTR { // syscall.EINTR 表示系统调用被中断，通常可以忽略
				fmt.Printf("failed to epoll wait %v\n", err)
				continue
			}
			for _, conn := range connections {
				if conn == nil {
					break
				}
				e.f(conn, ep) // 调用回调函数处理事件
			}
		}
	}
}

func (e *ePool) addTask(c *connection) {
	e.eChan <- c
}

// epoller 对象 轮询器
type epoller struct {
	fd int
	// 不需要显式初始化
	// 可以直接使用多个 goroutine 同时读写，而无需额外的锁
	// 适用于这种读多写少的场景
	fdToConnTable sync.Map
}

func newEpoller() (*epoller, error) {
	fd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &epoller{
		fd: fd,
	}, nil
}

// TODO: 默认水平触发模式,可采用非阻塞FD,优化边沿触发模式
// func (e *epoller) add(conn *connection) error {
// 	// Extract file descriptor associated with the connection
// 	fd := conn.fd
// 	// EPOLLIN：表示有数据可读。
// 	// EPOLLHUP：表示连接挂断。
// 	// Fd: int32(fd): 将文件描述符设置到epoll event中。
// 	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLHUP, Fd: int32(fd)})
// 	if err != nil {
// 		return err
// 	}
// 	e.fdToConnTable.Store(conn.fd, conn)
// 	// ep.tables.Store(conn.id, conn)
// 	// conn.BindEpoller(e)
// 	return nil
// }

// 优化后的epoll
func (e *epoller) add(conn *connection) error {
	fd := conn.fd
	// 设置为非阻塞模式
	if err := unix.SetNonblock(fd, true); err != nil {
		return err
	}
	// unix.EPOLLET：表示将 epoll 设置为边缘触发（edge-triggered）模式。
	// EPOLLIN：表示有数据可读。
	// EPOLLHUP：表示连接挂断。
	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: uint32(unix.EPOLLIN|unix.EPOLLHUP) | unix.EPOLLET, Fd: int32(fd)})
	if err != nil {
		return err
	}

	e.fdToConnTable.Store(conn.fd, conn)
	return nil
}

func (e *epoller) remove(c *connection) error {
	subTcpNum()
	fd := c.fd
	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil {
		return err
	}
	// ep.tables.Delete(c.id)
	e.fdToConnTable.Delete(c.fd)
	return nil
}

// 返回触发事件的连接列表和错误
func (e *epoller) wait(msec int) ([]*connection, error) {
	events := make([]unix.EpollEvent, config.GetGatewayEpollWaitQueueSize())
	n, err := unix.EpollWait(e.fd, events, msec)
	if err != nil {
		return nil, err
	}
	var connections []*connection
	for i := 0; i < n; i++ {
		if conn, ok := e.fdToConnTable.Load(int(events[i].Fd)); ok {
			connections = append(connections, conn.(*connection))
		}
	}
	return connections, nil
}
func socketFD(conn *net.TCPConn) int {
	tcpConn := reflect.Indirect(reflect.ValueOf(*conn)).FieldByName("conn")
	fdVal := tcpConn.FieldByName("fd")
	pfdVal := reflect.Indirect(fdVal).FieldByName("pfd")
	return int(pfdVal.FieldByName("Sysfd").Int())
}

// 更安全，更优的做法
// func socketFD(conn *net.TCPConn) (int, error) {
// 	file, err := conn.File()
// 	if err != nil {
// 			return 0, err
// 	}
// 	defer file.Close() // 记得关闭文件
// 	return int(file.Fd()), nil
// }

// 设置go 进程打开文件数的限制
func setLimit() {
	var rLimit syscall.Rlimit                                                 // 用于存储文件描述符的限制信息用于存储文件描述符的限制信息
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil { // 获取当前进程打开文件数的限制
		panic(err)
	}
	rLimit.Cur = rLimit.Max                                                   // 设置当前进程打开文件数的限制为最大值
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil { // 设置当前进程打开文件数的限制
		panic(err)
	}

	log.Printf("set cur limit: %d", rLimit.Cur)
}

func addTcpNum() {
	atomic.AddInt32(&tcpNum, 1)
}

func getTcpNum() int32 {
	return atomic.LoadInt32(&tcpNum)
}
func subTcpNum() {
	atomic.AddInt32(&tcpNum, -1)
}

func checkTcp() bool {
	num := getTcpNum()
	maxTcpNum := config.GetGatewayMaxTcpNum()
	return num <= maxTcpNum
}

// 启用给定 TCP 连接的 keep-alive 探测。这有助于检测和关闭空闲的无效连接，从而释放资源并提高应用程序的可靠性
func setTcpConifg(c *net.TCPConn) {
	_ = c.SetKeepAlive(true)
}
