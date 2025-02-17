package ipconf

import (
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/hardcore-os/plato/common/config"
	"github.com/hardcore-os/plato/ipconf/domain"
	"github.com/hardcore-os/plato/ipconf/source"
)

// RunMain 启动web容器
func RunMain(path string) {
	config.Init(path)                                  // 使用指定的配置文件路径初始化配置
	source.Init()                                      // 数据源要优先启动
	domain.Init()                                      // 初始化调度层
	s := server.Default(server.WithHostPorts(":6789")) // 创建一个 Hertz Web 服务器实例，监听 6789 端口。
	s.GET("/ip/list", GetIpInfoList)                   // 注册一个 GET 请求路由，当请求路径为 /ip/list 时，调用 GetIpInfoList 函数处理请求
	s.Spin()                                           // 启动 Web 服务器，开始监听请求
}
