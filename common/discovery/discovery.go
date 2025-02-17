package discovery

import (
	"context"
	"sync"

	"github.com/bytedance/gopkg/util/logger"
	"github.com/hardcore-os/plato/common/config"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3" // 创建别名
)

// ServiceDiscovery 服务发现
type ServiceDiscovery struct {
	cli  *clientv3.Client //etcd client
	lock sync.Mutex
	ctx  *context.Context
}

// NewServiceDiscovery  新建发现服务
func NewServiceDiscovery(ctx *context.Context) *ServiceDiscovery {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.GetEndpointsForDiscovery(),
		DialTimeout: config.GetTimeoutForDiscovery(),
	})
	if err != nil {
		logger.Fatal(err)
	}

	return &ServiceDiscovery{
		cli: cli,
		ctx: ctx,
	}
}

// WatchService 初始化服务列表和监视
// 即使 prefix 下没有键值对，s.cli.Get 函数仍然会执行，并返回一个空的键值对切片
// 过 s.watcher 函数，服务发现机制可以捕获到后续添加的键值对，从而实现动态的服务发现。
func (s *ServiceDiscovery) WatchService(prefix string, set, del func(key, value string)) error {
	//根据前缀获取现有的key
	resp, err := s.cli.Get(*s.ctx, prefix, clientv3.WithPrefix()) // 获取指定前缀下的所有键值对 在register.NewServiceRegister方法中添加
	if err != nil {
		return err
	}

	for _, ev := range resp.Kvs {
		set(string(ev.Key), string(ev.Value))
	}
	//监视前缀，修改变更的server
	// 如果 Watch 操作从当前 Revision 开始监听，可能会错过在 s.cli.Get 函数执行期间发生的数据变更。
	s.watcher(prefix, resp.Header.Revision+1, set, del) // 从当前版本号的下一个版本开始监听 保证不遗漏任何数据变更事件
	return nil
}

// watcher 监听前缀
func (s *ServiceDiscovery) watcher(prefix string, rev int64, set, del func(key, value string)) {
	rch := s.cli.Watch(*s.ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(rev)) // clientv3.WithRev(rev) 表示从指定的 revision 开始监听
	logger.CtxInfof(*s.ctx, "watching prefix:%s now...", prefix)
	// watcher 函数会一直监听 etcd 的变化，直到与 etcd 服务器的连接断开 或 s.ctx 上下文被取消。
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT: //修改或者新增
				set(string(ev.Kv.Key), string(ev.Kv.Value))
			case mvccpb.DELETE: //删除
				del(string(ev.Kv.Key), string(ev.Kv.Value))
			}
		}
	}
}

// Close 关闭服务
func (s *ServiceDiscovery) Close() error {
	return s.cli.Close()
}
