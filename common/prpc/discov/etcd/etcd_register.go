package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/gopkg/util/logger"
	"github.com/hardcore-os/plato/common/prpc/discov"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const KeyPrefix = "/plato/prpc/" // etcd 中存储 Service 信息的 key 前缀，所有服务注册信息都会存放在以 /plato/prpc/ 开头的路径下

// Register ...
type Register struct {
	Options                                  // 存储 etcd 相关的配置（如 Endpoints、DialTimeout）
	cli                 *clientv3.Client     // etcd 客户端实例，用于和 etcd 通信
	serviceRegisterCh   chan *discov.Service // 用于存放待注册的服务
	serviceUnRegisterCh chan *discov.Service // 用于存放待注销的服务
	lock                sync.Mutex
	downServices        atomic.Value                // 存放已经下线的服务
	registerServices    map[string]*registerService // 存储已注册的 Service 实例
	listeners           []func()                    // 监听器列表，当服务状态变化时调用回调函数
	watchServices       sync.Map                    // 存储已监听的服务
}

type registerService struct {
	service      *discov.Service                         // 当前注册的 Service 实例
	leaseID      clientv3.LeaseID                        // etcd 的租约 ID，用于定期续约，防止服务失效
	isRegistered bool                                    // 表示服务是否已成功注册到 etcd
	keepAliveCh  <-chan *clientv3.LeaseKeepAliveResponse // etcd KeepAlive 机制的返回通道，定期续约
}

// NewETCDRegister ...
func NewETCDRegister(opts ...Option) (discov.Discovery, error) {
	opt := defaultOption
	for _, o := range opts { // 可选参数，用于初始化 Options
		o(&opt)
	}

	r := &Register{
		Options:             opt,
		serviceRegisterCh:   make(chan *discov.Service),
		serviceUnRegisterCh: make(chan *discov.Service),
		lock:                sync.Mutex{},
		downServices:        atomic.Value{},
		registerServices:    make(map[string]*registerService),
	}

	if err := r.init(context.TODO()); err != nil { // 初始化 etcd 连接
		return nil, err
	}

	return r, nil
}

// Init 初始化 todo 需要改造从viper配置中读取endpoints
func (r *Register) init(ctx context.Context) error {
	var err error
	r.cli, err = clientv3.New(
		clientv3.Config{
			Endpoints:   r.endpoints,
			DialTimeout: r.dialTimeout,
		})

	if err != nil {
		return err
	}

	go r.run() // 异步处理服务注册和注销的请求

	return nil
}

func (r *Register) run() {
	for {
		select {
		case service := <-r.serviceRegisterCh: // 注册服务
			if _, ok := r.registerServices[service.Name]; ok {
				r.registerServices[service.Name].service.Endpoints = append(r.registerServices[service.Name].service.Endpoints, service.Endpoints...)
				r.registerServices[service.Name].isRegistered = false // 服务增加新端口后重新上报到etcd
			} else {
				r.registerServices[service.Name] = &registerService{
					service:      service,
					isRegistered: false,
				}
			}
		case service := <-r.serviceUnRegisterCh: // 注销服务
			if _, ok := r.registerServices[service.Name]; !ok {
				logger.CtxErrorf(context.TODO(), "UnRegisterService err, service %v was not registered", service.Name)
				continue
			}
			r.unRegisterService(context.TODO(), service)
		default: // 保持服务存活（续约）
			r.registerServiceOrKeepAlive(context.TODO())
			time.Sleep(r.registerServiceOrKeepAliveInterval)
		}
	}
}

func (r *Register) registerServiceOrKeepAlive(ctx context.Context) {
	for _, service := range r.registerServices {
		if !service.isRegistered {
			r.registerService(ctx, service)
		} else {
			// 避免 KeepAlive 续约失败导致 etcd 清理掉注册信息，提高稳定性
			go r.KeepAlive(ctx, service) // 开启 goroutine 并发续约
		}
	}
}

func (r *Register) registerService(ctx context.Context, service *registerService) {
	// 向 etcd 申请一个 租约（Lease），r.keepAliveInterval 指定租约的存活时间
	// 如果租约过期且没有续约，所有绑定该租约的 key 都会被删除
	leaseGrantResp, err := r.cli.Grant(ctx, r.keepAliveInterval)
	if err != nil {
		logger.CtxErrorf(ctx, "register service grant,err:%v", err)
		return
	}
	service.leaseID = leaseGrantResp.ID // 后续服务的 key 将绑定该租约，以确保 key 不会永久存在。

	for _, endpoint := range service.service.Endpoints { // 遍历该服务的所有实例
		key := r.getEtcdRegisterKey(service.service.Name, endpoint.IP, endpoint.Port) // 生成 etcd 存储的 key，格式类似：/plato/prpc/{service_name}/{ip}/{port}
		raw, err := json.Marshal(endpoint)                                            // 将 endpoint 转换为 JSON 字符串，以便存储到 etcd
		if err != nil {
			logger.CtxErrorf(ctx, "register service err,err:%v, register data:%v", err, string(raw))
			continue
		}
		// 使用 WithLease(leaseGrantResp.ID) 绑定租约，意味着该 key 只有在租约有效时才会存在，一旦租约到期，该 key 也会被 etcd 删除。
		_, err = r.cli.Put(ctx, key, string(raw), clientv3.WithLease(leaseGrantResp.ID))
		if err != nil {
			logger.CtxErrorf(ctx, "register service err,err:%v, register data:%v", err, string(raw))
			continue
		}

	}

	keepAliveCh, err := r.cli.KeepAlive(ctx, leaseGrantResp.ID) // 启动一个后台 goroutine，周期性地向 etcd 发送 keep-alive 请求，以保持租约有效。
	if err != nil {
		logger.CtxErrorf(ctx, "register service keepalive,err:%v", err)
		return
	}

	service.keepAliveCh = keepAliveCh // 用于接收 etcd 返回的 LeaseKeepAliveResponse，表示续约成功
	service.isRegistered = true

}

func (r *Register) unRegisterService(ctx context.Context, service *discov.Service) {
	endpoints := make([]*discov.Endpoint, 0)
	for _, endpoint := range r.registerServices[service.Name].service.Endpoints {
		var isRemove bool
		for _, unRegisterEndpoint := range service.Endpoints {
			if endpoint.IP == unRegisterEndpoint.IP && endpoint.Port == unRegisterEndpoint.Port { // 检查现有每个 endpoint 是否在 service.Endpoints（待删除列表）中
				_, err := r.cli.Delete(context.TODO(), r.getEtcdRegisterKey(service.Name, endpoint.IP, endpoint.Port))
				if err != nil {
					logger.CtxErrorf(ctx, "UnRegisterService etcd del err, service %v was not registered", service.Name)
				}
				isRemove = true
				break
			}
		}

		if !isRemove { // 该端点没被删
			endpoints = append(endpoints, endpoint)
		}
	}

	if len(endpoints) == 0 { // 该服务没有端点存在，则删除服务
		delete(r.registerServices, service.Name)
		r.watchServices.Delete(service.Name) // 移除 watch 记录
	} else { // 该服务还有端点存在，更新服务端点列表
		r.registerServices[service.Name].service.Endpoints = endpoints
	}
}

func (r *Register) KeepAlive(ctx context.Context, service *registerService) {
	for {
		select {
		case kaResp, ok := <-service.keepAliveCh:
			if !ok {
				// keepAliveCh 关闭，说明续约失败，需重新注册
				logger.CtxErrorf(ctx, "KeepAlive failed: lease expired, re-registering service: %s", service.service.Name)
				r.registerService(ctx, service) // 重新注册服务
				return
			}
			if kaResp == nil {
				// keepAliveCh 收到 nil，表示 etcd 可能异常
				logger.CtxErrorf(ctx, "KeepAlive received nil response, re-registering service: %s", service.service.Name)
				r.registerService(ctx, service) // 重新注册服务
				return
			}
			// 成功收到续约响应，继续循环等待下一个续约信号
			// 每当租约续约成功时，etcd 会在 keepAliveCh 中发送一条响应消息。
			// 具体的发送过程由 etcd 客户端库内部处理，我们并不需要显式地发送消息。
			logger.CtxInfof(ctx, "KeepAlive success: lease ID %d", kaResp.ID)

		case <-ctx.Done():
			// 上下文被取消，停止续约
			logger.CtxInfof(ctx, "KeepAlive stopped: context canceled for service %s", service.service.Name)
			return
		}
	}
}

func (r *Register) Name() string {
	return "etcd"
}

func (r *Register) AddListener(ctx context.Context, f func()) {
	r.listeners = append(r.listeners, f)
}

func (r *Register) NotifyListeners() {
	for _, listener := range r.listeners {
		listener()
	}
}

func (r *Register) Register(ctx context.Context, service *discov.Service) {
	r.serviceRegisterCh <- service
}

func (r *Register) UnRegister(ctx context.Context, service *discov.Service) {
	r.serviceUnRegisterCh <- service
}

func (r *Register) GetService(ctx context.Context, name string) *discov.Service {
	allServices := r.getDownServices()    // 从本地缓存获取服务，避免频繁访问 etcd
	if val, ok := allServices[name]; ok { // 如果本地缓存中存在该服务，则直接返回
		return val
	}

	// 防止并发获取service导致cache中的数据混乱
	r.lock.Lock()
	defer r.lock.Unlock()

	key := r.getEtcdRegisterPrefixKey(name) //  查询 etcd 获取服务实例
	getResp, err := r.cli.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		logger.CtxErrorf(ctx, "GetService etcd get error: %v", err)
		return nil
	}
	service := &discov.Service{
		Name:      name,
		Endpoints: make([]*discov.Endpoint, 0),
	}
	// 遍历 getResp.Kvs，解析 endpoint 信息，并加入service.Endpoints
	for _, item := range getResp.Kvs {
		var endpoint discov.Endpoint
		if err := json.Unmarshal(item.Value, &endpoint); err != nil {
			continue
		}

		service.Endpoints = append(service.Endpoints, &endpoint)
	}

	// 将 service 存入 allServices，并更新 downServices 缓存，减少后续 etcd 访问
	allServices[name] = service
	r.downServices.Store(allServices)
	// watch 监听etcd中key变更，从最新 Revision+1 开始监听，防止遗漏更新。
	// 避免重复 watch
	if _, exists := r.watchServices.LoadOrStore(name, struct{}{}); !exists {
		go r.watch(ctx, key, getResp.Header.Revision+1)
	}

	return service
}

// 监听 etcd 中的服务变更事件（新增、修改、删除），并更新本地缓存
func (r *Register) watch(ctx context.Context, key string, revision int64) {
	rch := r.cli.Watch(ctx, key, clientv3.WithRev(revision), clientv3.WithPrefix())
	for n := range rch {
		for _, ev := range n.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				var endpoint discov.Endpoint
				if err := json.Unmarshal(ev.Kv.Value, &endpoint); err != nil {
					continue
				}
				serviceName, _, _ := r.getServiceNameByETCDKey(string(ev.Kv.Key))
				r.updateDownService(&discov.Service{
					Name:      serviceName,
					Endpoints: []*discov.Endpoint{&endpoint},
				})
			case clientv3.EventTypeDelete: //  ! DELETE 事件没有 Value
				// 这里不能 Unmarshal，因为 DELETE 事件没有 Value
				serviceName, ip, port := r.getServiceNameByETCDKey(string(ev.Kv.Key))

				r.delDownService(&discov.Service{
					Name: serviceName,
					Endpoints: []*discov.Endpoint{
						{
							IP:   ip,
							Port: port,
						},
					},
				})
			}
		}
	}
}

func (r *Register) updateDownService(service *discov.Service) {
	r.lock.Lock()
	defer r.lock.Unlock()

	downServices := r.downServices.Load().(map[string]*discov.Service) // 获取本地缓存
	if _, ok := downServices[service.Name]; !ok {                      // 如果 service.Name 不在 downServices 里，说明是新的服务，直接加入并 return
		downServices[service.Name] = service
		r.downServices.Store(downServices)
		return
	}

	for _, newAddEndpoint := range service.Endpoints {
		var isExist bool
		for idx, endpoint := range downServices[service.Name].Endpoints {
			if newAddEndpoint.IP == endpoint.IP && newAddEndpoint.Port == endpoint.Port { // 如果端点已存在（IP + 端口匹配），则更新
				downServices[service.Name].Endpoints[idx] = newAddEndpoint
				isExist = true
				break
			}
		}

		if !isExist { // 如果端点不存在，则追加到 downServices
			downServices[service.Name].Endpoints = append(downServices[service.Name].Endpoints, newAddEndpoint)
		}
	}

	r.downServices.Store(downServices) // 存储更新后的 downServices

	r.NotifyListeners() // 通知订阅者（比如 watchServices）有新的服务变更。
}

func (r *Register) delDownService(service *discov.Service) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// 复制 downServices，确保 atomic.Value 操作的安全性
	oldDownServices := r.downServices.Load().(map[string]*discov.Service)
	newDownServices := make(map[string]*discov.Service, len(oldDownServices))

	for k, v := range oldDownServices {
		newDownServices[k] = v
	}

	// 如果服务不存在，直接返回
	if _, ok := newDownServices[service.Name]; !ok {
		return
	}

	// 复制 Endpoints，防止直接修改原 map
	oldEndpoints := newDownServices[service.Name].Endpoints
	newEndpoints := make([]*discov.Endpoint, 0)

	for _, endpoint := range oldEndpoints {
		var isRemove bool
		for _, delEndpoint := range service.Endpoints {
			if delEndpoint.IP == endpoint.IP && delEndpoint.Port == endpoint.Port {
				isRemove = true
				break
			}
		}

		if !isRemove {
			newEndpoints = append(newEndpoints, endpoint)
		}
	}

	// 如果所有 Endpoints 都被删除，则移除该服务
	if len(newEndpoints) == 0 {
		delete(newDownServices, service.Name)
	} else {
		newDownServices[service.Name] = &discov.Service{
			Name:      service.Name,
			Endpoints: newEndpoints,
		}
	}

	// 更新 downServices
	r.downServices.Store(newDownServices)

	// 通知监听器
	r.NotifyListeners()
}

func (r *Register) getDownServices() map[string]*discov.Service {
	allServices := r.downServices.Load()
	if allServices == nil {
		return make(map[string]*discov.Service, 0)
	}

	return allServices.(map[string]*discov.Service)
}

func (r *Register) getEtcdRegisterKey(name, ip string, port int) string {
	return fmt.Sprintf(KeyPrefix+"%v/%v/%v", name, ip, port)
}

func (r *Register) getEtcdRegisterPrefixKey(name string) string {
	return fmt.Sprintf(KeyPrefix+"%v", name)
}

func (r *Register) getServiceNameByETCDKey(key string) (string, string, int) {
	trimStr := strings.TrimPrefix(key, KeyPrefix)
	strs := strings.Split(trimStr, "/")

	ip, _ := strconv.Atoi(strs[2])
	return strs[0], strs[1], ip
}
