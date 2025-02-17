package discovery

import (
	"context"
	"log"

	"github.com/bytedance/gopkg/util/logger"
	"github.com/hardcore-os/plato/common/config"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ServiceRegister 创建租约注册服务
type ServiceRegister struct {
	cli     *clientv3.Client //etcd client
	leaseID clientv3.LeaseID //租约ID
	//租约keepalieve相应chan
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	key           string //key
	val           string //value
	ctx           *context.Context
}

// NewServiceRegister 新建注册服务
func NewServiceRegister(ctx *context.Context, key string, endportinfo *EndpointInfo, lease int64) (*ServiceRegister, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.GetEndpointsForDiscovery(),
		DialTimeout: config.GetTimeoutForDiscovery(),
	})
	if err != nil {
		log.Fatal(err)
	}

	ser := &ServiceRegister{
		cli: cli,
		key: key,
		val: endportinfo.Marshal(),
		ctx: ctx,
	}

	//申请租约设置时间keepalive
	if err := ser.putKeyWithLease(lease); err != nil {
		return nil, err
	}

	return ser, nil
}

// 设置租约
func (s *ServiceRegister) putKeyWithLease(lease int64) error {
	//设置租约时间
	resp, err := s.cli.Grant(*s.ctx, lease) // 授予某个实体（例如服务实例）在一段时间内使用某个资源的权利。当租约到期时，该实体将失去对资源的访问权限
	if err != nil {
		return err
	}
	//注册服务并绑定租约
	_, err = s.cli.Put(*s.ctx, s.key, s.val, clientv3.WithLease(resp.ID)) // 将一个键值对与一个特定的租约进行绑定，对应mvccpb.PUT操作类型
	if err != nil {
		return err
	}
	//设置续租 定期发送需求请求
	// 虽然lease为time.Now().Unix() 设置的租约时间极长，实际上等于 不使用租约的概念，那么续约的操作可能变得没有意义
	// 但是如果删去这一行，就无法保证服务失效时无法及时清除 无法定期检查服务是否仍然有效
	// 假设你设置了一个 10 秒的租约：
	// 第一次 KeepAlive 会让租约有效期从当前时间延长 10 秒。
	// 如果你每 5 秒就调用一次 KeepAlive，那么租约的有效期将被每次延长 10 秒（因为租约的 TTL 被重置）
	leaseRespChan, err := s.cli.KeepAlive(*s.ctx, resp.ID)
	if err != nil {
		return err
	}
	s.leaseID = resp.ID
	s.keepAliveChan = leaseRespChan
	return nil
}

func (s *ServiceRegister) UpdateValue(val *EndpointInfo) error {
	value := val.Marshal()
	_, err := s.cli.Put(*s.ctx, s.key, value, clientv3.WithLease(s.leaseID))
	if err != nil {
		return err
	}
	s.val = value
	logger.CtxInfof(*s.ctx, "ServiceRegister.updateValue leaseID=%d Put key=%s,val=%s, success!", s.leaseID, s.key, s.val)
	return nil
}

// ListenLeaseRespChan 监听 续租情况
func (s *ServiceRegister) ListenLeaseRespChan() {
	for leaseKeepResp := range s.keepAliveChan {
		logger.CtxInfof(*s.ctx, "lease success leaseID:%d, Put key:%s,val:%s reps:+%v",
			s.leaseID, s.key, s.val, leaseKeepResp)
	}
	logger.CtxInfof(*s.ctx, "lease failed !!!  leaseID:%d, Put key:%s,val:%s", s.leaseID, s.key, s.val)
}

// Close 注销服务
func (s *ServiceRegister) Close() error {
	//撤销租约
	if _, err := s.cli.Revoke(context.Background(), s.leaseID); err != nil {
		return err
	}
	logger.CtxInfof(*s.ctx, "lease close !!!  leaseID:%d, Put key:%s,val:%s  success!", s.leaseID, s.key, s.val)
	return s.cli.Close()
}
