package index_service

import (
	"context"
	"errors"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"log/slog"
	"strings"
	"sync"
	"time"
)

const (
	SERVICE_ROOT_PATH = "/radic/index" // etcd key的前缀
)

// ServiceHub 服务注册中心
type ServiceHub struct {
	client             *etcdv3.Client
	heartbeatFrequency int64 // 心跳频率
	watched            sync.Map
	loadBalancer       LoadBalancer // 负载均衡器：策略模式
}

var (
	serviceHub *ServiceHub // 该全局变量包外不可见，包外想使用时通过GetServiceHub()获得
	hubOnce    sync.Once   // 单例模式
)

// GetServiceHub 服务注册中心初始化
func GetServiceHub(etcdServers []string, heartbeatFrequency int64) *ServiceHub {
	if serviceHub == nil {
		hubOnce.Do(func() {
			if client, err := etcdv3.New(
				etcdv3.Config{
					Endpoints:   etcdServers,
					DialTimeout: 3 * time.Second,
				}); err != nil {
				slog.Error("连接不上etcd服务器", slog.Any("err", err))
			} else {
				serviceHub = &ServiceHub{
					client:             client,
					heartbeatFrequency: heartbeatFrequency,
					loadBalancer:       &RoundRobin{},
				}
			}
		})
	}
	return serviceHub
}

// 注册服务 第一次注册向etcd写一个key，后续注册仅仅是在续约

// service 微服务的名称
// endpoint 微服务server的地址
// leaseID 租约ID，第一次注册时置为0即可

func (hub *ServiceHub) Regist(service string, endpoint string, leaseID etcdv3.LeaseID) (etcdv3.LeaseID, error) {
	ctx := context.Background()
	if leaseID <= 0 {
		// 创建一个租约，有效期是heartbeatFrequency秒
		if lease, err := hub.client.Grant(ctx, hub.heartbeatFrequency); err != nil {
			slog.Error("创建租约失败", slog.Any("err", err))
			return 0, err
		} else {
			key := strings.TrimRight(SERVICE_ROOT_PATH, "/") + "/" + service + "/" + endpoint
			// 服务注册
			if _, err = hub.client.Put(ctx, key, "", etcdv3.WithLease(lease.ID)); err != nil {
				slog.Warn("写入服务节点失败", slog.Any("service", service), slog.Any("endpoint", endpoint), slog.Any("err", err))
				return lease.ID, err
			} else {
				return lease.ID, nil
			}
		}
	} else {
		// 续租
		if _, err := hub.client.KeepAliveOnce(ctx, leaseID); errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return hub.Regist(service, endpoint, 0) // 找不到租约，走注册流程
		} else if err != nil {
			slog.Warn("续约失败", slog.Any("err", err))
			return 0, err
		} else {
			return leaseID, nil
		}
	}
}

// UnRegist 注销服务
func (hub *ServiceHub) UnRegist(service string, endpoint string) error {
	ctx := context.Background()
	key := strings.TrimRight(SERVICE_ROOT_PATH, "/") + "/" + service + "/" + endpoint
	if _, err := hub.client.Delete(ctx, key); err != nil {
		slog.Warn("注销服务节点失败", slog.Any("service", service), slog.Any("endpoint", endpoint), slog.Any("err", err))
		return err
	} else {
		slog.Info("注销服务节点", slog.Any("service", service), slog.Any("endpoint", endpoint))
		return nil
	}
}

// GetServiceEndpoints 服务发现  client每次进行RPC调用之前都查询etcd，获取service集合，然后采用负载均衡算法选择一台service
func (hub *ServiceHub) GetServiceEndpoints(service string) []string {
	ctx := context.Background()
	prefix := strings.TrimRight(SERVICE_ROOT_PATH, "/") + "/" + service + "/"
	if resp, err := hub.client.Get(ctx, prefix, etcdv3.WithPrefix()); err != nil {
		slog.Warn("获取服务节点失败", slog.Any("service", service), slog.Any("err", err))
		return nil
	} else {
		endpoints := make([]string, 0, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			path := strings.Split(string(kv.Key), "/")
			endpoints = append(endpoints, path[len(path)-1])
		}
		slog.Info("刷新服务对应的server",
			slog.Any("service", service),
			slog.Any("endpoints", endpoints))
		return endpoints
	}

}
