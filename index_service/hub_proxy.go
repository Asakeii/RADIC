package index_service

import (
	"context"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// 结构体里使用匿名变量可以实现继承父类

// HubProxy 代理模式：对ServiceHub做一层代理，想访问endpoints时需要通过代理，代理提供了2个功能:缓存和限流保护
type HubProxy struct {
	hub           *ServiceHub
	endpointCache sync.Map // 本地缓存，用于存储查询的服务
	limiter       *rate.Limiter
	LoadBalancer  LoadBalancer
}

var (
	proxy     *HubProxy
	proxyOnce sync.Once
)

// HubProxy的构造函数,单例模式
// qps 一秒钟最多允许多少次请求

// GetServiceHubProxy HubProxy的构造函数
func GetServiceHubProxy(etcdServers []string, heartbeatFrequency int64, qps int) *HubProxy {
	if proxy == nil {
		proxyOnce.Do(func() {
			serviceHub := GetServiceHub(etcdServers, heartbeatFrequency)
			if serviceHub != nil {
				proxy = &HubProxy{
					hub:           serviceHub,
					endpointCache: sync.Map{},
					limiter:       rate.NewLimiter(rate.Every(time.Duration(1e9/qps)*time.Nanosecond), qps),
					// 每个1e9/qps纳秒产生一个令牌，即1s内产生qps个令牌， 令牌桶容量是qps
				}
			}
		})
	}
	return proxy
}

// Regist 注册服务
func (proxy *HubProxy) Regist(service string, endpoint string, leaseID etcdv3.LeaseID) (etcdv3.LeaseID, error) {
	return proxy.hub.Regist(service, endpoint, leaseID)
}

// UnRegist 注销服务
func (proxy *HubProxy) UnRegist(service string, endpoint string) error {
	return proxy.hub.UnRegist(service, endpoint)
}

func (proxy *HubProxy) watchEndpointsOfService(service string) {
	if _, exsits := proxy.hub.watched.LoadOrStore(service, true); exsits {
		return // 监听过了，不用反复监听
	}
	ctx := context.Background()
	prefix := strings.TrimRight(SERVICE_ROOT_PATH, "/") + "/" + service + "/"
	ch := proxy.hub.client.Watch(ctx, prefix, etcdv3.WithPrefix()) // 根据前缀监听，每个修改都会放入管道ch
	slog.Info("简体服务的节点变化", slog.Any("service", service))

	go func() {
		for response := range ch { // 遍历管道，这是个死循环，除非关闭管道
			for _, event := range response.Events { // 每次从ch里取出来的时间的集合
				path := strings.Split(string(event.Kv.Key), "/")
				if len(path) > 2 {
					service := path[len(path)-2]
					// 跟etcd进行一次全量同步
					endpoints := proxy.hub.GetServiceEndpoints(service)
					if len(endpoints) > 0 {
						proxy.endpointCache.Store(service, endpoints)
						// 查询etcd的结果放入本地缓存
					} else {
						proxy.endpointCache.Delete(service)
						// 该service下已经没有endpoint
					}

				}
			}
		}
	}()

} // 监听etcd的数据变化，及时更新本地缓存

func (proxy *HubProxy) GetServiceEndpoints(service string) []string {
	if !proxy.limiter.Allow() {
		return nil
	}

	proxy.watchEndpointsOfService(service) // 监听etcd的数据变化，及时更新本地缓存

	if endpoints, exist := proxy.endpointCache.Load(service); exist {
		return endpoints.([]string)
	} else {
		endpoints := proxy.hub.GetServiceEndpoints(service)
		if len(endpoints) > 0 {
			proxy.endpointCache.Store(service, endpoints) // 查询etcd的结果放入本地缓存
		}
		return endpoints
	}
}

func (proxy *HubProxy) GetServiceEndpoint(service string) string {
	return proxy.hub.GetServiceEndPoint(service)
}
