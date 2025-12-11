package index_service

import "sync"

type Sentinel struct {
	hub      IServiceHub
	connPool sync.Map
}

func NewSentinel(etcdServers []string) *Sentinel {
	return &Sentinel{

		hub:      GetServiceHubProxy(etcdServers, 3, 100), //走代理
		connPool: sync.Map{},
	}
}
