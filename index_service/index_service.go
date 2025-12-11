package index_service

import (
	"RADIC/types"
	"RADIC/util"
	"context"
	"fmt"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"strconv"
	"time"
)

// 每一个worker都应该向外提供一个接口用来输出结果，因为结果最终要合并并返回给用户

const (
	INDEX_SERVICE = "index_service"
)

type IServiceHub interface {
	Regist(service string, endpoint string, leaseID etcdv3.LeaseID) (etcdv3.LeaseID, error) //注册服务
	UnRegist(service string, endpoint string) error                                         // 注销服务
	GetServiceEndpoints(service string) []string                                            //服务发现
	GetServiceEndpoint(service string) string                                               //选择服务的一台endpoint
}

type IndexServiceWorker struct {
	Indexer *Indexer

	// 服务注册相关的配置
	hub      *ServiceHub
	selfAddr string
}

// Init 初始化索引
func (service *IndexServiceWorker) Init(DocNumEstimate int, dbtype int, DataDir string, etcdServers []string, servicePort int) error {
	service.Indexer = new(Indexer)
	service.Indexer.Init(DocNumEstimate, dbtype, DataDir)

	// 向注册中心注册自己
	if len(etcdServers) > 0 {
		if servicePort <= 1024 {
			return fmt.Errorf("invalid listen port %d, should more than 1204", servicePort)
		}
		selfLocalIp, err := util.GetLocalIP()
		if err != nil {
			panic(err)
		}
		selfLocalIp = "127.0.0.1" // TODO 单机模拟分布式，写死127.0.0.1

		service.selfAddr = selfLocalIp + ":" + strconv.Itoa(servicePort)

		var heartBeat int64 = 3
		hub := GetServiceHub(etcdServers, heartBeat)
		leaseId, err := hub.Regist(INDEX_SERVICE, service.selfAddr, 0)

		if err != nil {
			panic(err)
		}

		service.hub = hub

		// 周期性注册自己 (上报心跳)
		go func() {
			for {
				hub.Regist(INDEX_SERVICE, service.selfAddr, leaseId)
				time.Sleep(time.Duration(heartBeat)*time.Second - 100*time.Millisecond) // 比到期时间稍微短一点
			}
		}()
	}
	return nil
}

// LoadFromIndexFile 系统重启时，直接从索引文件里加载数据
func (service *IndexServiceWorker) LoadFromIndexFile() int {
	return service.Indexer.LoadFromIndexFile()
}

// Close 关闭索引
func (service *IndexServiceWorker) Close() error {
	if service.hub != nil {
		service.hub.UnRegist(INDEX_SERVICE, service.selfAddr)
	}
	return service.Indexer.Close()
}

// DeleteDoc 从索引上删除文档
func (service *IndexServiceWorker) DeleteDoc(ctx context.Context, docId *DocId) (*AffectedCount, error) {
	return &AffectedCount{int32(service.Indexer.DeleteDoc(docId.DocId))}, nil
}

// AddDoc 向索引中增加文档(如果已存在，会先删除)
func (service *IndexServiceWorker) AddDoc(ctx context.Context, doc *types.Document) (*AffectedCount, error) {
	n, err := service.Indexer.AddDoc(*doc)
	return &AffectedCount{int32(n)}, err
}

func (service *IndexServiceWorker) Search(ctx context.Context, request *SearchRequest) (*SearchResult, error) {
	result := service.Indexer.Search(request.Query, request.OnFlag, request.OffFlag, request.OrFlags)
	return &SearchResult{Results: result}, nil

}
