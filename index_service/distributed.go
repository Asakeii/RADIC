package index_service

import (
	"RADIC/types"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

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

func (sentinel *Sentinel) GetGrpcConn(endpoint string) *grpc.ClientConn {
	if v, exists := sentinel.connPool.Load(endpoint); exists {
		conn := v.(*grpc.ClientConn)
		// 如果连接状态不可用，则从连接缓存中删除
		if conn.GetState() == connectivity.TransientFailure || conn.GetState() == connectivity.Shutdown {
			slog.Warn("connection status to endpoint %s is %s", endpoint, conn.GetState())
			sentinel.connPool.Delete(endpoint)
		} else {
			return conn
		}
	}
	// 连接到服务器
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	conn, err := grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		slog.Warn("dial failed", slog.Any("endpoint", endpoint), slog.Any("err", err))
		return nil
	}
	slog.Info("connect to grpc server", slog.Any("endpoint", endpoint))
	sentinel.connPool.Store(endpoint, conn)
	return conn
}

// 向集群中添加文档（如果已存在，会先删除）
func (sentinel *Sentinel) AddDoc(doc types.Document) (int, error) {
	endpoint := sentinel.hub.GetServiceEndpoint(INDEX_SERVICE) // 根据负载均衡策略，选择一台worker
	if len(endpoint) == 0 {
		return 0, fmt.Errorf("there is no alive index worker")
	}
	conn := sentinel.GetGrpcConn(endpoint)
	if conn == nil {
		return 0, fmt.Errorf("connect to worker failed", endpoint)
	}
	client := NewIndexServiceClient(conn)
	affented, err := client.AddDoc(context.Background(), &doc)
	if err != nil {
		return 0, err
	}
	slog.Info("add doc to worker", slog.Any("affectedCount", affented.Count), slog.Any("endpoint", endpoint))
	return int(affented.Count), nil
}

// DeleteDoc 从集群上删除docId，返回成功删除的doc数（正常情况下不会超过1）
func (sentinel *Sentinel) DeleteDoc(docId string) int {
	endpoints := sentinel.hub.GetServiceEndpoints(INDEX_SERVICE)
	if len(endpoints) == 0 {
		return 0
	}
	var n int32
	wg := sync.WaitGroup{}
	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		go func(endpoint string) {
			defer wg.Done()
			conn := sentinel.GetGrpcConn(endpoint)
			if conn != nil {
				client := NewIndexServiceClient(conn)
				affected, err := client.DeleteDoc(context.Background(), &DocId{docId})
				if err != nil {
					slog.Warn("delete doc from worker failed", docId, err)
				} else {
					if affected.Count > 0 {
						atomic.AddInt32(&n, affected.Count)
						slog.Info("delete affectedCount from worker", slog.Any("affectedCount", affected.Count), slog.Any("endpoint", endpoint))
					}
				}
			}
		}(endpoint)
	}
	wg.Wait()
	return int(atomic.LoadInt32(&n))
}

func (sentinel *Sentinel) Search(query *types.TermQuery, onFlag uint64, offFlag uint64, orFlag []uint64) []*types.Document {
	endpoints := sentinel.hub.GetServiceEndpoints(INDEX_SERVICE)
	if len(endpoints) == 0 {
		return nil
	}
	docs := make([]*types.Document, 0, 1000)
	resultCh := make(chan *types.Document, 1000)
	wg := sync.WaitGroup{}
	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		func(endpoint string) {
			defer wg.Done()
			conn := sentinel.GetGrpcConn(endpoint)
			if conn != nil {
				client := NewIndexServiceClient(conn)
				result, err := client.Search(context.Background(), &SearchRequest{Query: query})
				if err != nil {
					slog.Info("search from cluster failed", slog.Any("err", err))
				} else {
					if len(result.Results) > 0 {
						slog.Info("search doc from worker", slog.Any("length", result))
						for _, doc := range result.Results {
							resultCh <- doc
						}
					}
				}
			}
		}(endpoint)
	}

	receiveFinish := make(chan struct{})
	go func() {
		for {
			doc, ok := <-resultCh
			if !ok {
				break
			}
			docs = append(docs, doc)
		}
		receiveFinish <- struct{}{}
	}()

	wg.Wait()
	close(resultCh)
	<-receiveFinish
	return docs

}
