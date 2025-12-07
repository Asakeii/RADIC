package test

import (
	"RADIC/util"
	"math/rand/v2"
	"strconv"
	"sync"
	"testing"
)

/*
goos: windows
goarch: amd64
pkg: RADIC/util/test
cpu: Intel(R) Core(TM) i5-10300H CPU @ 2.50GHz
BenchmarkConMap
BenchmarkConMap-8   	       2	 594916400 ns/op	587564104 B/op	 5995534 allocs/op
BenchmarkSynMap
BenchmarkSynMap-8   	       3	 986926833 ns/op	433386824 B/op	12956470 allocs/op
PASS
*/

var conMp = util.NewConcurrentHashMap(64, 1000) // sey数量稍微大一点，性能才能好
var synMp = sync.Map{}

func readConMap() {
	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(i)
		conMp.Get(key)
	}
}

func writeConMap() {
	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(rand.Int())
		conMp.Set(key, 1)
	}
}

func readSynMap() {
	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(i)
		synMp.Load(key)
	}
}

func writeSynMap() {
	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(rand.Int())
		synMp.Store(key, 1)
	}
}

// BenchmarkConMap 对ConcurrentHashMap进行并发基准测试
func BenchmarkConMap(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		const P = 300
		wg := sync.WaitGroup{}
		wg.Add(2 * P)
		for i := 0; i < P; i++ {
			go func() {
				defer wg.Done()
				readConMap()
			}()
		}
		for i := 0; i < P; i++ {
			go func() {
				defer wg.Done()
				writeConMap()
			}()
		}
		wg.Wait()
	}
}

// BenchmarkConMap 对syncMap进行并发基准测试
func BenchmarkSynMap(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		const P = 300
		wg := sync.WaitGroup{}
		wg.Add(2 * P)
		for i := 0; i < P; i++ {
			go func() {
				defer wg.Done()
				readSynMap()
			}()
		}
		for i := 0; i < P; i++ {
			go func() {
				defer wg.Done()
				writeSynMap()
			}()
		}
		wg.Wait()
	}
}
