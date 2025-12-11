package course

import (
	"golang.org/x/time/rate"
	"time"
)

var TotalQuery int32

func Handler() {
	// TODO 消费令牌
}

func CallHandler() {
	limiter := rate.NewLimiter(rate.Every(100*time.Millisecond), 1) // 每100s生成一个令牌
	n := 3
	for {
		reserve := limiter.ReserveN(time.Now(), n)
		time.Sleep(reserve.Delay()) // reserve.Delay()会告诉还需要多久会有充足的令牌
		Handler()
	}

}
