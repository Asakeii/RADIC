package util

import (
	"fmt"
	"net"
	"strings"
)

// GetLocalIP 返回本机首选的内网 IPv4 地址（优先 10.x/192.168.x/172.16-31.x）
// 如果都拿不到就返回第一个非 loopback 的 IPv4
func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	var candidate string

	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			ip := ipNet.IP.String()

			// 优先返回常见内网段
			if strings.HasPrefix(ip, "192.168.") ||
				strings.HasPrefix(ip, "10.", "10.") ||
				(strings.HasPrefix(ip, "172.") && func() bool {
					// 172.16.0.0 - 172.31.255.255
					second, _ := ipNet.IP[1]
					return second >= 16 && second <= 31
				}()) {
				return ip, nil
			}

			// 其它私有地址先记着，后面万一没有就用它
			if candidate == "" {
				candidate = ip
			}
		}
	}

	if candidate != "" {
		return candidate, nil
	}

	return "", fmt.Errorf("no available local IPv4 address")
}
