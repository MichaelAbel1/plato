package util

import "net"

const (
	localhost = "127.0.0.1"
)

// ExternalIP 获取ip
func ExternalIP() string {
	ifaces, err := net.Interfaces() // 获取当前机器的所有网络接口
	if err != nil {
		return localhost
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 { // 检查网络接口是否处于启用状态。如果未启用，跳过该接口。
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 { // 检查网络接口是否为回环接口。如果是回环接口，跳过该接口。
			continue // loopback interface
		}
		addrs, err := iface.Addrs() //  获取网络接口的 IP 地址列表
		if err != nil {
			return localhost
		}
		for _, addr := range addrs {
			ip := getIpFromAddr(addr)
			if ip == nil {
				continue
			}
			return ip.String()
		}
	}
	return localhost // 如果没有找到有效的 IP 地址，返回本地 IP 地址。
}

// 获取ip
func getIpFromAddr(addr net.Addr) net.IP {
	var ip net.IP
	switch v := addr.(type) {
	case *net.IPNet:
		ip = v.IP
	case *net.IPAddr:
		ip = v.IP
	}
	if ip == nil || ip.IsLoopback() {
		return nil
	}
	ip = ip.To4()
	if ip == nil {
		return nil // not an ipv4 address
	}

	return ip

}
