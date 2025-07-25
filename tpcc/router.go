package tpcc

import (
	"fmt"
	"strconv"
	"strings"
)

var (
	AllServers  map[string]struct{}
	WIDRouteMap map[int]string
)

func init() {
	AllServers = make(map[string]struct{})
	WIDRouteMap = make(map[int]string)
	for _, line := range strings.Split(routeData, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		tmp := strings.Split(line, "|")
		pName := strings.TrimSpace(tmp[2])
		pID, err := strconv.Atoi(pName[1:])
		if err != nil {
			panic("Invalid partition name: " + pName)
		}
		addr := strings.TrimSpace(tmp[3])
		addr = fmt.Sprintf("%v:4002", strings.Split(addr, ":")[0])
		AllServers[addr] = struct{}{}
		WIDRouteMap[pID] = addr
	}

	fmt.Println("================================ route info ======================================")
	fmt.Println("All Servers:", AllServers)
	for wid, addr := range WIDRouteMap {
		fmt.Printf("WID %d -> %s\n", wid, addr)
	}
	fmt.Println("================================ route info ======================================")
}

// store_id | partition_name | partition_addr
var routeData = `
|        7 | p0             | 192.168.173.161:25360 |
|        6 | p1             | 192.168.173.159:25360 |
|        7 | p10            | 192.168.173.161:25360 |`
