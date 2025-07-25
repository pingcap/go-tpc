package tpcc

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

var (
	AllServers map[string]struct{}
	PID2Addr   map[int]string
	Addr2PID   = make(map[string][]int) // for reverse lookup, not used in this example
)

func genWID(addr string) int {
	pidList := Addr2PID[addr]
	pid := pidList[rand.Intn(len(pidList))]
	wid := pid + 200*rand.Intn(5)
	return wid % 1000
}

func init() {
	data, err := os.ReadFile("./route.txt")
	if err != nil {
		fmt.Println("read route.txt err:", err)
		return
	}
	routeData := string(data)
	AllServers = make(map[string]struct{})
	PID2Addr = make(map[int]string)
	Addr2PID = make(map[string][]int)
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
		PID2Addr[pID] = addr
		if _, ok := Addr2PID[addr]; !ok {
			Addr2PID[addr] = []int{}
		}
		Addr2PID[addr] = append(Addr2PID[addr], pID)
	}
	fmt.Println("============================== route info ==============================")
	fmt.Println("AllServers:", AllServers)
	for addr, pids := range Addr2PID {
		fmt.Println("Addr:", addr, "PIDs:", pids)
	}
	fmt.Println("============================== route info ==============================")
}
