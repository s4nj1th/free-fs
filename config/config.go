package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ChunkSize         = 1024 * 1024 // 1MB
	ReplicationFactor = 3
	HeartbeatInterval = 2 * time.Second
	HeartbeatTimeout  = 10 * time.Second
)

var Nodes = map[int]string{
	1: "172.28.0.11:50051", // Chunkserver 1
	2: "172.28.0.12:50051", // Chunkserver 2
	3: "172.28.0.13:50051", // Chunkserver 3
	4: "172.28.0.14:50051", // Master
}

func init() {
	if env := os.Getenv("FREEFS_NODES"); env != "" {
		newNodes := make(map[int]string)
		parts := strings.Split(env, ",")
		for _, part := range parts {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				id, err := strconv.Atoi(kv[0])
				if err == nil {
					newNodes[id] = kv[1]
				}
			}
		}
		if len(newNodes) > 0 {
			Nodes = newNodes
		}
	}
}
