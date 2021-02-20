package shardkv

import (
	"../shardmaster"
	"encoding/json"
)

func Min(x int, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}

func Max(x int, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

func deepCopyConfig(src shardmaster.Config) shardmaster.Config {
	buffer, _ := json.Marshal(src)
	var dst shardmaster.Config
	_ = json.Unmarshal(buffer, &dst)
	return dst
}

func deepCopyShard(src Shard) Shard {
	buffer, _ := json.Marshal(src)
	var dst Shard
	_ = json.Unmarshal(buffer, &dst)
	return dst
}

func deepCopyShardData(src map[string]string) map[string]string {
	buffer, _ := json.Marshal(src)
	var dst map[string]string
	_ = json.Unmarshal(buffer, &dst)
	return dst
}

func deepCopyClientRequestSeq(src map[int64]int32) map[int64]int32 {
	buffer, _ := json.Marshal(src)
	var dst map[int64]int32
	_ = json.Unmarshal(buffer, &dst)
	return dst
}
