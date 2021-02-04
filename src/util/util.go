package util

import (
	"sort"
)

func DeepCopyMap(m map[int][]string) map[int][]string {
	newMap := make(map[int][]string)
	for k, v := range m {
		newSlice := make([]string, len(v))
		copy(newSlice, v)
		newMap[k] = newSlice
	}
	return newMap
}

func GetKeyOfMap(m map[int][]string) []int {
	var result []int
	for k, _ := range m {
		result = append(result, k)
	}
	sort.Ints(result)
	return result
}
