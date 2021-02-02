package util

func DeepCopyMap(m map[int][]string) map[int][]string {
	newMap := make(map[int][]string)
	for k, v := range m {
		newSlice := make([]string, len(v))
		copy(newSlice, v)
		newMap[k] = newSlice
	}
	return newMap
}
