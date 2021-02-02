package main

import (
	"fmt"
	"sync"
	"util"
)

type myStruct struct {
	Key   string
	Value string
}

func concurrentTest(index *int, mu *sync.Mutex) {
	for i := 0; i < 5; i++ {
		go func() {
			mu.Lock()
			(*index)++
			mu.Unlock()
		}()
	}
}

func main() {
	//file, err := os.OpenFile("mytest", os.O_RDWR, os.ModePerm)
	//if err != nil {
	//	fmt.Println(err)
	//}
	//myJson := myStruct{
	//	Key:   "123",
	//	Value: "321",
	//}
	//enc := json.NewEncoder(file)
	//err = enc.Encode(myJson)
	//if err != nil {
	//	fmt.Println(err)
	//}
	//fmt.Printf("file path: %v\n", path.Join(file.Name()))
	//err = file.Close()
	//if err != nil {
	//	fmt.Println(err)
	//}
	x := map[string][]string{
		"a": []string{"a", "b", "c"},
		"b": []string{"b", "c", "d"},
	}
	y := util.DeepCopy(x)
	x["a"] = []string{"a"}
	fmt.Println(x, y)
}
