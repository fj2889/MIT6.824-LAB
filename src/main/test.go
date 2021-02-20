package main

import (
	"fmt"
	"sync"
)

type myStruct struct {
	Key   string
	Value int
	x     map[string]int
	y     []string
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
	t := make(map[string]myStruct)
	t["a"] = myStruct{
		y: []string{
			"1", "2",
		},
	}
	b := t["a"]
	fmt.Println(t["a"].x["b"])
	delete(t, "a")
	fmt.Println(b, t)
}
