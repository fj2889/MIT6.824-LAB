package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		taskInfo := CallAskTask()
		switch taskInfo.State {
		case TaskMap:
			fmt.Printf("worker: get a map task - %v\n", taskInfo.MapIndex)
			executeMap(mapf, taskInfo)
		case TaskReduce:
			fmt.Printf("worker: get a reduce task - %v\n", taskInfo.ReduceIndex)
			executeReduce(reducef, taskInfo)
		case TaskWait:
			fmt.Println("worker: get a wait task")
			time.Sleep(time.Duration(5) * time.Second)
		case TaskEnd:
			fmt.Println("worker: all task done, time to quit")
			return
		}
	}
}

func executeMap(mapf func(string, string) []KeyValue, taskInfo TaskInfo) {
	// execute map function
	filename := taskInfo.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermidiate := mapf(filename, string(content))

	// output the intermediate result to nReduce files
	outfiles := make([]*os.File, taskInfo.NReduce)
	outenc := make([]*json.Encoder, taskInfo.NReduce)
	for i := 0; i < taskInfo.NReduce; i++ {
		outfiles[i], err = ioutil.TempFile("", "mr-temp-*")
		if err != nil {
			fmt.Printf("worker: cannot create temp file for mr-%v-%v\n", taskInfo.MapIndex, i)
		}
		outenc[i] = json.NewEncoder(outfiles[i])
	}
	for _, kva := range intermidiate {
		outIndex := ihash(kva.Key) % taskInfo.NReduce
		err := outenc[outIndex].Encode(kva)
		if err != nil {
			fmt.Printf("worker: encode the key %v and the value %v in the file %v failed.\n",
				kva.Key, kva.Value, taskInfo.Filename)
			panic("encode failed")
		}
	}

	// rename these temp files
	for i := 0; i < taskInfo.NReduce; i++ {
		outname := "mr-" + strconv.Itoa(taskInfo.MapIndex) + "-" + strconv.Itoa(i)
		err = os.Rename(path.Join(outfiles[i].Name()), path.Join(outname))
		if err != nil {
			fmt.Printf("worker: cannot rename temp file %v to %v\n", outfiles[i].Name(), outname)
			panic("file close failed")
		}
		err := outfiles[i].Close()
		if err != nil {
			fmt.Printf("worker: cannot close file %v\n", outfiles[i].Name())
			panic("file close failed")
		}
	}

	// tell master that map task have done
	fmt.Printf("worker: complete map task - %v\n", taskInfo.MapIndex)
	CallTellDone(taskInfo)
}

func executeReduce(reducef func(string, []string) string, taskInfo TaskInfo) {
	// get key value from intermediate files
	var intermediate []KeyValue
	for i := 0; i < taskInfo.NFile; i++ {
		innFileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskInfo.ReduceIndex)
		innFile, err := os.Open(innFileName)
		if err != nil {
			fmt.Printf("worker: cannot open file %v\n", innFileName)
			panic("cannot open intermediate file")
		}
		dec := json.NewDecoder(innFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// sort
	sort.Sort(ByKey(intermediate))

	// output reduce result file
	oname := "mr-out-" + strconv.Itoa(taskInfo.ReduceIndex)
	ofile, _ := ioutil.TempFile(".", "mr-out-temp-*")

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, _ = fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	_ = os.Rename(path.Join(ofile.Name()), oname)
	_ = ofile.Close()

	fmt.Printf("worker: complete reduce task - %v\n", taskInfo.ReduceIndex)
	CallTellDone(taskInfo)
}

func CallTellDone(taskinfo TaskInfo) {
	request := taskinfo
	response := TaskInfo{}
	call("Master.TellDone", &request, &response)
}

func CallAskTask() TaskInfo {
	request := TaskInfo{}
	response := TaskInfo{}
	call("Master.AskForTask", &request, &response)
	return response
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
