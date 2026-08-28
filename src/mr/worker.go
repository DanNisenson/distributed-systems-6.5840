package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var (
	coordSockName string // socket for coordinator
	workerId      string
	mapfn         func(string, string) []KeyValue
	reducefn      func(string, []string) string
)

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerId = generateId(10)
	coordSockName = sockname
	mapfn = mapf
	reducefn = reducef

	run()
}

func run() {
	task := getTask()

	switch task.Type {

	// poll coordinator
	case TaskTypeWait:
		log.Printf("[%s] worker=%s status=wait",
			time.Now().Format("15:04:05.000"), workerId)
		time.Sleep(1 * time.Second)
		run()

	// exit program
	case TaskTypeExit:
		log.Printf("[%s] worker=%s status=exit",
			time.Now().Format("15:04:05.000"), workerId)
		os.Exit(0)
	}

	switch task.Type {

	case TaskTypeMap:
		start := time.Now()
		content, err := readFile(task.Input)
		if err != nil {
			// ... handle error
		} else {
			log.Printf("[%s] worker=%s task=%s type=%s file=%s elapsed=%d status=file_read",
				time.Now().Format("15:04:05.000"), workerId, task.Id, task.Type, task.Input, time.Since(start).Milliseconds())
		}
		result := mapfn(task.Input, string(content))
		handleMapResult(result, task.Id, task.NReduce)

	case TaskTypeReduce:
		matches, err := filepath.Glob("./" + task.Input)
		if err != nil {
			log.Fatalf("[%s] worker=%s task=%s type=%s file=%s reason=%s status=FATAL_ERROR",
				time.Now().Format("15:04:05.000"), workerId, task.Id, task.Type, task.Input, err)
			return
		}

		log.Printf("[%s] worker=%s task=%s type=%s files=%s status=reading_buckets",
			time.Now().Format("15:04:05.000"), workerId, task.Id, task.Type, task.Input)

		var kvs []KeyValue
		for _, filename := range matches {
			bytes, err := readFile(filename)
			if err != nil {
				log.Fatalf("[%s] worker=%s task=%s type=%s file=%s reason=%s status=FATAL_ERROR",
					time.Now().Format("15:04:05.000"), workerId, task.Id, task.Type, task.Input, err)
				return
			}
			var content []KeyValue
			json.Unmarshal(bytes, &content)
			kvs = append(kvs, content...)
		}

		log.Printf("[%s] worker=%s task=%s type=%s status=grouping",
			time.Now().Format("15:04:05.000"), workerId, task.Id, task.Type)

		// map[Key][]Value
		groups := make(map[string][]string)
		for _, kv := range kvs {
			if groups[kv.Key] != nil {
				groups[kv.Key] = append(groups[kv.Key], kv.Value)
			} else {
				groups[kv.Key] = []string{kv.Value}
			}
		}

		log.Printf("[%s] worker=%s task=%s type=%s status=run_reduce",
			time.Now().Format("15:04:05.000"), workerId, task.Id, task.Type)

		var output []byte
		for key, values := range groups {
			result := reducefn(key, values)
			formatted := fmt.Sprintf("%v %v\n", key, result)
			output = append(output, formatted...)
		}

		log.Printf("[%s] worker=%s task=%s type=%s status=writing_output",
			time.Now().Format("15:04:05.000"), workerId, task.Id, task.Type)

		writeFile(output, "mr-out-"+strconv.Itoa(task.Idx))
	}

	markAsDone(task.Id)
	run()
}

func getTask() GetTaskOut {
	args := GetTaskIn{}
	args.WorkerId = workerId
	reply := GetTaskOut{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok == false {
		fmt.Printf("Coordinator.GetTask failed!\n")
	}

	return reply
}

func handleMapResult(result []KeyValue, taskId string, nBuckets int) error {

	// init buckets for results
	buckets := make([][]KeyValue, nBuckets)
	for n := range nBuckets {
		buckets[n] = []KeyValue{}
	}

	// add result to it's corresponding bucket
	for _, kv := range result {
		idx := ihash(kv.Key) % nBuckets
		buckets[idx] = append(buckets[idx], kv)
	}

	err := writeBucketsToDisk(taskId, buckets)
	if err != nil {
		log.Fatalf("cannot write to disk")
	}

	return nil
}

func writeBucketsToDisk(taskId string, buckets [][]KeyValue) error {
	start := time.Now()

	var wg sync.WaitGroup
	errCh := make(chan error, len(buckets))

	for i, bucket := range buckets {
		wg.Add(1)
		go func(i int, bucket []KeyValue) {
			defer wg.Done()

			sort.Sort(ByKey(bucket))

			json, err := json.Marshal(bucket)
			if err != nil {
				errCh <- err
				return
			}

			filename := fmt.Sprintf("mr-%s-%d.json", taskId, i)
			writeFile(json, filename)
		}(i, bucket)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	log.Printf("[%s] worker=%s task=%s type=map elapsed=%d status=write_bucket",
		time.Now().Format("15:04:05.000"), workerId, taskId, time.Since(start).Milliseconds())

	return nil
}

func readFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return nil, err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return nil, err
	}
	file.Close()

	return content, nil
}

func writeFile(content []byte, filename string) error {
	f, err := os.CreateTemp("./", "tmp")
	if err != nil {
		log.Fatal(err)
		return err
	}

	if _, err := f.Write(content); err != nil {
		log.Fatal(err)
		return err
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
		return err
	}

	if err := os.Rename(f.Name(), filename); err != nil {
		os.Remove(f.Name())
		log.Fatal(err)
		return err
	}

	return nil
}

func markAsDone(taskId string) {
	args := UpdateTaskIn{}
	args.WorkerId = workerId
	args.TaskId = taskId
	args.Status = TaskStatusDone
	reply := UpdateTaskOut{}

	ok := call("Coordinator.UpdateTask", &args, &reply)
	if ok {
		log.Printf("[%s] worker=%s task=%s type=map status=done_ack",
			time.Now().Format("15:04:05.000"), workerId, taskId)
	} else {
		fmt.Printf("Coordinator.UpdateTask failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

// ==========================================
// UTILS

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// simple ID generation function
func generateId(n int) string {
	const letters = "0123456789abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
