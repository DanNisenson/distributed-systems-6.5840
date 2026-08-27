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
	"sort"
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
)

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	workerId = generateId(10)
	mapfn = mapf

	run()
}

func run() {
	task := getTask()

	// exit worker if no task is available: assume job done
	if task.Filename == "" {
		log.Printf("[%s] worker=%s status=exit",
			time.Now().Format("15:04:05.000"), workerId)
		os.Exit(0)
	} else {
		log.Printf("[%s] worker=%s task=%s type=%s file=%s status=assigned",
			time.Now().Format("15:04:05.000"), workerId, task.Id, task.Type, task.Filename)
	}

	start := time.Now()
	content, err := readFile(task.Filename)
	if err != nil {
		// ... handle error
	} else {
		log.Printf("[%s] worker=%s task=%s type=%s file=%s elapsed=%d status=file_read",
			time.Now().Format("15:04:05.000"), workerId, task.Id, task.Type, task.Filename, time.Since(start).Milliseconds())
	}

	switch task.Type {
	case TaskTypeMap:
		result := mapfn(task.Filename, content)
		handleMapResult(result, task.Id, task.NReduce)
	case TaskTypeReduce:
		println("REDUCE")
	}
}

func getTask() GetTaskOut {
	args := GetTaskIn{}
	args.WorkerId = workerId
	reply := GetTaskOut{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok == false {
		fmt.Printf("call failed!\n")
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

	markAsDone(taskId)
	run()

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

	log.Printf("[%s] worker=%s task=%s type=map elapsed=%d status=write_to_disk",
		time.Now().Format("15:04:05.000"), workerId, taskId, time.Since(start).Milliseconds())

	return nil
}

func readFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return "", err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return "", err
	}
	file.Close()

	return string(content), nil
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
	reply := GetTaskOut{}

	ok := call("Coordinator.UpdateTask", &args, &reply)
	if ok {
		log.Printf("[%s] worker=%s task=%s type=map status=done_ack",
			time.Now().Format("15:04:05.000"), workerId, taskId)
	} else {
		fmt.Printf("call failed!\n")
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