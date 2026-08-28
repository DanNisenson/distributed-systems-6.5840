package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
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

type worker struct {
	coordSockName string // socket for coordinator
	workerId      string
	mapfn         func(string, string) []KeyValue
	reducefn      func(string, []string) string
	logger        *Logger
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := worker{
		coordSockName: sockname,
		workerId:      generateId(10),
		mapfn:         mapf,
		reducefn:      reducef,
	}

	w.logger = NewLogger()
	w.logger.AddDefault("worker", w.workerId)

	go func() {
		for {
			w.pingCoordinator()
			time.Sleep(4 * time.Second)
		}
	}()

	w.run()
}

func (w *worker) pingCoordinator() {
	args := PingIn{WorkerId: w.workerId}
	reply := PingOut{}

	w.call("Coordinator.Ping", &args, &reply)
}

func (w *worker) run() {

	task := w.getTask()

	w.logger.AddDefault("task", task.Id)
	w.logger.AddDefault("type", task.Type)

	w.logger.Info("status", "got_task")

	switch task.Type {
	case TaskTypeExit:
		w.logger.Debug("status", "exit")
		os.Exit(0)
	case TaskTypeWait:
		w.logger.Debug("status", "wait")
		time.Sleep(3 * time.Second)
		w.run()
	case TaskTypeMap:
		w.execMap(task)
	case TaskTypeReduce:
		w.execReduce(task)
	}

	w.markAsDone(task.Id)
	w.run()
}

func (w *worker) getTask() GetTaskOut {
	args := GetTaskIn{}
	args.WorkerId = w.workerId
	reply := GetTaskOut{}

	ok := w.call("Coordinator.GetTask", &args, &reply)
	if ok == false {
		w.logger.Error("status", "FATAL", "reason", "Coordinator.GetTask failed!")
	}

	return reply
}

func (w *worker) execMap(task GetTaskOut) {
	content, err := w.readFile(task.Input)
	if err != nil {
		w.logger.Error("!!status", "FATAL", "reason", err)
		return
	}

	w.logger.Debug("status", "file_read")

	intermediate := w.mapfn(task.Input, string(content))

	buckets := w.splitIntoBuckets(intermediate, task.NReduce)

	err = w.writeBucketsToDisk(task.Id, buckets)
	if err != nil {
		w.logger.Error("status", "FATAL", "reason", err)
	}
}

func (w *worker) splitIntoBuckets(kvs []KeyValue, nBuckets int) [][]KeyValue {
	buckets := make([][]KeyValue, nBuckets)

	for n := range nBuckets {
		buckets[n] = []KeyValue{}
	}

	for _, kv := range kvs {
		idx := ihash(kv.Key) % nBuckets
		buckets[idx] = append(buckets[idx], kv)
	}

	return buckets
}

func (w *worker) writeBucketsToDisk(taskId string, buckets [][]KeyValue) error {
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
			w.writeFile(json, filename)
		}(i, bucket)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	w.logger.Debug("status", "write_bucket", "elapsed", time.Since(start).Milliseconds())

	return nil
}

func (w *worker) execReduce(task GetTaskOut) {
	filenames, err := filepath.Glob("./" + task.Input)
	if err != nil {
		w.logger.Error("status", "FATAL", "reason", err)
		return
	}
	w.logger.Debug("status", "reading_buckets")

	kvs, err := w.readReduceInputFiles(filenames)
	if err != nil {
		w.logger.Error("status", "FATAL", "reason", err)
		return
	}
	w.logger.Debug("status", "grouping")

	byKey := w.groupValuesByKey(kvs)
	w.logger.Debug("status", "run_reduce")

	output := w.reduceValuesByKey(byKey)
	w.logger.Debug("status", "writing_output")

	w.writeFile(output, "mr-out-"+strconv.Itoa(task.Idx))

	w.deleteIntermediateFiles(filenames)
}

func (w *worker) readReduceInputFiles(filenames []string) ([]KeyValue, error) {
	var kvs []KeyValue
	for _, filename := range filenames {
		bytes, err := w.readFile(filename)
		if err != nil {
			return nil, err
		}
		var content []KeyValue
		json.Unmarshal(bytes, &content)
		kvs = append(kvs, content...)
	}

	return kvs, nil
}

func (w *worker) groupValuesByKey(kvs []KeyValue) map[string][]string {
	groups := make(map[string][]string)
	for _, kv := range kvs {
		if groups[kv.Key] != nil {
			groups[kv.Key] = append(groups[kv.Key], kv.Value)
		} else {
			groups[kv.Key] = []string{kv.Value}
		}
	}
	return groups
}

func (w *worker) reduceValuesByKey(byKey map[string][]string) []byte {
	var output []byte
	for key, values := range byKey {
		result := w.reducefn(key, values)
		formatted := fmt.Sprintf("%v %v\n", key, result)
		output = append(output, formatted...)
	}
	return output
}

func (w *worker) deleteIntermediateFiles(filenames []string) {
	for _, filename := range filenames {
		err := os.Remove(filename)
		if err != nil {
			w.logger.Error("status", "ERROR", "reason", err)
		}
	}
}

func (w *worker) readFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		w.logger.Error("status", "FATAL", "reason", err)
		return nil, err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		w.logger.Error("status", "FATAL", "reason", err)
		return nil, err
	}
	file.Close()

	return content, nil
}

func (w *worker) writeFile(content []byte, filename string) error {
	f, err := os.CreateTemp("./", "tmp")
	if err != nil {
		w.logger.Error("status", "FATAL", "reason", err)
		return err
	}

	if _, err := f.Write(content); err != nil {
		w.logger.Error("status", "FATAL", "reason", err)
		return err
	}
	if err := f.Close(); err != nil {
		w.logger.Error("status", "FATAL", "reason", err)
		return err
	}

	if err := os.Rename(f.Name(), filename); err != nil {
		os.Remove(f.Name())
		w.logger.Error("status", "FATAL", "reason", err)
		return err
	}

	return nil
}

func (w *worker) markAsDone(taskId string) {
	args := UpdateTaskIn{}
	args.WorkerId = w.workerId
	args.TaskId = taskId
	args.Status = TaskStatusDone
	reply := UpdateTaskOut{}

	ok := w.call("Coordinator.UpdateTask", &args, &reply)
	if ok {
		w.logger.Info("status", "done_ack")
	} else {
		w.logger.Error("status", "FATAL", "reason", "Coordinator.UpdateTask failed!")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func (w *worker) call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", w.coordSockName)
	if err != nil {
		// we assume the job is done
		w.logger.Info("status", "EXIT", "reason", "can't connect to coordinator server")
		os.Exit(0)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	w.logger.Error("status", "CALL FAILED", "args", fmt.Sprintf("%+v", args))
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
