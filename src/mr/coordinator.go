package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	mu      sync.Mutex
	tasks   []Task
	nReduce int
}

var taskCounter = 1

func (c *Coordinator) GetTask(args *GetTaskIn, reply *GetTaskOut) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	idx := slices.IndexFunc(c.tasks, func(t Task) bool {
		return t.status == TaskStatusPending
	})

	if idx != -1 {
		reply.Id = c.tasks[idx].id
		reply.Filename = c.tasks[idx].filename
		reply.Type = c.tasks[idx].tType
		reply.NReduce = c.nReduce

		c.tasks[idx].status = TaskStatusOnGoing
		c.tasks[idx].workerId = args.WorkerId

		log.Printf("[%s] coordinator taskId=%s workerId=%s status=hand_task",
			time.Now().Format("15:04:05.000"), c.tasks[idx].id, c.tasks[idx].workerId)
	}

	return nil
}

func (c *Coordinator) UpdateTask(args *UpdateTaskIn, reply *GetTaskOut) error {
	idx := slices.IndexFunc(c.tasks, func(t Task) bool {
		return t.id == args.TaskId
	})

	c.tasks[idx].status = args.Status

	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
	}

	c.createTasks(files)
	c.server(sockname)

	return &c
}

// create list of tasks from filenames
func (c *Coordinator) createTasks(files []string) {
	c.tasks = []Task{}

	for _, filename := range files {
		task := Task{
			id:       strconv.Itoa(taskCounter),
			filename: filename,
			tType:    TaskTypeMap,
			status:   TaskStatusPending,
		}

		c.tasks = append(c.tasks, task)
		taskCounter += 1
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)

	log.Printf("[%s] coordinator socket=%s status=listening",
		time.Now().Format("15:04:05.000"), sockname)

	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}
