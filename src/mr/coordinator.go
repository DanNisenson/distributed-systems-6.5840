
package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// Task ID generator

type TaskId struct {
	taskCounter int
}

func (t *TaskId) get() string {
	t.taskCounter++
	return strconv.Itoa(t.taskCounter)
}

var taskId = TaskId{taskCounter: 0}

// System coordinator

type Task struct {
	id       string
	input    string
	tType    TaskType
	status   TaskStatus
	workerId string
	idx      int
}

type JobPhase string

const (
	JobPhaseMap    JobPhase = "map"
	JobPhaseReduce JobPhase = "reduce"
	JobPhaseDone   JobPhase = "done"
)

type Coordinator struct {
	mu      sync.Mutex
	phase   JobPhase
	tasks   []Task
	nReduce int
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
	}

	c.createTasks(files, nReduce)
	c.server(sockname)
	c.phase = JobPhaseMap

	return &c
}

func (c *Coordinator) GetTask(args *GetTaskIn, reply *GetTaskOut) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == JobPhaseDone {
		reply.Id = taskId.get()
		reply.Type = TaskTypeExit
		return nil
	}

	idx := c.findPendingTask()

	if idx == -1 {
		reply.Id = taskId.get()
		reply.Type = TaskTypeWait

		return nil
	}

	// pointer to allow task update
	task := &c.tasks[idx]

	// set reply
	reply.Id = task.id
	reply.Input = task.input
	reply.Type = task.tType
	reply.Idx = task.idx
	reply.NReduce = c.nReduce

	// update task
	task.status = TaskStatusOnGoing
	task.workerId = args.WorkerId

	log.Printf("[%s] coordinator taskId=%s workerId=%s status=hand_task",
		time.Now().Format("15:04:05.000"), task.id, task.workerId)

	return nil
}

func (c *Coordinator) UpdateTask(args *UpdateTaskIn, reply *UpdateTaskOut) error {

	mapDone := true
	reduceDone := true

	for i := range c.tasks {
		// point to struct instead of copying
		task := &c.tasks[i]

		// update task status
		if task.id == args.TaskId {
			c.tasks[i].status = args.Status
		}

		// compute job phase
		if task.tType == TaskTypeMap && task.status != TaskStatusDone {
			mapDone = false
		} else if task.tType == TaskTypeReduce && task.status != TaskStatusDone {
			reduceDone = false
		}
	}

	// update job phase
	if mapDone == true && reduceDone == false {
		c.phase = JobPhaseReduce
	} else if mapDone == true && reduceDone == true {
		c.phase = JobPhaseDone
	}

	reply.Success = true

	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create list of tasks from filenames
func (c *Coordinator) createTasks(files []string, nReduce int) {
	c.tasks = []Task{}

	for _, filename := range files {
		task := Task{
			id:     taskId.get(),
			input:  filename,
			tType:  TaskTypeMap,
			status: TaskStatusPending,
		}
		c.tasks = append(c.tasks, task)
	}

	for i := range nReduce {
		task := Task{
			id:     taskId.get(),
			idx:    i,
			input:  fmt.Sprintf("mr-*-%d.json", i),
			tType:  TaskTypeReduce,
			status: TaskStatusPending,
		}
		c.tasks = append(c.tasks, task)
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

func (c *Coordinator) findPendingTask() int {

	tType := TaskTypeMap
	if c.phase == JobPhaseReduce {
		tType = TaskTypeReduce
	}

	for i, task := range c.tasks {
		if task.tType == tType && task.status == TaskStatusPending {
			return i
		}
	}
	return -1
}
