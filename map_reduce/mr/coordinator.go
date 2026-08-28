package mr

import (
	"fmt"
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
	lastPing int64
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
	logger  *Logger
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
	}

	c.logger = NewLogger()
	c.logger.AddDefault("coordinator", true)

	c.createTasks(files, nReduce)
	c.server(sockname)

	c.mu.Lock()
	c.phase = JobPhaseMap
	c.mu.Unlock()

	return &c
}

func (c *Coordinator) Ping(args *PingIn, reply *PingOut) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.tasks {
		if c.tasks[i].workerId == args.WorkerId {
			c.tasks[i].lastPing = time.Now().UnixMilli()
		}
	}

	return nil
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
	task.lastPing = time.Now().UnixMilli()

	go func() {
		for {
			time.Sleep(10 * time.Second)
			c.checkTask(task)
		}
	}()

	c.logger.Info("status", "hand_task", "taskId", task.id, "workerId", task.workerId)

	return nil
}

func (c *Coordinator) checkTask(task *Task) {
	c.mu.Lock()
	if task.status == TaskStatusOnGoing {
		c.logger.Info("DEBUG", "CHECK TASK", "task", task.id, "DEAD", time.Now().UnixMilli()-task.lastPing > 10_000)
		c.logger.Info("TASKS", c.tasks)

		if time.Now().UnixMilli()-task.lastPing > 10_000 {
			task.status = TaskStatusPending
			task.workerId = ""
			c.logger.Info("DEB", "RESET TASK", "task", task.id)
		}
	}
	c.mu.Unlock()
}

func (c *Coordinator) UpdateTask(args *UpdateTaskIn, reply *UpdateTaskOut) error {
	c.mu.Lock()
	defer c.mu.Unlock()

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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == JobPhaseDone
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

	c.logger.Info("status", "listening", "socket", sockname)

	if e != nil {
		c.logger.Error("status", "FATAL", "reason", e)
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
