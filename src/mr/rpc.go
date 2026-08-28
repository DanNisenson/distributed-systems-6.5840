
package mr


type TaskType string

const (
	TaskTypeMap    TaskType = "map"
	TaskTypeReduce TaskType = "reduce"
	TaskTypeWait   TaskType = "wait"
	TaskTypeExit   TaskType = "exit"
)

type TaskStatus string

const (
	TaskStatusPending TaskStatus = "pending"
	TaskStatusOnGoing TaskStatus = "ongoing"
	TaskStatusDone    TaskStatus = "done"
)

type GetTaskIn struct {
	WorkerId string
}

type GetTaskOut struct {
	Id      string
	Input   string
	Type    TaskType
	NReduce int
	Idx     int
}

type UpdateTaskIn struct {
	WorkerId string
	TaskId   string
	Status   TaskStatus
}

type UpdateTaskOut struct {
	Success bool
}

