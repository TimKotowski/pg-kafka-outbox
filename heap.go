package outbox

import (
	"container/heap"
)

type CronJobHeap []*cronJobScheduler

func (c CronJobHeap) Len() int           { return len(c) }
func (c CronJobHeap) Less(i, j int) bool { return c[i].nextRunAt.Before(c[j].nextRunAt) }
func (c CronJobHeap) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

func (c *CronJobHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*c = append(*c, x.(*cronJobScheduler))
}

func (c *CronJobHeap) Pop() any {
	old := *c
	n := len(old)
	x := old[n-1]
	*c = old[0 : n-1]
	return x
}

var (
	_ JobSchedulerQueue[cronJobScheduler] = &jobSchedulerQueue{}
)

type JobSchedulerQueue[T any] interface {
	Push(x *T)
	Pop() *T
	Len() int
}

type jobSchedulerQueue struct {
	cronJobHeap *CronJobHeap
}

func NewJobSchedulerQueue() JobSchedulerQueue[cronJobScheduler] {
	cronJobHeap := &CronJobHeap{}
	heap.Init(cronJobHeap)
	return &jobSchedulerQueue{
		cronJobHeap: cronJobHeap,
	}
}

func (j *jobSchedulerQueue) Push(x *cronJobScheduler) {
	j.cronJobHeap.Push(x)
}

func (j *jobSchedulerQueue) Pop() *cronJobScheduler {
	return heap.Pop(j.cronJobHeap).(*cronJobScheduler)
}

func (j *jobSchedulerQueue) Len() int {
	return j.cronJobHeap.Len()
}
