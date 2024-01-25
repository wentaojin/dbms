/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package pool

import (
	"context"
	"sync"
	"time"

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type IPool interface {
	// SubmitTask adds a task to the pool
	SubmitTask(t Task)
	// Wait waits for all tasks to be dispatched and completed.
	Wait()
	// Release releases the pool and all its workers.
	Release()
	// RunningWorkerCount returns the number of workers that are currently working
	RunningWorkerCount() int
	// FreeWorkerCount returns the number of workers that are currently free
	FreeWorkerCount() int
}

type Task struct {
	Name  string      `json:"name"`
	Group string      `json:"group"`
	Job   interface{} `json:"job"`
}

type Result struct {
	Task  Task
	Error error
}

type pool struct {
	maxWorkers int
	// workerStack represent worker stack, used to judge current task all worker whether done
	workerStack []int
	// workers represents worker do nums
	workers []*worker
	// tasks are added to this channel first, then dispatched to workers. Default buffer size is 1 million.
	taskQueue chan Task
	// Set by WithTaskQueueSize(), used to set the size of the task queue. Default is 1024.
	taskQueueSize int
	// Set by WithResultCallback(), used to handle the result of a task. Default is nil.
	resultCallback func(r Result)
	// Set by WithRetryCount(), used to retry the error of a task, Default is 1
	retryCount int
	// Set by WithPanicHandle(), used to set a method for a task, Default is false, which means meet panic report error
	panicHandle bool
	// Set by WithExecuteTask(), used to represent the worker need process task.
	executeHandleFn func(ctx context.Context, t Task) error
	// Set by canceledHandleFn(), used ti represent the worker need process canceled task
	canceledHandleFn func(ctx context.Context, t Task) error

	ctx  context.Context
	lock sync.Locker
	// Conditional signals are a type of blocking wait between multiple goroutines. sync.Cond can be used to wait and notify goroutine mechanisms so that they can wait or continue execution under specific conditions
	cond *sync.Cond
}

// NewPool creates a new pool of workers.
func NewPool(ctx context.Context, maxWorkers int, opts ...Option) IPool {
	p := &pool{
		maxWorkers:     maxWorkers,
		retryCount:     0,
		taskQueue:      nil,
		taskQueueSize:  constant.DefaultTaskQueueChannelSize,
		resultCallback: nil,
		lock:           new(sync.Mutex),
		panicHandle:    false,

		ctx:              ctx,
		executeHandleFn:  nil,
		canceledHandleFn: nil,
	}
	// options
	for _, opt := range opts {
		opt(p)
	}

	p.taskQueue = make(chan Task, p.taskQueueSize)
	p.workers = make([]*worker, p.maxWorkers)
	p.workerStack = make([]int, p.maxWorkers)

	if p.cond == nil {
		p.cond = sync.NewCond(p.lock)
	}

	// Create workers with the minimum number
	for i := 0; i < p.maxWorkers; i++ {
		w := newWorker()
		p.workers[i] = w
		p.workerStack[i] = i
		w.start(p, i)
	}

	// task dispatch worker
	go p.dispatch()
	return p
}

func (p *pool) SubmitTask(t Task) {
	p.taskQueue <- t
}

func (p *pool) Wait() {
	for {
		p.lock.Lock()
		workerStackLen := len(p.workerStack)
		p.lock.Unlock()
		if len(p.taskQueue) == 0 && workerStackLen == len(p.workers) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (p *pool) RunningWorkerCount() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.workers) - len(p.workerStack)
}

func (p *pool) FreeWorkerCount() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.workerStack)
}

func (p *pool) Release() {
	close(p.taskQueue)
	p.cond.L.Lock()
	for len(p.workerStack) != p.maxWorkers {
		p.cond.Wait()
	}
	p.cond.L.Unlock()
	for _, w := range p.workers {
		close(w.taskQueue)
	}
	p.workers = nil
	p.workerStack = nil
}

// dispatch tasks to workers.
func (p *pool) dispatch() {
	for t := range p.taskQueue {
		p.cond.L.Lock()
		for len(p.workerStack) == 0 {
			p.cond.Wait()
		}
		p.cond.L.Unlock()
		workerIndex := p.popWorker()
		p.workers[workerIndex].taskQueue <- t
	}
}

func (p *pool) popWorker() int {
	p.lock.Lock()
	workerIndex := p.workerStack[len(p.workerStack)-1]
	p.workerStack = p.workerStack[:len(p.workerStack)-1]
	p.lock.Unlock()
	return workerIndex
}

func (p *pool) pushWorker(workerIndex int) {
	p.lock.Lock()
	p.workerStack = append(p.workerStack, workerIndex)
	p.lock.Unlock()
	p.cond.Signal()
}

func (t Task) String() string {
	jsonStr, _ := stringutil.MarshalJSON(t)
	return jsonStr
}
