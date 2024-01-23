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
	"fmt"
	"reflect"

	"go.uber.org/zap"

	"github.com/wentaojin/dbms/logger"
)

// worker represents a worker in the pool
type worker struct {
	ctx       context.Context
	taskQueue chan Task
}

func newWorker(ctx context.Context) *worker {
	return &worker{ctx: ctx, taskQueue: make(chan Task, 1)}
}

// start the worker in a separate goroutine.
// The worker will run tasks from its taskQueue until the taskQueue is closed.
// For the length of the taskQueue is 1, the worker will be pushed back to the pool after executing 1 task.
func (w *worker) start(p *pool, thread int) {
	go func() {
		for t := range w.taskQueue {
			if !reflect.DeepEqual(t, Task{}) {
				err := w.execute(p, t)
				w.handleResult(thread, t, p, err)
			}
			p.pushWorker(thread)
		}
	}()
}

// execute the task and returns the result and error
// If the task fails, it will be retried according to the retryCount of the pool.
func (w *worker) execute(p *pool, t Task) (err error) {
	for i := 0; i < p.retryCount+1; i++ {
		if p.executeTimeout > 0 {
			err = w.executeWithTimeout(p, t)
		} else {
			err = w.executeWithContext(p, t)
		}
		if err == nil || i == p.retryCount {
			return err
		}
	}
	return
}

// executeWithTimeout executes a task with a timeout and returns the result and error.
func (w *worker) executeWithTimeout(p *pool, t Task) error {
	// Task skip
	if p.executeTaskFn == nil {
		return nil
	}
	// Create a context with timeout
	timeoutCtx, timeoutCancel := context.WithTimeout(w.ctx, p.executeTimeout)
	defer timeoutCancel()

	// Create a channel to receive the result of the task
	errChan := make(chan error)

	// Run the task in a separate goroutine
	go func() {
		err := p.executeTaskFn(timeoutCtx, t)
		select {
		case errChan <- err:
		case <-timeoutCtx.Done():
			// The context was cancelled, stop the task
			return
		}
	}()

	// Wait for the task to finish or for the context to timeout
	select {
	case err := <-errChan:
		// The task finished successfully
		return err
	case <-timeoutCtx.Done():
		// The context timed out, the task took too long
		return fmt.Errorf("task [%s] execute timed out [%v]", t.String(), p.executeTimeout)
	}
}

func (w *worker) executeWithContext(p *pool, t Task) error {
	// Task skip
	if p.executeTaskFn == nil {
		return nil
	}

	// Create a context with timeout
	cancelCtx, cancelFn := context.WithCancel(w.ctx)
	defer cancelFn()

	// Create a channel to receive the result of the task
	errChan := make(chan error)

	// Run the task in a separate goroutine
	go func() {
		err := p.executeTaskFn(cancelCtx, t)
		select {
		case errChan <- err:
		case <-cancelCtx.Done():
			// The context was cancelled, stop the task
			return
		}
	}()

	// Wait for the task to finish or for the context to timeout
	select {
	case err := <-errChan:
		// The task finished successfully
		return err
	case <-cancelCtx.Done():
		// The context timed out, the task took too long
		return fmt.Errorf("task [%s] execute had been canceled", t.String())
	}
}

// handleResult handles the result of a task.
func (w *worker) handleResult(thread int, t Task, p *pool, err error) {
	if p.resultCallback != nil {
		if p.panicHandle {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("the worker running the task panic", zap.String("task", t.String()), zap.Any("error", r))
				}
			}()
		}
		p.resultCallback(Result{
			Thread: thread,
			Task:   t,
			Error:  err,
		})
	}
}
