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
	"errors"
	"reflect"

	"github.com/wentaojin/dbms/logger"
	"go.uber.org/zap"
)

// worker represents a worker in the pool
type worker struct {
	taskQueue chan Task
}

func newWorker() *worker {
	return &worker{taskQueue: make(chan Task, 1)}
}

// start the worker in a separate goroutine.
// The worker will run tasks from its taskQueue until the taskQueue is closed.
// For the length of the taskQueue is 1, the worker will be pushed back to the pool after executing 1 task.
func (w *worker) start(p *pool, thread int) {
	go func() {
		for t := range w.taskQueue {
			if !reflect.DeepEqual(t, Task{}) {
				w.handleResult(t, p, w.executeWithContextRetry(p, t))
			}
			p.pushWorker(thread)
		}
	}()
}

// execute the task and returns the result and error
// If the task fails, it will be retried according to the retryCount of the pool.
func (w *worker) executeWithContextRetry(p *pool, t Task) (err error) {
	err = w.executeWithContext(p, t)
	if err != nil {
		if errors.Is(err, errors.New("canceled")) {
			return p.canceledHandleFn(context.TODO(), t)
		} else {
			for i := 0; i < p.retryCount; i++ {
				err = w.executeWithContext(p, t)
				if err == nil || i == p.retryCount {
					return err
				}
			}
			return
		}
	}
	return
}

func (w *worker) executeWithContext(p *pool, t Task) error {
	if p.executeHandleFn == nil {
		return nil
	}

	// Create a context with cancel
	cancelCtx, cancelFn := context.WithCancel(p.ctx)

	// Create a channel to receive the result of the task
	errChan := make(chan error)

	// Run the task in a separate goroutine
	go func() {
		err := p.executeHandleFn(cancelCtx, t)
		select {
		case errChan <- err:
		case <-p.ctx.Done():
			cancelFn()
			logger.Error("the worker task would been canceled", zap.String("task", t.String()))
			return
		}
	}()

	// Wait for the task to finish or for the context to timeout
	select {
	case err := <-errChan:
		// The task finished successfully
		return err
	case <-p.ctx.Done():
		logger.Error("the worker task had been canceled", zap.String("task", t.String()))
		return errors.New("canceled")
	}
}

// handleResult handles the result of a task.
func (w *worker) handleResult(t Task, p *pool, err error) {
	if p.resultCallback != nil {
		if p.panicHandle {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("the worker running the task panic",
						zap.String("task", t.String()),
						zap.Int("running workers", p.RunningWorkerCount()),
						zap.Int("free workers", p.FreeWorkerCount()),
						zap.Any("error", r))
				}
			}()
		}
		p.resultCallback(Result{
			Task:  t,
			Error: err,
		})
	}
}
