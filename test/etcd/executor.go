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
package etcd

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/service"
	"github.com/wentaojin/dbms/utils/stringutil"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/wentaojin/dbms/logger"
	"go.uber.org/zap"
)

type Executor struct {
	ctx              context.Context
	etcdClient       *clientv3.Client
	balanceSleepTime int64
	planChan         chan *Plan
}

func NewExecutor(ctx context.Context, etcdClient *clientv3.Client, balanceSleepTime int64) *Executor {
	return &Executor{ctx: ctx, etcdClient: etcdClient, balanceSleepTime: balanceSleepTime}
}

func (e *Executor) PushTaskPlan(p *Plan) {
	e.planChan <- p
}

func (e *Executor) Execute() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("worker task event execute panic", zap.Any("panic recover", r))
			}
		}()
		for {
			select {
			case <-e.ctx.Done():
				logger.Warn("worker task event execute cancel", zap.Any("task plans", <-e.planChan))
				return
			default:
				for p := range e.planChan {
					err := e.run(p)
					if err != nil {
						// task status
						p.Status = TaskDatabaseStatusFailed
					}
					_, err = model.GetITaskLogRW().CreateLog(e.ctx, &task.Log{
						TaskName:   p.Task.Name,
						WorkerAddr: p.Addr,
					})
					if err != nil {
						panic(err)
					}
				}
			}
		}
	}()
}

func (e *Executor) run(p *Plan) error {
	if strings.EqualFold(p.Status, TaskDatabaseStatusWaiting) {
		// There are micro-level differences in the clock verification of different machines. For the sake of task queue scheduling, when a slight delay in task scheduling is allowed, random sleep 0-1000ms // When the minimum execution time of a task is less than the sleep time, it will cause different nodes to Repeat
		time.Sleep(time.Duration(rand.Int63n(e.balanceSleepTime)) * time.Millisecond)

		// grab distributed lock
		lock := NewLocker(p.Task.Name, e.etcdClient)
		if err := lock.Lock(); err != nil {
			return err
		}
		p.Status = TaskDatabaseStatusWaiting
		logger.Info("worker task event execute start", zap.String("task", p.String()))

		// execute
		t, err := model.GetITaskRW().GetTask(p.CancelCtx, &task.Task{TaskName: p.Task.Name})
		if err != nil {
			return err
		}
		switch stringutil.StringUpper(t.TaskMode) {
		case TaskModeStructMigrate:
			err := service.StartStructMigrateTask(p.CancelCtx, p.Task.Name, p.Addr)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("current worker [%s] task [%s] subTaskName [%s] isn't Support, please contact author or reselect", p.Task.Name, t.TaskName, p.Addr)
		}

		// release lock
		err = lock.UnLock()
		if err != nil {
			return err
		}

		p.Status = TaskDatabaseStatusSuccess
		logger.Info("worker task event execute finished", zap.String("worker", p.Addr), zap.String("task", p.Task.Name), zap.String("task", p.String()))
		return nil
	}
	logger.Warn("worker task event execute skipped", zap.String("worker", p.Addr), zap.String("task", p.Task.Name), zap.Any("task plan", p.String()))
	return nil
}
