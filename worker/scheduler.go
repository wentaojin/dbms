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
package worker

import (
	"context"
	"strings"
	"time"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/logger"
	"go.uber.org/zap"

	"github.com/gorhill/cronexpr"
)

type Plan struct {
	Addr       string               `json:"addr"`
	Task       *Task                `json:"task"`
	Expr       *cronexpr.Expression `json:"-"`
	Next       time.Time            `json:"next"`     // next schedule time
	Expected   time.Time            `json:"expected"` // expected schedule time
	Real       time.Time            `json:"real"`     // real schedule time
	Status     string               `json:"status"`
	CancelCtx  context.Context      `json:"-"` // task cancel
	CancelFunc context.CancelFunc   `json:"-"`
}

type Scheduler struct {
	Ctx           context.Context
	Executor      *Executor
	TaskEventChan chan *Event
	TaskPlans     map[string]*Plan
}

func NewScheduler(ctx context.Context, executor *Executor, taskEventChanSize int64) *Scheduler {
	return &Scheduler{
		Ctx:           ctx,
		Executor:      executor,
		TaskEventChan: make(chan *Event, taskEventChanSize),
		TaskPlans:     make(map[string]*Plan),
	}
}

func (s *Scheduler) ScheduleEvent() {
	go s.Scheduling()
}

func (s *Scheduler) Scheduling() {
	interval := s.TryScheduling()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case t := <-s.TaskEventChan:
			s.HandleTaskEvent(t)
		case <-ticker.C:
		case <-s.Ctx.Done():
			logger.Error("worker task event schedule cancel", zap.Any("task plans", s.TaskPlans))
			return
		}
		interval = s.TryScheduling()
		ticker.Reset(interval)
	}
}

// TryScheduling used for try schedule
// example: current time 0s, there are the three tasks，and they are executed once at 1s, 2s and 3s respectively
// now  a    b    c
// 0s - 1s - 2s - 3s
// current only wait 1s
func (s *Scheduler) TryScheduling() (interval time.Duration) {
	now := time.Now()
	var near *time.Time

	for name, p := range s.TaskPlans {
		// execute outdated task
		if p.Next.Before(now) || p.Next.Equal(now) {
			logger.Error("worker task schedule event success", zap.String("task", name), zap.String("status", "success"))
			p.Expected = p.Next
			p.Real = now
			// execute task
			s.Executor.PushTaskPlan(p)
			p.Next = p.Expr.Next(now)
		}
		// find the shortly outdated task
		if near == nil || p.Next.Before(*near) {
			near = &p.Next
		}
	}

	if near == nil {
		interval = 1 * time.Second
		return
	}
	interval = (*near).Sub(time.Now())
	return
}

func (s *Scheduler) HandleTaskEvent(eve *Event) {
	switch eve.Opt {
	case constant.DefaultTaskActionSubmitEvent:
		plan, err := s.CreateTaskPlan(eve)
		if err != nil {
			logger.Error("worker task schedule event create plan failed", zap.String("worker", eve.Addr), zap.String("task", eve.Task.Name), zap.String("status", "failed"))
			return
		}
		s.TaskPlans[eve.Task.Name] = plan
	case constant.DefaultTaskActionDeleteEvent:
		tp, ok := s.TaskPlans[eve.Task.Name]
		if ok {
			if strings.EqualFold(tp.Status, constant.TaskDatabaseStatusRunning) {
				logger.Warn("worker task schedule event plan running, but current prepare deleted", zap.String("worker", eve.Addr), zap.String("task", eve.Task.Name), zap.String("status", "deleted"))
				tp.CancelFunc()
				tp.Status = constant.TaskDatabaseStatusKilled
				tp.CancelCtx, tp.CancelFunc = context.WithCancel(context.Background())
			}
		}
		delete(s.TaskPlans, eve.Task.Name)
	case constant.DefaultTaskActionKillEvent:
		tp, ok := s.TaskPlans[eve.Task.Name]
		if ok {
			if strings.EqualFold(tp.Status, constant.TaskDatabaseStatusRunning) {
				logger.Warn("worker task schedule event plan killed", zap.String("worker", eve.Addr), zap.String("task", eve.Task.Name), zap.String("status", "killed"))
				tp.CancelFunc()
				tp.Status = constant.TaskDatabaseStatusKilled
				tp.CancelCtx, tp.CancelFunc = context.WithCancel(context.Background())
			}
		} else {
			logger.Warn("worker task schedule event plan canceled, but task isn't in taskPlan", zap.String("worker", eve.Addr), zap.String("task", eve.Task.Name))
		}
	}
}
func (s *Scheduler) PushTaskEvent(eve *Event) {
	s.TaskEventChan <- eve
}

func (s *Scheduler) CreateTaskPlan(eve *Event) (*Plan, error) {
	var (
		expr *cronexpr.Expression
		err  error
	)

	if expr, err = cronexpr.Parse(eve.Task.Express); err != nil {
		return nil, err
	}
	tp := &Plan{
		Addr: eve.Addr,
		Task: eve.Task,
		Expr: expr,
		Next: expr.Next(time.Now()),
	}
	tp.CancelCtx, tp.CancelFunc = context.WithCancel(context.TODO())

	return tp, nil
}

func (p *Plan) String() string {
	jsonStr, _ := stringutil.MarshalJSON(p)
	return jsonStr
}
