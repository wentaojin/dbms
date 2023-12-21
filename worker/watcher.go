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

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/etcdutil"

	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/constant"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type Task struct {
	Name    string // task unique logo, when create task, need configure
	Express string // scheduled, if express value is null, the task would immediately execute
}

type Event struct {
	Opt  int
	Addr string
	Task *Task
}

type Watcher struct {
	ctx        context.Context
	addr       string
	scheduler  *Scheduler
	etcdClient *clientv3.Client
}

func NewWatcher(ctx context.Context, addr string, scheduler *Scheduler, etcdClient *clientv3.Client) *Watcher {
	return &Watcher{ctx: ctx, addr: addr, scheduler: scheduler, etcdClient: etcdClient}
}

func (w *Watcher) Watch() {
	go w.watchSubmit()
	go w.watchKill()
}

func (w *Watcher) watchSubmit() {
	logger.Info("worker watch scheduled task event starting",
		zap.String("key with prefix", constant.DefaultScheduledTaskSubmitPrefixKey))

	// get current all task
	resps, err := etcdutil.GetKey(w.etcdClient, constant.DefaultScheduledTaskSubmitPrefixKey, clientv3.WithPrefix())
	if err != nil {
		logger.Warn("worker get scheduled task event revision failed", zap.String("key with prefix", constant.DefaultScheduledTaskSubmitPrefixKey), zap.Error(err))
		return
	}
	for _, kv := range resps.Kvs {
		task := &Task{}
		if err = stringutil.UnmarshalJSON(kv.Value, task); err != nil {
			logger.Warn("worker watch scheduled task event unmarshal JSON", zap.String("key", string(kv.Key)), zap.String("value", string(kv.Value)), zap.Error(err))
			continue
		}
		w.scheduler.PushTaskEvent(&Event{
			Opt:  constant.DefaultTaskActionSubmitEvent,
			Addr: w.addr,
			Task: task,
		})
	}

	watchCh := etcdutil.WatchKey(w.etcdClient, constant.DefaultScheduledTaskSubmitPrefixKey, clientv3.WithRev(resps.Header.Revision+1), clientv3.WithPrefix())
	for {
		select {
		case <-w.ctx.Done():
			logger.Error("worker watch scheduled task event cancel", zap.String("key prefix", constant.DefaultScheduledTaskSubmitPrefixKey))
			return
		default:
			for wresp := range watchCh {
				for _, ev := range wresp.Events {
					switch ev.Type {
					// modify or add
					case mvccpb.PUT:
						// move to scheduler
						w.scheduler.PushTaskEvent(w.pushTaskEvent(constant.DefaultTaskActionSubmitEvent, ev))
					// delete
					case mvccpb.DELETE:
						w.scheduler.PushTaskEvent(w.pushTaskEvent(constant.DefaultTaskActionDeleteEvent, ev))
					}
				}
			}
		}
	}
}

func (w *Watcher) watchKill() {
	// the all keys are existed lease within the kill dir,so current worker start, kill dir is null and don't need configuring version
	watchCh := etcdutil.WatchKey(w.etcdClient, constant.DefaultScheduledTaskKillPrefixKey, clientv3.WithPrefix())
	for {
		select {
		case <-w.ctx.Done():
			logger.Error("worker watch scheduled task kill event cancel", zap.String("key prefix", constant.DefaultScheduledTaskKillPrefixKey))
			return
		default:
			for wresp := range watchCh {
				for _, ev := range wresp.Events {
					switch ev.Type {
					// modify or add
					case mvccpb.PUT:
						// move to scheduler
						w.scheduler.PushTaskEvent(w.pushTaskEvent(constant.DefaultTaskActionKillEvent, ev))
					// delete
					case mvccpb.DELETE:
						// the all keys are existed lease within the kill dir,so don't need processing delete event
					}
				}
			}
		}
	}
}

func (w *Watcher) pushTaskEvent(opt int, eve *clientv3.Event) *Event {
	var event *Event

	switch opt {
	case constant.DefaultTaskActionSubmitEvent:
		task := &Task{}
		err := stringutil.UnmarshalJSON(eve.Kv.Value, task)
		if err != nil {
			return nil
		}
		event = &Event{
			Opt:  opt,
			Addr: w.addr,
			Task: task,
		}
	case constant.DefaultTaskActionDeleteEvent:
		event = &Event{
			Opt:  opt,
			Addr: w.addr,
			Task: &Task{
				Name: strings.TrimPrefix(string(eve.Kv.Key), constant.DefaultScheduledTaskSubmitPrefixKey),
			},
		}
	case constant.DefaultTaskActionKillEvent:
		event = &Event{
			Opt:  opt,
			Addr: w.addr,
			Task: &Task{
				Name: strings.TrimPrefix(string(eve.Kv.Key), constant.DefaultScheduledTaskKillPrefixKey),
			},
		}
	}
	return event
}

func (t *Task) String() string {
	jsonStr, _ := stringutil.MarshalJSON(t)
	return jsonStr
}
