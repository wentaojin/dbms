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
package etcdutil

import (
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"

	"go.etcd.io/etcd/api/v3/mvccpb"

	"go.uber.org/zap"

	"github.com/wentaojin/dbms/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Discovery struct {
	etcdClient *clientv3.Client
	machines   map[string][]*Instance
	lock       sync.Mutex
}

func NewServiceDiscovery(etcdCli *clientv3.Client) *Discovery {
	return &Discovery{
		etcdClient: etcdCli,
		machines:   make(map[string][]*Instance),
	}
}

func (d *Discovery) Discovery(prefixKey string) error {
	// according to the configuring prefix key, and get key information from etcd
	keyResp, err := GetKey(d.etcdClient, prefixKey, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for _, ev := range keyResp.Kvs {
		d.Set(stringutil.BytesToString(ev.Key), stringutil.BytesToString(ev.Value))
	}

	go d.Watch(prefixKey)

	return nil
}

// Watch used for
// 1, watch service register, and initial instance list
// 2, watch prefix, modify the occurring change's instance
func (d *Discovery) Watch(prefixKey string) {
	logger.Info("discovery instance watching", zap.String("discovery key prefix", prefixKey))

	watchCh := WatchKey(d.etcdClient, prefixKey, clientv3.WithPrefix())

	for {
		select {
		case <-d.etcdClient.Ctx().Done():
			logger.Error("discovery instance watch cancel", zap.String("discovery key prefix", prefixKey))
			return
		default:
			for wresp := range watchCh {
				if wresp.Err() != nil {
					logger.Error("discovery instance watch failed", zap.String("discovery key prefix", prefixKey), zap.Error(wresp.Err()))
					// skip, only log
					//return fmt.Errorf("discovery instance watch failed, error: [%v]", wresp.Err())
				}
				for _, ev := range wresp.Events {
					switch ev.Type {
					// modify or add
					case mvccpb.PUT:
						d.Set(stringutil.BytesToString(ev.Kv.Key), stringutil.BytesToString(ev.Kv.Value))
					// delete
					case mvccpb.DELETE:
						d.Del(stringutil.BytesToString(ev.Kv.Key))
					}
				}
			}
		}
	}
}

// Set used for add service machine
func (d *Discovery) Set(key string, val string) {
	d.lock.Lock()
	defer d.lock.Unlock()

	// check if the machine is already registered
	keyS := stringutil.StringSplit(key, constant.StringSeparatorSlash)
	instAddr := keyS[len(keyS)-1]
	hostPort := stringutil.StringSplit(instAddr, constant.StringSeparatorDoubleColon)
	host := hostPort[0]

	var w *Instance
	err := stringutil.UnmarshalJSON([]byte(val), &w)
	if err != nil {
		panic(fmt.Sprintf("the instance key [%s] value [%v] set unmarshal json failed: %v", key, val, err))
	}

	if insts, ok := d.machines[host]; ok {
		// modify machine entry
		isFound := false
		for i, inst := range insts {
			if strings.EqualFold(w.Addr, inst.Addr) {
				insts[i] = w
				isFound = true
			}
		}
		if !isFound {
			d.machines[host] = append(d.machines[host], w)
		}
	} else {
		// create a new machine entry
		d.machines[host] = append(d.machines[host], w)
	}

	jsonMachines, err := stringutil.MarshalJSON(d.machines)
	if err != nil {
		panic(fmt.Sprintf("the discovery machine marshal json string failed: %v", err))
	}
	logger.Info("discovery instance set", zap.String("instance", instAddr), zap.String("key", key), zap.String("value", val), zap.String("machine", jsonMachines))
}

// Del used for del service instance
func (d *Discovery) Del(key string) {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Check if the machine is already registered
	keyS := stringutil.StringSplit(key, constant.StringSeparatorSlash)
	instAddr := keyS[len(keyS)-1]

	hostPort := stringutil.StringSplit(instAddr, constant.StringSeparatorDoubleColon)
	host := hostPort[0]

	var ws []*Instance
	if insts, ok := d.machines[host]; ok {
		for _, w := range insts {
			if !strings.EqualFold(w.Addr, instAddr) {
				ws = append(ws, w)
			}
		}
		d.machines[host] = ws
		logger.Info("discovery instance del", zap.String("instance", instAddr), zap.String("key", key))
	} else {
		logger.Warn("discovery instance not exist, skip", zap.String("instance", instAddr), zap.String("key", key))
	}
}

// Assign used for get free status service instance addr
func (d *Discovery) Assign(taskName string) (string, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	// the machine worker statistics, exclude the master role
	// sort according to the busyness of the machine workers. If the busyness is the same, the original worker registration order will be randomized.
	var leastBusyMachine string
	minBusyWorkers := math.MaxInt32

	for machine, insts := range d.machines {
		busyWorkers := 0
		usableWorkers := 0
		for _, w := range insts {
			if strings.EqualFold(w.Role, constant.DefaultInstanceRoleWorker) {
				switch {
				case strings.EqualFold(w.State, constant.DefaultInstanceStoppedState) && strings.EqualFold(w.TaskName, taskName):
					return w.Addr, nil
				case strings.EqualFold(w.State, constant.DefaultInstanceFailedState) && strings.EqualFold(w.TaskName, taskName):
					return w.Addr, nil
				case strings.EqualFold(w.State, constant.DefaultInstanceBoundState) && strings.EqualFold(w.TaskName, taskName):
					return w.Addr, nil
				case strings.EqualFold(w.State, constant.DefaultInstanceFreeState) && strings.EqualFold(w.TaskName, ""):
					usableWorkers++
				case strings.EqualFold(w.State, constant.DefaultInstanceBoundState) && strings.EqualFold(w.TaskName, ""):
					usableWorkers++
				case strings.EqualFold(w.State, constant.DefaultInstanceFailedState) && strings.EqualFold(w.TaskName, ""):
					usableWorkers++
				case strings.EqualFold(w.State, constant.DefaultInstanceStoppedState) && strings.EqualFold(w.TaskName, ""):
					usableWorkers++
				default:
					busyWorkers++
				}
			}
		}

		if busyWorkers < minBusyWorkers && usableWorkers > 0 {
			minBusyWorkers = busyWorkers
			leastBusyMachine = machine
		}
	}

	if strings.EqualFold(leastBusyMachine, "") {
		return "", fmt.Errorf("there are not machine with avaliable worker instance currently, please waiting register or scale-out worker instance")
	}

	var (
		busyWorkers []string
		freeWorkers []string
	)
	for _, w := range d.machines[leastBusyMachine] {
		if strings.EqualFold(w.Role, constant.DefaultInstanceRoleWorker) {
			switch {
			case strings.EqualFold(w.State, constant.DefaultInstanceStoppedState) && strings.EqualFold(w.TaskName, taskName):
				return w.Addr, nil
			case strings.EqualFold(w.State, constant.DefaultInstanceFailedState) && strings.EqualFold(w.TaskName, taskName):
				return w.Addr, nil
			case strings.EqualFold(w.State, constant.DefaultInstanceBoundState) && strings.EqualFold(w.TaskName, taskName):
				return w.Addr, nil
			case strings.EqualFold(w.State, constant.DefaultInstanceFreeState) && strings.EqualFold(w.TaskName, ""):
				freeWorkers = append(freeWorkers, w.Addr)
			case strings.EqualFold(w.State, constant.DefaultInstanceBoundState) && strings.EqualFold(w.TaskName, ""):
				freeWorkers = append(freeWorkers, w.Addr)
			case strings.EqualFold(w.State, constant.DefaultInstanceFailedState) && strings.EqualFold(w.TaskName, ""):
				freeWorkers = append(freeWorkers, w.Addr)
			case strings.EqualFold(w.State, constant.DefaultInstanceStoppedState) && strings.EqualFold(w.TaskName, ""):
				freeWorkers = append(freeWorkers, w.Addr)
			default:
				busyWorkers = append(busyWorkers, w.Addr)
			}
		}
	}

	if len(freeWorkers) == 0 {
		return "", fmt.Errorf("there are not free instance in the least busy machine [%s] currently, total workers [%d] busy workers [%d] free workers [%d], please waiting or scale-out worker instance", leastBusyMachine, len(d.machines[leastBusyMachine]), len(busyWorkers), len(freeWorkers))
	}

	elem, err := stringutil.GetRandomElem(freeWorkers)
	if err != nil {
		return "", err
	}

	jsonMachines, err := stringutil.MarshalJSON(d.machines)
	if err != nil {
		panic(fmt.Sprintf("the discovery machine assign marshal json string failed: %v", err))
	}
	logger.Info("the worker assign task", zap.String("machine", jsonMachines), zap.String("worker", elem))

	return elem, nil
}

// Close used for close service
func (d *Discovery) Close() error {
	return d.etcdClient.Close()
}
