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
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/params"
	"github.com/wentaojin/dbms/model/task"

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
func (d *Discovery) Assign(taskName, assignHost string) (string, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	jsonMachines, err := stringutil.MarshalJSON(d.machines)
	if err != nil {
		return "", fmt.Errorf("the discovery machine assign marshal json string failed: %v", err)
	}

	// for tasks in stopped, failed, or bound status, return the current worker directly.
	key, err := GetKey(d.etcdClient, stringutil.StringBuilder(constant.DefaultInstanceTaskReferencesPrefixKey, taskName))
	if err != nil {
		return "", err
	}
	resp := len(key.Kvs)
	if resp > 1 {
		return "", fmt.Errorf("the current task [%s] key [%s] has multiple values, the number exceeds 1, which does not meet expectations, Please contac author or retry", taskName, stringutil.StringBuilder(constant.DefaultInstanceTaskReferencesPrefixKey, taskName))
	} else if resp == 1 {
		// return worker addr
		workerAddr := stringutil.BytesToString(key.Kvs[0].Value)

		logger.Info("the worker assign task",
			zap.String("machine menus", jsonMachines),
			zap.String("assign worker", workerAddr))
		return workerAddr, nil
	}

	// give persistent state high priority
	keys, err := GetKey(d.etcdClient, constant.DefaultInstanceTaskReferencesPrefixKey, clientv3.WithPrefix())
	if err != nil {
		return "", err
	}

	hasExistedWorkerTaskM := make(map[string]string)

	for _, v := range keys.Kvs {
		keySli := stringutil.StringSplit(stringutil.BytesToString(v.Key), constant.StringSeparatorSlash)
		taskW := keySli[len(keySli)-1]
		hasExistedWorkerTaskM[stringutil.BytesToString(v.Value)] = taskW
	}

	// the csv migrate task with enable-import-feature need bound the dbms-worker instance, ignore leastBusyMachine scheduler rule
	ctx := context.Background()
	getTask, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: taskName})
	if err != nil {
		return "", err
	}

	if strings.EqualFold(getTask.TaskMode, constant.TaskModeCSVMigrate) {
		paramsInfo, err := model.GetIParamsRW().GetTaskCustomParam(ctx, &params.TaskCustomParam{
			TaskName:  taskName,
			TaskMode:  constant.TaskModeCSVMigrate,
			ParamName: constant.ParamNameCsvMigrateEnableImportFeature,
		})
		if err != nil {
			return "", err
		}

		enableImportFeature, err := strconv.ParseBool(paramsInfo.ParamValue)
		if err != nil {
			return "", err
		}

		if enableImportFeature {
			datasourceT, err := model.GetIDatasourceRW().GetDatasource(ctx, getTask.DatasourceNameT)
			if err != nil {
				return "", err
			}
			var csvFreeWorker []string
			csvBusyWorkerM := make(map[string]string)

			if insts, ok := d.machines[datasourceT.Host]; ok {
				for _, w := range insts {
					if strings.EqualFold(w.Role, constant.DefaultInstanceRoleWorker) {
						if t, ok := hasExistedWorkerTaskM[w.Addr]; ok {
							if t == taskName {
								logger.Info("the worker assign task",
									zap.String("machine menus", jsonMachines),
									zap.String("assign worker", w.Addr))
								return w.Addr, nil
							}
						}
						switch {
						case strings.EqualFold(w.State, constant.DefaultInstanceStoppedState) && strings.EqualFold(w.TaskName, taskName):
							return w.Addr, nil
						case strings.EqualFold(w.State, constant.DefaultInstanceFailedState) && strings.EqualFold(w.TaskName, taskName):
							return w.Addr, nil
						case strings.EqualFold(w.State, constant.DefaultInstanceBoundState) && strings.EqualFold(w.TaskName, taskName):
							return w.Addr, nil
						case strings.EqualFold(w.State, constant.DefaultInstanceFreeState) && strings.EqualFold(w.TaskName, ""):
							csvFreeWorker = append(csvFreeWorker, w.Addr)
						case strings.EqualFold(w.State, constant.DefaultInstanceBoundState) && strings.EqualFold(w.TaskName, ""):
							csvFreeWorker = append(csvFreeWorker, w.Addr)
						case strings.EqualFold(w.State, constant.DefaultInstanceFailedState) && strings.EqualFold(w.TaskName, ""):
							csvFreeWorker = append(csvFreeWorker, w.Addr)
						case strings.EqualFold(w.State, constant.DefaultInstanceStoppedState) && strings.EqualFold(w.TaskName, ""):
							csvFreeWorker = append(csvFreeWorker, w.Addr)
						default:
							csvBusyWorkerM[w.Addr] = w.TaskName
						}
					}
				}
				switch {
				case len(csvFreeWorker) == 0 && len(csvBusyWorkerM) != 0:
					jsonStr, err := stringutil.MarshalJSON(csvBusyWorkerM)
					if err != nil {
						return "", err
					}
					return "", fmt.Errorf("there are not avaliable instance in the datasource_name_t [%s] machine [%s] currently, the csv migrate task [enable-import-feature = true] require need bound datasource_name_t machine worker, Please scale-out worker instance in the machine [%s] or set the params  [enable-import-feature = false] or waiting free worker, display busy workers:\n %v",
						datasourceT.DatasourceName, datasourceT.Host, datasourceT.Host, jsonStr)
				case len(csvFreeWorker) == 0 && len(csvBusyWorkerM) == 0:
					return "", fmt.Errorf("there are not avaliable instance in the datasource_name_t [%s] machine [%s] currently, the csv migrate task [enable-import-feature = true] require need bound datasource_name_t machine worker, Please scale-out worker instance in the machine [%s] or set the params  [enable-import-feature = false]",
						datasourceT.DatasourceName, datasourceT.Host, datasourceT.Host)
				default:
					elem, err := stringutil.GetRandomElem(csvFreeWorker)
					if err != nil {
						return "", err
					}

					// double check
					if existTask, ok := hasExistedWorkerTaskM[elem]; ok {
						if existTask == taskName {
							logger.Info("the worker assign task",
								zap.String("machine menus", jsonMachines),
								zap.String("assign worker", elem))
							return elem, nil
						}
					}

					logger.Info("the worker assign task",
						zap.String("machine menus", jsonMachines),
						zap.String("assign worker", elem))
					return elem, nil
				}
			}

			return "", fmt.Errorf("the host address [%s] where the current datasource_name_t [%s] is located does not exist in the list of registered worker hosts. Please expand the worker instance on the host [%s] where datasource_name_t [%s] is located or set the params [enable-import-feature = false] and rerun the task", datasourceT.Host, datasourceT.DatasourceName, datasourceT.Host, datasourceT.DatasourceName)
		}
	}

	// prioritize the tasks of the specified host. When the tasks of the specified host do not exist, report error.
	if !strings.EqualFold(assignHost, "") {
		if insts, ok := d.machines[assignHost]; ok {
			var usableWorkers []string
			busyWorkers := 0
			for _, w := range insts {
				if strings.EqualFold(w.Role, constant.DefaultInstanceRoleWorker) {
					if t, ok := hasExistedWorkerTaskM[w.Addr]; ok {
						if t == taskName {
							logger.Info("the worker assign task",
								zap.String("machine menus", jsonMachines),
								zap.String("assign worker", w.Addr))
							return w.Addr, nil
						}
					}
					switch {
					case strings.EqualFold(w.State, constant.DefaultInstanceStoppedState) && strings.EqualFold(w.TaskName, taskName):
						return w.Addr, nil
					case strings.EqualFold(w.State, constant.DefaultInstanceFailedState) && strings.EqualFold(w.TaskName, taskName):
						return w.Addr, nil
					case strings.EqualFold(w.State, constant.DefaultInstanceBoundState) && strings.EqualFold(w.TaskName, taskName):
						return w.Addr, nil
					case strings.EqualFold(w.State, constant.DefaultInstanceFreeState) && strings.EqualFold(w.TaskName, ""):
						usableWorkers = append(usableWorkers, w.Addr)
					case strings.EqualFold(w.State, constant.DefaultInstanceBoundState) && strings.EqualFold(w.TaskName, ""):
						usableWorkers = append(usableWorkers, w.Addr)
					case strings.EqualFold(w.State, constant.DefaultInstanceFailedState) && strings.EqualFold(w.TaskName, ""):
						usableWorkers = append(usableWorkers, w.Addr)
					case strings.EqualFold(w.State, constant.DefaultInstanceStoppedState) && strings.EqualFold(w.TaskName, ""):
						usableWorkers = append(usableWorkers, w.Addr)
					default:
						busyWorkers++
					}
				}
			}

			if len(usableWorkers) == 0 {
				jsonMachines, err := stringutil.MarshalJSON(d.machines[assignHost])
				if err != nil {
					return "", fmt.Errorf("the discovery machine assign marshal json string failed: %v", err)
				}

				return "", fmt.Errorf("the work instances have assign tasks with in the assign machine [%s]. There are currently no available work instance machines. Please wait to register or expand the work instance, display the machine assign details: \n%s", assignHost, jsonMachines)
			}

			elem, err := stringutil.GetRandomElem(usableWorkers)
			if err != nil {
				return "", err
			}

			if t, ok := hasExistedWorkerTaskM[elem]; ok {
				if t == taskName {
					logger.Info("the worker assign task",
						zap.String("machine menus", jsonMachines),
						zap.String("assign worker", elem))
					return elem, nil
				}
			}

			logger.Info("the worker assign task",
				zap.String("machine menus", jsonMachines),
				zap.String("assign worker", elem))
			return elem, nil
		}
		return "", fmt.Errorf("the currently specified host [%s] does not exist. Please wait to register or expand the work instance.", assignHost)
	}

	// the machine worker statistics, exclude the master role
	// sort according to the busyness of the machine workers. If the busyness is the same, the original worker registration order will be randomized.
	var leastBusyMachine string
	minBusyWorkers := math.MaxInt32

	for machine, insts := range d.machines {
		busyWorkers := 0
		usableWorkers := 0
		for _, w := range insts {
			if strings.EqualFold(w.Role, constant.DefaultInstanceRoleWorker) {
				if t, ok := hasExistedWorkerTaskM[w.Addr]; ok {
					if t == taskName {
						logger.Info("the worker assign task",
							zap.String("machine menus", jsonMachines),
							zap.String("assign worker", w.Addr))
						return w.Addr, nil
					}
				}
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
		return "", fmt.Errorf("all work instances have assign tasks. There are currently no available work instance machines. Please wait to register or expand the work instance.")
	}

	var (
		busyWorkers []string
		freeWorkers []string
	)
	for _, w := range d.machines[leastBusyMachine] {
		if strings.EqualFold(w.Role, constant.DefaultInstanceRoleWorker) {
			if t, ok := hasExistedWorkerTaskM[w.Addr]; ok {
				if t == taskName {
					logger.Info("the worker assign task",
						zap.String("machine menus", jsonMachines),
						zap.String("assign worker", w.Addr))
					return w.Addr, nil
				}
			}
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
		return "", fmt.Errorf("there are not free instance in the least busy machine [%s] currently, total workers [%d] busy workers [%d] free workers [%d], Please waiting or scale-out worker instance", leastBusyMachine, len(d.machines[leastBusyMachine]), len(busyWorkers), len(freeWorkers))
	}

	elem, err := stringutil.GetRandomElem(freeWorkers)
	if err != nil {
		return "", err
	}

	if t, ok := hasExistedWorkerTaskM[elem]; ok {
		if t == taskName {
			logger.Info("the worker assign task",
				zap.String("machine menus", jsonMachines),
				zap.String("assign worker", elem))
			return elem, nil
		}
	}

	logger.Info("the worker assign task",
		zap.String("machine menus", jsonMachines),
		zap.String("assign worker", elem))
	return elem, nil
}

// Close used for close service
func (d *Discovery) Close() error {
	return d.etcdClient.Close()
}
