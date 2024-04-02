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
	nodes      map[string]string
	lock       sync.Mutex
}

func NewServiceDiscovery(etcdCli *clientv3.Client) *Discovery {
	return &Discovery{
		etcdClient: etcdCli,
		nodes:      make(map[string]string),
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
// 1, watch service register, and initial node list
// 2, watch prefix, modify the occurring change's node
func (d *Discovery) Watch(prefixKey string) {
	logger.Info("discovery node watching", zap.String("discovery key prefix", prefixKey))

	watchCh := WatchKey(d.etcdClient, prefixKey, clientv3.WithPrefix())

	for {
		select {
		case <-d.etcdClient.Ctx().Done():
			logger.Error("discovery node watch cancel", zap.String("discovery key prefix", prefixKey))
			return
		default:
			for wresp := range watchCh {
				if wresp.Err() != nil {
					logger.Error("discovery node watch failed", zap.String("discovery key prefix", prefixKey), zap.Error(wresp.Err()))
					// skip, only log
					//return fmt.Errorf("discovery node watch failed, error: [%v]", wresp.Err())
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

// Set used for add service node
func (d *Discovery) Set(key string, val string) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.nodes[key] = val

	logger.Info("discovery node set", zap.String("key", key), zap.String("node", val))
}

// Del used for del service node
func (d *Discovery) Del(key string) {
	d.lock.Lock()
	defer d.lock.Unlock()

	delWorker := d.nodes[key]
	delete(d.nodes, key)

	logger.Info("discovery node del", zap.String("key", key), zap.String("node", delWorker))
}

// GetAllNodeAddr used for get all service node addr
func (d *Discovery) GetAllNodeAddr() map[string]string {
	d.lock.Lock()
	defer d.lock.Unlock()

	nodeAddr := make(map[string]string)
	for k, v := range d.nodes {
		keyS := stringutil.StringSplit(k, constant.StringSeparatorSlash)
		instAddr := keyS[len(keyS)-1]
		nodeAddr[instAddr] = v
	}

	return d.nodes
}

// GetFreeWorker used for get free service node addr
func (d *Discovery) GetFreeWorker(taskName string) (string, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	var nodes []string
	registerKeyResp, err := GetKey(d.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerRegisterPrefixKey), clientv3.WithPrefix())
	if err != nil {
		return "", err
	}
	if len(registerKeyResp.Kvs) == 0 {
		return "", fmt.Errorf("there are not active node currently, please waiting register or scale-out worker node")
	} else {
		for _, ev := range registerKeyResp.Kvs {
			var n *Node
			err = stringutil.UnmarshalJSON(ev.Value, &n)
			if err != nil {
				return "", err
			}
			nodes = append(nodes, n.Addr)
		}
	}

	stateKeyResp, err := GetKey(d.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey), clientv3.WithPrefix())
	if err != nil {
		return "", err
	}

	var (
		freeWorkers   []string
		activeWorkers []string
	)

	if len(stateKeyResp.Kvs) == 0 {
		return nodes[0], nil
	} else {
		for _, ev := range stateKeyResp.Kvs {
			var w *Worker
			err = stringutil.UnmarshalJSON(ev.Value, &w)
			if err != nil {
				return "", err
			}

			switch {
			case strings.EqualFold(w.State, constant.DefaultWorkerStoppedState) && strings.EqualFold(w.TaskName, taskName):
				if stringutil.IsContainedString(nodes, w.Addr) {
					return w.Addr, nil
				}

			case strings.EqualFold(w.State, constant.DefaultWorkerFailedState) && strings.EqualFold(w.TaskName, taskName):
				if stringutil.IsContainedString(nodes, w.Addr) {
					return w.Addr, nil
				}
			case strings.EqualFold(w.State, constant.DefaultWorkerBoundState) && strings.EqualFold(w.TaskName, taskName):
				if stringutil.IsContainedString(nodes, w.Addr) {
					return w.Addr, nil
				}
			case strings.EqualFold(w.State, constant.DefaultWorkerFreeState) && strings.EqualFold(w.TaskName, ""):
				if stringutil.IsContainedString(nodes, w.Addr) {
					freeWorkers = append(freeWorkers, w.Addr)
				}
			case strings.EqualFold(w.State, constant.DefaultWorkerBoundState) && strings.EqualFold(w.TaskName, ""):
				if stringutil.IsContainedString(nodes, w.Addr) {
					freeWorkers = append(freeWorkers, w.Addr)
				}
			case strings.EqualFold(w.State, constant.DefaultWorkerFailedState) && strings.EqualFold(w.TaskName, ""):
				if stringutil.IsContainedString(nodes, w.Addr) {
					freeWorkers = append(freeWorkers, w.Addr)
				}
			default:
				activeWorkers = append(activeWorkers, w.Addr)
			}
		}
	}
	if len(freeWorkers) == 0 {
		elem, err := stringutil.GetRandomElem(stringutil.StringItemsFilterDifference(nodes, activeWorkers))
		if err != nil {
			return "", err
		}
		return elem, nil
	}
	if len(freeWorkers) > 0 {
		elem, err := stringutil.GetRandomElem(freeWorkers)
		if err != nil {
			return "", err
		}
		return elem, nil
	}
	return "", fmt.Errorf("there are not free node currently, please waiting or scale-out worker node")
}

// Close used for close service
func (d *Discovery) Close() error {
	return d.etcdClient.Close()
}
