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
	nodes      map[string]string // health node list
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
		d.Set(string(ev.Key), string(ev.Value))
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
						d.Set(string(ev.Kv.Key), string(ev.Kv.Value))
					// delete
					case mvccpb.DELETE:
						d.Del(string(ev.Kv.Key))
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
func (d *Discovery) GetFreeWorker() (string, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	keyResp, err := GetKey(d.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey), clientv3.WithPrefix())
	if err != nil {
		return "", err
	}

	nodeAddr := make(map[string]string)
	for k, v := range d.nodes {
		keyS := stringutil.StringSplit(k, constant.StringSeparatorSlash)
		instAddr := keyS[len(keyS)-1]
		nodeAddr[instAddr] = v
	}

	var freeWorkers []string
	for k, v := range nodeAddr {
		switch {
		case len(keyResp.Kvs) == 0:
			return k, nil
		default:
			var n *Node
			err = stringutil.UnmarshalJSON([]byte(v), &n)
			if err != nil {
				return "", err
			}
			for _, ev := range keyResp.Kvs {
				addr := string(ev.Key)
				if strings.EqualFold(addr, k) {
					var w *Worker
					err = stringutil.UnmarshalJSON(ev.Value, &w)
					if err != nil {
						return "", err
					}
					if strings.EqualFold(w.State, constant.DefaultWorkerFreeState) {
						freeWorkers = append(freeWorkers, k)
					}
					if strings.EqualFold(w.State, constant.DefaultWorkerBoundState) && strings.EqualFold(w.TaskName, "") {
						freeWorkers = append(freeWorkers, k)
					}
					if strings.EqualFold(w.State, constant.DefaultWorkerStoppedState) && !strings.EqualFold(w.TaskName, "") {
						return k, nil
					}
					if strings.EqualFold(w.State, constant.DefaultWorkerFailedState) && !strings.EqualFold(w.TaskName, "") {
						return k, nil
					}
					if strings.EqualFold(w.State, constant.DefaultWorkerFailedState) && strings.EqualFold(w.TaskName, "") {
						freeWorkers = append(freeWorkers, k)
					}
				}
			}

			freeWorkers = append(freeWorkers, k)
		}
	}

	if len(freeWorkers) == 0 {
		return "", fmt.Errorf("there are not free node currently, please waiting or scale-out worker node")
	}

	elem, err := stringutil.GetRandomElem(freeWorkers)
	if err != nil {
		return "", err
	}
	return elem, nil
}

// Close used for close service
func (d *Discovery) Close() error {
	return d.etcdClient.Close()
}
