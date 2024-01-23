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
	"time"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/constant"
	"go.uber.org/zap"

	"github.com/wentaojin/dbms/logger"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Node struct {
	Addr string
	// the Role column represent the node role
	Role string
}

func (n *Node) String() string {
	jsonByte, _ := stringutil.MarshalJSON(n)
	return jsonByte
}

// Worker register service information, used for etcd key -> value
type Worker struct {
	Addr string
	// the State column represent the node state, options: BOUND、FREE
	State string
	// the TaskName column represent the node bound task
	TaskName string
}

type Register struct {
	addr         string // register name, unique logo
	key          string // register key
	value        string // register value
	keepaliveTTL int64  // lease
	etcdClient   *clientv3.Client
	leaseID      clientv3.LeaseID
}

func NewServiceRegister(etcdCli *clientv3.Client, addr, key, value string, keepaliveTTL int64) *Register {
	return &Register{
		etcdClient:   etcdCli,
		addr:         addr,
		key:          key,
		value:        value,
		keepaliveTTL: keepaliveTTL,
	}
}

func (r *Register) Register(ctx context.Context) (err error) {
	// set lease time, send ping to keep alive
	grantResp, err := r.etcdClient.Grant(ctx, r.keepaliveTTL)
	if err != nil {
		return fmt.Errorf("grant lease [%d] failed: [%v]", r.keepaliveTTL, err)
	}
	r.leaseID = grantResp.ID

	_, err = PutKey(r.etcdClient, r.key, r.value, clientv3.WithLease(r.leaseID))
	if err != nil {
		return fmt.Errorf("put key [%s] value [%s] failed: [%v]", r.key, r.value, err)
	}

	logger.Info("register node operate",
		zap.String("addr", r.addr),
		zap.String("key", r.key),
		zap.String("value", r.value),
		zap.String("status", "new create success"))

	go r.keepAlive(ctx)

	return nil
}

func (r *Register) Revoke() error {
	// revoke lease
	_, err := r.etcdClient.Revoke(context.Background(), r.leaseID)
	if err != nil {
		return fmt.Errorf("revoke lease failed: [%v]", err)
	}

	logger.Warn("worker revoke lease",
		zap.String("addr", r.addr),
		zap.String("key", r.key),
		zap.String("value", r.value))

	return r.etcdClient.Close()
}

// keepAlive used for watch the key keepalive
func (r *Register) keepAlive(ctx context.Context) {
	for {
		cancelCtx, cancelFunc := context.WithCancel(ctx)

		// set lease time, send ping to keep alive
		keepAliveRespCh, err := r.etcdClient.KeepAlive(cancelCtx, r.leaseID)
		if err != nil {
			logger.Error("renew lease keepalive failed, it would be retrying", zap.Error(err))
			goto RETRY
		}
		// process renew lease
		for {
			select {
			case resp := <-keepAliveRespCh:
				// renew lease failed, maybe it's because the network is down or ping error
				if resp == nil {
					goto RETRY
				}
			}
		}
	RETRY:
		// waiting some time, and retry lease
		time.Sleep(constant.DefaultInstanceServiceRetryInterval)
		if cancelFunc != nil {
			cancelFunc()
		}
	}
}

func (w *Worker) String() string {
	jsonByte, _ := stringutil.MarshalJSON(w)
	return jsonByte
}
