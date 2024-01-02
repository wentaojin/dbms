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
package configutil

const (
	DefaultWorkerNamePrefix   = "worker"
	DefaultWorkerRegisterAddr = "127.0.0.1:2381"
	// DefaultWorkerKeepaliveTTL represented service lease keep alive ttl (seconds)
	DefaultWorkerKeepaliveTTL    = 5
	DefaultEventQueueChannelSize = 1024
	// DefaultBalanceSleepTime represented service lease keep alive ttl (millions)
	DefaultBalanceSleepTime = 100
)

// WorkerOptions etcd server relative config items
type WorkerOptions struct {
	Name             string `toml:"name" json:"name"`
	Endpoint         string `toml:"endpoint" json:"endpoint"` // server addrs
	WorkerAddr       string `toml:"worker-addr" json:"worker-addr"`
	KeepaliveTTL     int64  `toml:"keepalive-ttl" json:"keepalive-ttl"`
	EventQueueSize   int64  `toml:"event-queue-size" json:"event-queue-size"`
	BalanceSleepTime int64  `toml:"balance-sleep-time" json:"balance-sleep-time"`
}

type WorkerOption func(opts *WorkerOptions)

func DefaultWorkerServerConfig() *WorkerOptions {
	return &WorkerOptions{
		Name:             DefaultWorkerNamePrefix,
		Endpoint:         DefaultMasterClientAddr,
		WorkerAddr:       DefaultWorkerRegisterAddr,
		KeepaliveTTL:     DefaultWorkerKeepaliveTTL,
		EventQueueSize:   DefaultEventQueueChannelSize,
		BalanceSleepTime: DefaultBalanceSleepTime,
	}
}

func WithWorkerName(name string) WorkerOption {
	return func(opts *WorkerOptions) {
		opts.Name = name
	}
}

func WithWorkerAddr(addr string) WorkerOption {
	return func(opts *WorkerOptions) {
		opts.WorkerAddr = addr
	}
}

func WithMasterEndpoint(addr string) WorkerOption {
	return func(opts *WorkerOptions) {
		opts.Endpoint = addr
	}
}

func WithWorkerLease(aliveTTL int64) WorkerOption {
	return func(opts *WorkerOptions) {
		opts.KeepaliveTTL = aliveTTL
	}
}

func WithEventQueueSize(queueSize int64) WorkerOption {
	return func(opts *WorkerOptions) {
		opts.EventQueueSize = queueSize
	}
}

func WithBalanceSleepTime(time int64) WorkerOption {
	return func(opts *WorkerOptions) {
		opts.BalanceSleepTime = time
	}
}
