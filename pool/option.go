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
)

// Option represents an option for the pool.
type Option func(*pool)

// WithExecuteHandle sets the task func for the pool.
func WithExecuteHandle(taskFn func(ctx context.Context, t Task) error) Option {
	return func(p *pool) {
		p.executeHandleFn = taskFn
	}
}

// WithResultCallback sets the result callback for the pool.
func WithResultCallback(callback func(r Result)) Option {
	return func(p *pool) {
		p.resultCallback = callback
	}
}

// WithRetryCount sets the retry count for the pool.
func WithRetryCount(retryCount int) Option {
	return func(p *pool) {
		p.retryCount = retryCount
	}
}

// WithTaskQueueSize sets the size of the task queue for the pool.
func WithTaskQueueSize(size int) Option {
	return func(p *pool) {
		p.taskQueueSize = size
	}
}

// WithPanicHandle sets the method of the task panic for the pool.
func WithPanicHandle(panicH bool) Option {
	return func(p *pool) {
		p.panicHandle = panicH
	}
}

// WithCanceledHandle sets the method of the task canceled for the pool
func WithCanceledHandle(cancelH func(ctx context.Context, t Task) error) Option {
	return func(p *pool) {
		p.canceledHandleFn = cancelH
	}
}
