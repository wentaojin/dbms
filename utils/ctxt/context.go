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
package ctxt

import (
	"context"
	"runtime"
	"sync"

	"github.com/wentaojin/dbms/logger/printer"

	"github.com/wentaojin/dbms/utils/executor"
)

type contextKey string

const (
	ctxKey = contextKey("TASK_CONTEXT")
)

// ExecutorGetter get the executor by host.
type ExecutorGetter interface {
	Get(host string) (e executor.Executor)
	// GetSSHKeySet gets the SSH private and public key path
	GetSSHKeySet() (privateKeyPath, publicKeyPath string)
}

// Context is used to share state when multiple tasks are executed.
// Use Mutex locks to prevent concurrent reading/writing of certain fields
// Because the same context can be shared in parallel tasks.
type Context struct {
	Mutex sync.RWMutex

	Ev EventBus

	Exec struct {
		Executors    map[string]executor.Executor
		Stdouts      map[string][]byte
		Stderrs      map[string][]byte
		CheckResults map[string][]interface{}
	}

	// Private key/public key is used to access the remote server through the user
	PrivateKeyPath string
	PublicKeyPath  string
	Concurrency    int // max number of parallel tasks running at the same time
}

// New create a context instance.
func New(ctx context.Context, limit int, logger *printer.Logger) context.Context {
	concurrency := runtime.NumCPU()
	if limit > 0 {
		concurrency = limit
	}

	return context.WithValue(
		context.WithValue(
			ctx,
			printer.ContextKeyLogger,
			logger,
		),
		ctxKey,
		&Context{
			Mutex: sync.RWMutex{},
			Ev:    NewEventBus(),
			Exec: struct {
				Executors    map[string]executor.Executor
				Stdouts      map[string][]byte
				Stderrs      map[string][]byte
				CheckResults map[string][]any
			}{
				Executors:    make(map[string]executor.Executor),
				Stdouts:      make(map[string][]byte),
				Stderrs:      make(map[string][]byte),
				CheckResults: make(map[string][]any),
			},
			Concurrency: concurrency, // default to CPU count
		},
	)
}

// GetInner return *Context from context.Context's value
func GetInner(ctx context.Context) *Context {
	return ctx.Value(ctxKey).(*Context)
}

// Get implements the operation.ExecutorGetter interface.
func (ctx *Context) Get(host string) (e executor.Executor) {
	ctx.Mutex.Lock()
	e, ok := ctx.Exec.Executors[host]
	ctx.Mutex.Unlock()

	if !ok {
		panic("no init executor for " + host)
	}
	return
}

// GetSSHKeySet implements the operation.ExecutorGetter interface.
func (ctx *Context) GetSSHKeySet() (privateKeyPath, publicKeyPath string) {
	return ctx.PrivateKeyPath, ctx.PublicKeyPath
}

// GetExecutor get the executor.
func (ctx *Context) GetExecutor(host string) (e executor.Executor, ok bool) {
	ctx.Mutex.RLock()
	e, ok = ctx.Exec.Executors[host]
	ctx.Mutex.RUnlock()
	return
}

// SetExecutor set the executor.
func (ctx *Context) SetExecutor(host string, e executor.Executor) {
	ctx.Mutex.Lock()
	if e != nil {
		ctx.Exec.Executors[host] = e
	} else {
		delete(ctx.Exec.Executors, host)
	}
	ctx.Mutex.Unlock()
}

// GetOutputs get the outputs of a host (if has any)
func (ctx *Context) GetOutputs(hostID string) ([]byte, []byte, bool) {
	ctx.Mutex.RLock()
	stdout, ok1 := ctx.Exec.Stdouts[hostID]
	stderr, ok2 := ctx.Exec.Stderrs[hostID]
	ctx.Mutex.RUnlock()
	return stdout, stderr, ok1 && ok2
}

// SetOutputs set the outputs of a host
func (ctx *Context) SetOutputs(hostID string, stdout []byte, stderr []byte) {
	ctx.Mutex.Lock()
	ctx.Exec.Stdouts[hostID] = stdout
	ctx.Exec.Stderrs[hostID] = stderr
	ctx.Mutex.Unlock()
}

// GetCheckResults get the the check result of a host (if has any)
func (ctx *Context) GetCheckResults(host string) (results []any, ok bool) {
	ctx.Mutex.RLock()
	results, ok = ctx.Exec.CheckResults[host]
	ctx.Mutex.RUnlock()
	return
}

// SetCheckResults append the check result of a host to the list
func (ctx *Context) SetCheckResults(host string, results []any) {
	ctx.Mutex.Lock()
	if currResult, ok := ctx.Exec.CheckResults[host]; ok {
		ctx.Exec.CheckResults[host] = append(currResult, results...)
	} else {
		ctx.Exec.CheckResults[host] = results
	}
	ctx.Mutex.Unlock()
}
