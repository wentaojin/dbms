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
package cluster

import (
	"context"
	"time"

	"github.com/wentaojin/dbms/utils/executor"
	"github.com/wentaojin/dbms/utils/module"
)

// PortStarted wait until a port is being listened
func PortStarted(ctx context.Context, e executor.Executor, port int, timeout uint64) error {
	c := module.WaitForConfig{
		Port:    port,
		State:   "started",
		Timeout: time.Duration(timeout) * time.Second,
	}
	w := module.NewWaitFor(c)
	return w.Execute(ctx, e)
}

// PortStopped wait until a port is being released
func PortStopped(ctx context.Context, e executor.Executor, port int, timeout uint64) error {
	c := module.WaitForConfig{
		Port:    port,
		State:   "stopped",
		Timeout: time.Duration(timeout) * time.Second,
	}
	w := module.NewWaitFor(c)
	return w.Execute(ctx, e)
}
