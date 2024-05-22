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
package module

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/wentaojin/dbms/logger"

	"github.com/wentaojin/dbms/utils/executor"

	"go.uber.org/zap"
)

const (
	// DefaultSystemdSleepTime unit: seconds
	DefaultSystemdSleepTime = 1
	// DefaultSystemdExecuteTimeout unit: seconds
	DefaultSystemdExecuteTimeout = 60
)

// WaitForConfig is the configurations of WaitFor module.
type WaitForConfig struct {
	OS    string        // OS version
	Port  int           // Port number to poll.
	Sleep time.Duration // Duration to sleep between checks, default 1 second.
	// Choices:
	// started
	// stopped
	// When checking a port started will ensure the port is open, stopped will check that it is closed
	State   string
	Timeout time.Duration // Maximum duration to wait for.
}

// WaitFor is the module used to wait for some condition.
type WaitFor struct {
	c WaitForConfig
}

// NewWaitFor create a WaitFor instance.
func NewWaitFor(c WaitForConfig) *WaitFor {
	if c.Sleep == 0 {
		c.Sleep = time.Duration(DefaultSystemdSleepTime) * time.Second
	}
	if c.Timeout == 0 {
		c.Timeout = time.Duration(DefaultSystemdExecuteTimeout) * time.Second
	}
	if c.State == "" {
		c.State = "started"
	}

	w := &WaitFor{
		c: c,
	}

	return w
}

// Execute the module return nil if successfully wait for the event.
func (w *WaitFor) Execute(ctx context.Context, e executor.Executor) (err error) {
	pattern := []byte(fmt.Sprintf(":%d ", w.c.Port))

	retryOpt := RetryOption{
		Delay:   w.c.Sleep,
		Timeout: w.c.Timeout,
	}
	if err = Retry(func() error {
		if strings.EqualFold(w.c.OS, "darwin") {
			// only listing TCP ports
			cmd := fmt.Sprintf("lsof -i :%d", w.c.Port)
			stdout, stderr, err := e.Execute(ctx, cmd, false)
			if len(stderr) != 0 || err != nil {
				return fmt.Errorf("command [%s] executed failed, stderr: %v, err: %v", cmd, string(stderr), err)
			}
			if bytes.Contains(stdout, []byte("LISTEN")) {
				return nil
			}
			return errors.New("still waiting for port state to be satisfied")
		}
		// only listing TCP ports
		stdout, stderr, err := e.Execute(ctx, "ss -ltn", false)
		if len(stderr) != 0 || err != nil {
			return fmt.Errorf("command [ss -ltn] executed failed, stderr: %v, err: %v", string(stderr), err)
		}
		switch w.c.State {
		case "started":
			if bytes.Contains(stdout, pattern) {
				return nil
			}
		case "stopped":
			if !bytes.Contains(stdout, pattern) {
				return nil
			}
		}
		return errors.New("still waiting for port state to be satisfied")
	}, retryOpt); err != nil {
		logger.Debug("retry error: %s", zap.Error(err))
		return fmt.Errorf("timed out waiting for port %d to be %s after %s, err: %v", w.c.Port, w.c.State, w.c.Timeout, err)
	}
	return nil
}
