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
	"context"
	"fmt"
	"github.com/wentaojin/dbms/utils/executor"
	"strings"
	"time"
)

// LaunchdModuleConfig is the configurations used to initialize a LaunchdModule
type LaunchdModuleConfig struct {
	Unit        string        // the name of systemd unit(s)
	Action      string        // the action to perform with the unit
	Timeout     time.Duration // timeout to execute the command
	SystemdMode string
}

// LaunchdModule is the module used to control systemd units
type LaunchdModule struct {
	cmd     string        // the built command
	sudo    bool          // does the command need to be run as root
	timeout time.Duration // timeout to execute the command
}

// NewLaunchdModule builds and returns a LaunchdModule object base on
// given config.
func NewLaunchdModule(config LaunchdModuleConfig) *LaunchdModule {
	systemctl := "launchctl"

	var action string
	sudo := true

	switch strings.ToLower(config.Action) {
	case "start":
		action = fmt.Sprintf("bootstrap system /Library/LaunchAgents/%s", config.Unit)
	case "stop":
		action = fmt.Sprintf("bootout system /Library/LaunchAgents/%s", config.Unit)
	case "disable":
		action = fmt.Sprintf("disable system/%s", config.Unit)
	case "enable":
		action = fmt.Sprintf("enable system/%s", config.Unit)
	default:
		action = fmt.Sprintf("%s %s", strings.ToLower(config.Action), config.Unit)
	}

	if config.SystemdMode == "user" {
		switch strings.ToLower(config.Action) {
		case "start":
			action = fmt.Sprintf("bootstrap gui/$(id -u) ~/Library/LaunchAgents/%s", config.Unit)
		case "stop":
			action = fmt.Sprintf("bootout gui/$(id -u) ~/Library/LaunchAgents/%s", config.Unit)
		case "disable":
			action = fmt.Sprintf("disable gui/$(id -u)/%s", config.Unit)
		case "enable":
			action = fmt.Sprintf("enable gui/$(id -u)/%s", config.Unit)
		default:
			action = fmt.Sprintf("%s %s", strings.ToLower(config.Action), config.Unit)
		}
		sudo = false
	}

	cmd := fmt.Sprintf("%s %s", systemctl, action)

	mod := &LaunchdModule{
		cmd:     cmd,
		sudo:    sudo,
		timeout: config.Timeout,
	}

	// the default TimeoutStopSec of systemd is 90s, after which it sends a SIGKILL
	// to remaining processes, set the default value slightly larger than it
	if config.Timeout == 0 {
		mod.timeout = time.Second * 100
	}

	return mod
}

// Execute passes the command to executor and returns its results, the executor
// should be already initialized.
func (mod *LaunchdModule) Execute(ctx context.Context, exec executor.Executor) ([]byte, []byte, error) {
	return exec.Execute(ctx, mod.cmd, mod.sudo, mod.timeout)
}
