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
	"github.com/wentaojin/dbms/utils/stringutil"
	"strings"

	"github.com/wentaojin/dbms/utils/executor"
)

const (
	defaultShell = "/bin/bash"

	// UserActionAdd
	UserActionAdd = "add"
	UserActionDel = "del"
	// UserActionModify = "modify"

	// TODO: In RHEL/CentOS the commands are in /usr/sbin but in some other distributions they may be in other locations like /usr/bin and we will need to check and find the correct path for the commands in the future
	useraddCmd  = "/usr/sbin/useradd"
	userdelCmd  = "/usr/sbin/userdel"
	groupaddCmd = "/usr/sbin/groupadd"
	// usermodCmd = "/usr/sbin/usermod"
)

// UserModuleConfig is the configuration used to initialize UserModule
type UserModuleConfig struct {
	OS     string // OS Version
	Action string // Create, delete or change users
	Name   string // username
	Group  string // user group
	Home   string // user home
	Shell  string // user login shell
	Sudoer bool   // When true, the user will be added to the sudoers list
}

// UserModule is the module used to control systemd units
type UserModule struct {
	config UserModuleConfig
	cmd    string // Shell command to be executed
}

func NewUserModule(config UserModuleConfig) *UserModule {
	cmd := ""

	switch config.Action {
	case UserActionAdd:
		cmd = useraddCmd
		// -m must be used, otherwise the home directory will not be created. If you want to specify the path to the home directory, use -d and specify the path
		// useradd -m -d /PATH/TO/FOLDER
		cmd += " -m"
		if config.Home != "" {
			cmd += " -d" + config.Home
		}

		//Set the user's login shell
		if config.Shell != "" {
			cmd = fmt.Sprintf("%s -s %s", cmd, config.Shell)
		} else {
			cmd = fmt.Sprintf("%s -s %s", cmd, defaultShell)
		}

		//Set user group
		if config.Group == "" {
			config.Group = config.Name
		}

		// groupadd -f <group-name>
		groupAdd := fmt.Sprintf("%s -f %s", groupaddCmd, config.Group)

		// useradd -g <group-name> <user-name>
		cmd = fmt.Sprintf("%s -g %s %s", cmd, config.Group, config.Name)

		// chown privilege and group
		var chownCmd string
		if config.Home != "" {
			chownCmd = fmt.Sprintf("chown %s:%s %s", config.Name, config.Group, config.Home)
		} else {
			chownCmd = fmt.Sprintf("chown %s:%s %s", config.Name, config.Group, fmt.Sprintf("/home/%s", config.Name))
		}

		//Prevent errors when the username is already in use
		cmd = fmt.Sprintf("id -u %s > /dev/null 2>&1 || (%s && %s && %s)", config.Name, groupAdd, cmd, chownCmd)

		//Add user to sudoers list
		if config.Sudoer {
			if strings.EqualFold(config.OS, "linux") {
				cmd = fmt.Sprintf("%s && %s",
					cmd,
					fmt.Sprintf("cat > /etc/sudoers.d/%s << EOF\n%s\nEOF", config.Name, stringutil.StringJoin(stringutil.GetTopologyUserSudoPrivileges(config.Name), "\n")))
			} else {
				sudoLine := fmt.Sprintf("%s ALL=(ALL) NOPASSWD:ALL",
					config.Name)
				cmd = fmt.Sprintf("%s && %s",
					cmd,
					fmt.Sprintf("echo '%s' > /etc/sudoers.d/%s", sudoLine, config.Name))
			}
		}

	case UserActionDel:
		cmd = fmt.Sprintf("%s -r %s", userdelCmd, config.Name)
		// prevent errors when user does not exist
		cmd = fmt.Sprintf("%s || [ $? -eq 6 ]", cmd)

		//	case UserActionModify:
		//		cmd = usermodCmd
	}

	return &UserModule{
		config: config,
		cmd:    cmd,
	}
}

// Execute passes the command to the executor and returns its result. The executor should have been initialized
func (mod *UserModule) Execute(ctx context.Context, exec executor.Executor) ([]byte, []byte, error) {
	a, b, err := exec.Execute(ctx, mod.cmd, true)
	if err != nil {
		switch mod.config.Action {
		case UserActionAdd:
			return a, b, ErrUserAddFailed.
				Wrap(err, "Failed to create new system user '%s' on remote host", mod.config.Name)
		case UserActionDel:
			return a, b, ErrUserDeleteFailed.
				Wrap(err, "Failed to delete system user '%s' on remote host", mod.config.Name)
		}
	}
	return a, b, nil
}
