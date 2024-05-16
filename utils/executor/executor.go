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
package executor

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/joomcode/errorx"
	"github.com/pkg/errors"
)

// SSHType represent the type of the chanel used by ssh
type SSHType string

var (
	errNS = errorx.NewNamespace("executor")

	// SSH authorized_keys file
	defaultSSHAuthorizedKeys = "~/.ssh/authorized_keys"

	// SSHTypeBuiltin is the type of easy ssh executor
	SSHTypeBuiltin SSHType = "builtin"

	// SSHTypeNone is the type of local executor (no ssh will be used)
	SSHTypeNone SSHType = "none"
)

// Executor is the SSH executor interface, all tasks will be executed through the SSH executor
type Executor interface {
	// Execute the run command and return stdout and stderr
	// If cmd times out and cannot exit, an error will be returned. The default timeout is 60 seconds.
	Execute(ctx context.Context, cmd string, sudo bool, timeout ...time.Duration) (stdout []byte, stderr []byte, err error)

	// Transfer copy files from or to target
	Transfer(ctx context.Context, src, dst string, download bool, limit int) error
}

func New(etype SSHType, sudo bool, c SSHConfig) (Executor, error) {
	// set default values
	if c.Port <= 0 {
		c.Port = 22
	}

	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = time.Duration(DefaultConnectTimeout) * time.Second // 默认 SSH 连接超时时间
	}

	var executor Executor
	switch etype {
	case SSHTypeBuiltin:
		e := &EasySSHExecutor{
			Locale: "C",
			Sudo:   sudo,
		}
		e.initialize(c)
		executor = e
	case SSHTypeNone:
		if err := checkLocalIP(c.Host); err != nil {
			return nil, err
		}
		e := &Local{
			Config: &c,
			Sudo:   sudo,
			Locale: "C",
		}
		executor = e
	default:
		return nil, errors.Errorf("unregistered executor: %s", etype)
	}

	return executor, nil
}

func checkLocalIP(ip string) error {
	ifaces, err := net.Interfaces()
	if err != nil {
		return err
	}

	var foundIps []string
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				if ip == v.IP.String() {
					return nil
				}
				foundIps = append(foundIps, v.IP.String())
			case *net.IPAddr:
				if ip == v.IP.String() {
					return nil
				}
				foundIps = append(foundIps, v.IP.String())
			}
		}
	}

	return fmt.Errorf("address %s not found in all interfaces, found ips: %s", ip, strings.Join(foundIps, ","))
}

// FindSSHAuthorizedKeysFile Find the correct path to the SSH authorization key file
func FindSSHAuthorizedKeysFile(ctx context.Context, exec Executor) string {
	// Detect whether a custom path to the authorization key file is set
	// NOTE: we do not yet support:
	//   - custom config for user (~/.ssh/config)
	//   - sshd started with custom config (other than /etc/ssh/sshd_config)
	//   - ssh server implementations other than OpenSSH (such as dropbear)
	sshAuthorizedKeys := defaultSSHAuthorizedKeys
	cmd := "grep -Ev '^\\s*#|^\\s*$' /etc/ssh/sshd_config"

	// Errors are ignored because there is a default value
	stdout, _, _ := exec.Execute(ctx, cmd, true)
	for _, line := range strings.Split(string(stdout), "\n") {
		if !strings.Contains(line, "AuthorizedKeysFile") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			sshAuthorizedKeys = fields[1]
			break
		}
	}

	if !strings.HasPrefix(sshAuthorizedKeys, "/") && !strings.HasPrefix(sshAuthorizedKeys, "~") {
		sshAuthorizedKeys = fmt.Sprintf("~/%s", sshAuthorizedKeys)
	}
	return sshAuthorizedKeys
}
