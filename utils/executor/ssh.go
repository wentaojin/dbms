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
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/wentaojin/dbms/logger"

	"github.com/joomcode/errorx"

	"go.uber.org/zap"

	"github.com/appleboy/easyssh-proxy"
)

const (
	// DefaultExecuteTimeout default SSH command execution timeout, unit: seconds
	DefaultExecuteTimeout = 60
	// DefaultConnectTimeout default SSH connection timeout, unit: seconds
	DefaultConnectTimeout = 10
)

var (
	errNSSSH              = errNS.NewSubNamespace("ssh")
	ErrPropSSHCommand     = errorx.RegisterPrintableProperty("ssh_command")
	ErrPropSSHStdout      = errorx.RegisterPrintableProperty("ssh_stdout")
	ErrPropSSHStderr      = errorx.RegisterPrintableProperty("ssh_stderr")
	ErrSSHExecuteFailed   = errNSSSH.NewType("execute_failed")
	ErrSSHExecuteTimedout = errNSSSH.NewType("execute_timeout")
)

// EasySSHExecutor implements EasySSH Executor as the SSH transport protocol layer
type (
	EasySSHExecutor struct {
		Config *easyssh.MakeConfig
		Locale string // The locale to use when executing the command
		Sudo   bool   // Whether all commands run using this executor use sudo
	}

	// SSHConfig is the configuration required to establish an SSH connection
	SSHConfig struct {
		Host           string        // SSH Host
		Port           int           // SSH Host Port
		User           string        // SSH Host Username
		Password       string        // SSH Host Username Password
		KeyFile        string        // SSH Private Key File
		Passphrase     string        // SSH Private Key Password
		ConnectTimeout time.Duration // TCP Connect timeout
		ExecuteTimeout time.Duration // Command execute timeout
		Proxy          *SSHConfig    // ssh proxy config
	}
)

var _ Executor = &EasySSHExecutor{}

// Execute the run command over SSH, which does not invoke any specific shell by default
func (e *EasySSHExecutor) Execute(ctx context.Context, cmd string, sudo bool, execTimeout ...time.Duration) ([]byte, []byte, error) {
	// Try to gain root privileges
	if e.Sudo || sudo {
		cmd = fmt.Sprintf("sudo -H bash -c \"%s\"", cmd)
	}

	// Set a basic PATH in case it is empty when logging in
	cmd = fmt.Sprintf("PATH=$PATH:/usr/bin:/usr/sbin %s", cmd)

	if e.Locale != "" {
		cmd = fmt.Sprintf("export LANG=%s; %s", e.Locale, cmd)
	}

	//Run the command on the remote host
	//The default timeout in easyssh-proxy is 60 seconds
	if len(execTimeout) == 0 {
		execTimeout = append(execTimeout, time.Duration(DefaultExecuteTimeout)*time.Second)
	}

	stdout, stderr, done, err := e.Config.Run(cmd, execTimeout...)
	logger.Info("SSHCommand",
		zap.String("host", e.Config.Server),
		zap.String("port", e.Config.Port),
		zap.String("cmd", cmd),
		zap.Error(err),
		zap.String("stdout", stdout),
		zap.String("stderr", stderr))
	if err != nil {
		sshErr := ErrSSHExecuteFailed.
			Wrap(err, "Failed to execute command over SSH for '%s@%s:%s'", e.Config.User, e.Config.Server, e.Config.Port).
			WithProperty(ErrPropSSHCommand, cmd).
			WithProperty(ErrPropSSHStdout, stdout).
			WithProperty(ErrPropSSHStderr, stderr)
		if len(stdout) > 0 || len(stderr) > 0 {
			output := strings.TrimSpace(strings.Join([]string{stdout, stderr}, "\n"))
			sshErr = sshErr.
				WithProperty(
					errorx.RegisterPrintableProperty(
						fmt.Sprintf("Command output on remote host %s", e.Config.Server)),
					output)
		}
		return []byte(stdout), []byte(stderr), sshErr
	}
	// execute timeout
	if !done {
		return []byte(stdout), []byte(stderr), ErrSSHExecuteTimedout.
			Wrap(err, "Execute command over SSH timeout for '%s@%s:%s'", e.Config.User, e.Config.Server, e.Config.Port).
			WithProperty(ErrPropSSHCommand, cmd).
			WithProperty(ErrPropSSHStdout, stdout).
			WithProperty(ErrPropSSHStderr, stderr)
	}

	return []byte(stdout), []byte(stderr), nil
}

// Transfer the copy file via SCP
// This function depends on `scp` (tools from OpenSSH or other SSH implementations)
// This function is based on easyssh.MakeConfig.Scp() but supports copying to remote files
func (e *EasySSHExecutor) Transfer(ctx context.Context, src, dst string, download bool, limit int) error {
	// Call Scp method with file you want to upload to remote server.
	if !download {
		err := e.Config.Scp(src, dst)
		if err != nil {
			return errors.Annotatef(err, "failed to scp %s to %s@%s:%s", src, e.Config.User, e.Config.Server, dst)
		}
		return nil
	}

	// download file from remote
	session, client, err := e.Config.Connect()
	if err != nil {
		return err
	}
	defer client.Close()
	defer session.Close()

	return ScpDownload(session, client, src, dst, limit)
}

// Initialize the build and initialize a EasySSHExecutor
func (e *EasySSHExecutor) initialize(config SSHConfig) {
	// Create easyssh configuration
	e.Config = &easyssh.MakeConfig{
		Server:  config.Host,
		Port:    strconv.Itoa(config.Port),
		User:    config.User,
		Timeout: config.ConnectTimeout,
	}

	// If there is a private key, use the private key for authentication first.
	if len(config.KeyFile) > 0 {
		e.Config.KeyPath = config.KeyFile
		e.Config.Passphrase = config.Passphrase
	} else if len(config.Password) > 0 {
		e.Config.Password = config.Password
	}
}
