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
package operator

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/wentaojin/dbms/logger/printer"
	"github.com/wentaojin/dbms/utils/cluster"

	"errors"

	"go.uber.org/zap"

	"github.com/ScaleFT/sshkeys"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/ctxt"
	"github.com/wentaojin/dbms/utils/executor"
	"github.com/wentaojin/dbms/utils/module"
	"github.com/wentaojin/dbms/utils/stringutil"
	"golang.org/x/crypto/ssh"
)

// SSHConnectionProps is SSHConnectionProps
type SSHConnectionProps struct {
	Password               string
	IdentityFile           string
	IdentityFilePassphrase string
}

// ReadIdentityFileOrPassword is ReadIdentityFileOrPassword
func ReadIdentityFileOrPassword(identityFilePath string, usePass bool) (*SSHConnectionProps, error) {
	// If identity file is not specified, prompt to read password
	if usePass {
		password := stringutil.PromptForPassword("Input SSH password: ")
		return &SSHConnectionProps{
			Password: password,
		}, nil
	}

	// Identity file is specified, check identity file
	if len(identityFilePath) > 0 && stringutil.IsPathExist(identityFilePath) {
		buf, err := os.ReadFile(identityFilePath)
		if err != nil {
			return nil, fmt.Errorf(`failed to read SSH identity file [%s], please check whether your SSH identity file [%v] exists and have access permission, error detail: %v
`, identityFilePath, identityFilePath, err)
		}

		// Try to decode as not encrypted
		_, err = ssh.ParsePrivateKey(buf)
		if err == nil {
			return &SSHConnectionProps{
				IdentityFile: identityFilePath,
			}, nil
		}

		// Other kind of error.. e.g. not a valid SSH key
		var passphraseMissingError *ssh.PassphraseMissingError
		if !errors.As(err, &passphraseMissingError) {
			return nil, fmt.Errorf(`failed to read SSH identity file [%s], looks like your SSH private key [%v] is invalid, error detail: %v`, identityFilePath, identityFilePath, err)
		}

		// SSH key is passphrase protected
		passphrase := stringutil.PromptForPassword("The SSH identity key is encrypted. Input its passphrase: ")
		if _, err := sshkeys.ParseEncryptedPrivateKey(buf, []byte(passphrase)); err != nil {
			return nil, fmt.Errorf(`failed to decrypt SSH identity file [%s], error detail: %v`, identityFilePath, err)
		}

		return &SSHConnectionProps{
			IdentityFile:           identityFilePath,
			IdentityFilePassphrase: passphrase,
		}, nil
	}

	// No password, nor identity file were specified, check ssh-agent via the env SSH_AUTH_SOCK
	sshAuthSock := os.Getenv("SSH_AUTH_SOCK")
	if len(sshAuthSock) == 0 {
		return nil, fmt.Errorf("none of ssh password, identity file, SSH_AUTH_SOCK specified")
	}
	stat, err := os.Stat(sshAuthSock)
	if err != nil {
		return nil, fmt.Errorf(`failed to stat SSH_AUTH_SOCK file [%s], error detail: %v`, sshAuthSock, err)
	}
	if stat.Mode()&os.ModeSocket == 0 {
		return nil, fmt.Errorf("the SSH_AUTH_SOCK file [%s] is not a valid unix socket file, error detail: %v", sshAuthSock, err)
	}

	return &SSHConnectionProps{}, nil
}

// DeletePublicKey deletes the SSH public key from host
func DeletePublicKey(ctx context.Context, host string) error {
	e, exist := ctxt.GetInner(ctx).GetExecutor(host)
	if !exist {
		return errors.New("no executor")
	}
	logger.Info("Delete public key", zap.String("host", host))
	_, pubKeyPath := ctxt.GetInner(ctx).GetSSHKeySet()
	publicKey, err := os.ReadFile(pubKeyPath)
	if err != nil {
		return err
	}

	pubKey := string(bytes.TrimSpace(publicKey))
	pubKey = strings.ReplaceAll(pubKey, "/", "\\/")
	pubKeysFile := executor.FindSSHAuthorizedKeysFile(ctx, e)

	// delete the public key with Linux `sed` toolkit
	c := module.ShellModuleConfig{
		Command:  fmt.Sprintf("sed -i '/%s/d' %s", pubKey, pubKeysFile),
		UseShell: false,
	}
	shell := module.NewShellModule(c)
	stdout, stderr, err := shell.Execute(ctx, e)

	if len(stdout) > 0 {
		logger.Info(string(stdout))
	}
	if len(stderr) > 0 {
		logger.Error("Delete public key failed", zap.String("host", host), zap.String("error", string(stderr)))
		return fmt.Errorf("failed to delete host [%s] public key, stderr detial: %v", host, string(stderr))
	}

	if err != nil {
		logger.Error("Delete public key failed", zap.String("host", host), zap.Error(err))
		return fmt.Errorf("failed to delete host [%s] public key, error detial: %v", host, err)
	}
	logger.Info("Delete public key success", zap.String("host", host))
	return nil
}

// DeleteGlobalDirs deletes all global directories if they are empty
func DeleteGlobalDirs(ctx context.Context, host string, options *cluster.GlobalOptions) error {
	if options == nil {
		return nil
	}

	e := ctxt.GetInner(ctx).Get(host)
	logger := ctx.Value(printer.ContextKeyLogger).(*printer.Logger)
	logger.Infof("Clean global directories %s", host)

	// remove instant client
	instantDir := filepath.Join(options.DeployDir, cluster.InstantClientDir)
	logger.Infof("\tClean directory %s on instance %s", instantDir, host)

	c := module.ShellModuleConfig{
		Command:  fmt.Sprintf("rm -rf %s;", instantDir),
		Chdir:    "",
		UseShell: false,
	}
	shell := module.NewShellModule(c)
	stdout, stderr, err := shell.Execute(ctx, e)

	if len(stdout) > 0 {
		logger.Infof(string(stdout))
	}
	if len(stderr) > 0 {
		logger.Errorf(string(stderr))
	}

	if err != nil {
		return fmt.Errorf("failed to clean directory %s on: %s, error detail: %v", instantDir, host, err)
	}

	// remove global dirs
	for _, dir := range []string{options.LogDir, options.DeployDir, options.DataDir} {
		if dir == "" {
			continue
		}
		dir = stringutil.Abs(options.User, dir)

		logger.Infof("\tClean directory %s on instance %s", dir, host)

		c := module.ShellModuleConfig{
			Command:  fmt.Sprintf("rmdir %s > /dev/null 2>&1 || true", dir),
			Chdir:    "",
			UseShell: false,
		}
		s := module.NewShellModule(c)
		stdout, stderr, err = s.Execute(ctx, e)

		if len(stdout) > 0 {
			logger.Infof(string(stdout))
		}
		if len(stderr) > 0 {
			logger.Errorf(string(stderr))
		}

		if err != nil {
			return fmt.Errorf("failed to clean directory %s on: %s, error detail: %v", dir, host, err)
		}
	}

	logger.Infof("Clean global directories %s success", host)
	return nil
}
