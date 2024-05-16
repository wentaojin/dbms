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
	"fmt"
	"os"

	"github.com/wentaojin/dbms/utils/cluster"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/executor"
)

// Options represents the operation options
type Options struct {
	Roles               []string
	Nodes               []string
	Force               bool             // Option for upgrade/tls subcommand
	SSHTimeout          uint64           // timeout in seconds when connecting an SSH server
	OptTimeout          uint64           // timeout in seconds for operations that support it, not to confuse with SSH timeout
	APITimeout          uint64           // timeout in seconds for API operations that support it, like transferring store leader
	SSHType             executor.SSHType // the ssh type: 'builtin', 'system', 'none'
	Concurrency         int              // max number of parallel tasks to run
	SSHProxyHost        string           // the ssh proxy host
	SSHProxyPort        int              // the ssh proxy port
	SSHProxyUser        string           // the ssh proxy user
	SSHProxyIdentity    string           // the ssh proxy identity file
	SSHProxyUsePassword bool             // use password instead of identity file for ssh proxy connection
	SSHProxyTimeout     uint64           // timeout in seconds when connecting the proxy host
	SSHCustomScripts    SSHCustomScripts // custom scripts to be executed during the operation

	// What type of things should we cleanup in clean command
	CleanupData     bool // should we cleanup data
	CleanupLog      bool // should we clenaup log
	CleanupAuditLog bool // should we clenaup tidb server auit log

	// Some data will be retained when destroying instances
	RetainDataRoles []string
	RetainDataNodes []string

	DisplayMode string // the output format

	MetaDir     string // the metadata dir
	MirrorDir   string // the mirror dir
	SkipConfirm bool
	Operation   Operation
}

// SSHCustomScripts represents the custom ssh script set to be executed during cluster operations
type SSHCustomScripts struct {
	BeforeRestartInstance SSHCustomScript
	AfterRestartInstance  SSHCustomScript
}

// SSHCustomScript represents a custom ssh script to be executed during cluster operations
type SSHCustomScript struct {
	Raw string
}

// Command returns the ssh command in string format
func (s SSHCustomScript) Command() string {
	b, err := os.ReadFile(s.Raw)
	if err != nil {
		return s.Raw
	}
	return string(b)
}

// Operation represents the type of cluster operation
type Operation byte

// Operation represents the kind of cluster operation
const (
	// StartOperation Operation = iota
	// StopOperation
	RestartOperation Operation = iota
	DestroyOperation
	UpgradeOperation
	ScaleInOperation
	ScaleOutOperation
	DestroyTombstoneOperation
)

var opStringify = [...]string{
	"StartOperation",
	"StopOperation",
	"RestartOperation",
	"DestroyOperation",
	"UpgradeOperation",
	"ScaleInOperation",
	"ScaleOutOperation",
	"DestroyTombstoneOperation",
}

func (op Operation) String() string {
	if op <= DestroyTombstoneOperation {
		return opStringify[op]
	}
	return fmt.Sprintf("unknonw-op(%d)", op)
}

// FilterComponent filter components by set
func FilterComponent(comps []cluster.Component, components stringutil.StringSet) (res []cluster.Component) {
	if len(components) == 0 {
		res = comps
		return
	}

	for _, c := range comps {
		if !components.Exist(c.ComponentRole()) {
			continue
		}
		res = append(res, c)
	}

	return
}

// FilterInstance filter instances by set
func FilterInstance(instances []cluster.Instance, nodes stringutil.StringSet) (res []cluster.Instance) {
	if len(nodes) == 0 {
		res = instances
		return
	}

	for _, c := range instances {
		if !nodes.Exist(c.InstanceName()) {
			continue
		}
		res = append(res, c)
	}

	return
}
