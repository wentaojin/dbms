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
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/wentaojin/dbms/utils/cluster/embed/launchd"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/cluster/embed/script"

	"github.com/google/uuid"
	"github.com/pingcap/errors"

	"github.com/wentaojin/dbms/utils/cluster/embed/systemd"

	"github.com/wentaojin/dbms/utils/executor"
)

const (
	ComponentDBMSMaster = "dbms-master"
	ComponentDBMSWorker = "dbms-worker"
)

type DBMSMasterComponent struct {
	Topology *Topology
}

func (m *DBMSMasterComponent) ComponentName() string {
	return ComponentDBMSMaster
}

func (m *DBMSMasterComponent) ComponentRole() string {
	return ComponentDBMSMaster
}

func (m *DBMSMasterComponent) Instances() []Instance {
	ins := make([]Instance, 0)
	for _, s := range m.Topology.MasterServers {
		ins = append(ins, &MasterInstance{
			BaseInstance: BaseInstance{
				InstanceTopology: s,
				Host:             s.Host,
				ManageHost:       s.ManageHost,
				// ListenHost: m.Topology.GlobalOptions.ListenHost, // TODO: current isnot support
				ListenHost: s.Host,
				Port:       s.Port,
				SSHPort:    s.SSHPort,
				PeerPort:   s.PeerPort,
				NumaNode:   s.InstanceNumaNode,
				DeployDir:  s.DeployDir,
				DataDir:    s.DataDir,
				LogDir:     s.LogDir,
				Patched:    s.Patched,
				OSVersion:  s.OS,
				OSArch:     s.Arch,
				StatusFn:   s.Status,
				Config:     s.Config,
				Component:  m,
			},
			topo: m.Topology,
		})
	}
	return ins
}

type DBMSWorkerComponent struct {
	Topology *Topology
}

func (m *DBMSWorkerComponent) ComponentName() string {
	return ComponentDBMSWorker
}

func (m *DBMSWorkerComponent) ComponentRole() string {
	return ComponentDBMSWorker
}

func (m *DBMSWorkerComponent) Instances() []Instance {
	ins := make([]Instance, 0)
	for _, s := range m.Topology.WorkerServers {
		ins = append(ins, &WorkerInstance{
			BaseInstance: BaseInstance{
				InstanceTopology: s,
				Host:             s.Host,
				ManageHost:       s.ManageHost,
				// ListenHost: m.Topology.GlobalOptions.ListenHost, // TODO: current isnot support
				ListenHost: s.Host,
				Port:       s.Port,
				SSHPort:    s.SSHPort,
				NumaNode:   s.InstanceNumaNode,
				DeployDir:  s.DeployDir,
				LogDir:     s.LogDir,
				Patched:    s.Patched,
				OSVersion:  s.OS,
				OSArch:     s.Arch,
				StatusFn:   s.Status,
				Config:     s.Config,
				Component:  m,
			},
			topo: m.Topology,
		})
	}
	return ins
}

// MasterInstance represent the dbms-master instance
type MasterInstance struct {
	BaseInstance
	topo *Topology
}

// InstanceInitConfig implements Instance interface
func (m *MasterInstance) InstanceInitConfig(ctx context.Context, e executor.Executor, deployUser string, cacheDir string) error {
	err := m.BaseInstance.InitSystemdConfig(ctx, e, m.topo.GlobalOptions, deployUser, cacheDir)
	if err != nil {
		return err
	}

	var initialCluster []string
	for _, ms := range m.topo.MasterServers {
		initialCluster = append(initialCluster, stringutil.JoinHostPort(ms.Host, ms.PeerPort))
	}

	cfg := &script.DBMSMasterScript{
		ClientAddr:       stringutil.JoinHostPort(m.InstanceListenHost(), m.InstancePort()),
		PeerAddr:         stringutil.JoinHostPort(m.InstanceListenHost(), m.InstancePeerPort()),
		InitialCluster:   stringutil.StringJoin(initialCluster, ","),
		OS:               stringutil.StringLower(m.OSVersion),
		InstantClientDir: filepath.Join(m.topo.GlobalOptions.DeployDir, InstantClientDir),
		DeployDir:        m.DeployDir,
		DataDir:          m.DataDir,
		LogDir:           m.LogDir,
		InstanceNumaNode: m.NumaNode,
	}

	fp := filepath.Join(cacheDir, fmt.Sprintf("run_dbms-master_%s_%d.sh", m.InstanceHost(), m.InstancePort()))
	if err = cfg.ConfigToFile(fp); err != nil {
		return err
	}

	dst := filepath.Join(m.DeployDir, ScriptDirName, "run_dbms-master.sh")
	if err := e.Transfer(ctx, fp, dst, false, 0); err != nil {
		return err
	}
	_, _, err = e.Execute(ctx, "chmod +x "+dst, false)
	if err != nil {
		return err
	}

	return m.MergeServerConfig(ctx, e, m.topo.ServerConfigs.Master, m.GetInstanceConfig(), cacheDir)

}

// InstanceScaleConfig implements Instance interface
func (m *MasterInstance) InstanceScaleConfig(ctx context.Context, e executor.Executor, topo *Topology, deployUser, cacheDir string) error {
	// global options from exist topo file
	if err := m.BaseInstance.InitSystemdConfig(ctx, e, topo.GlobalOptions, deployUser, cacheDir); err != nil {
		return err
	}

	var masters []string
	// get master list from exist topo file
	for _, masterspec := range topo.MasterServers {
		masters = append(masters, stringutil.JoinHostPort(masterspec.Host, masterspec.Port))
	}
	cfg := &script.DBMSMasterScaleScript{
		ClientAddr:       stringutil.JoinHostPort(m.InstanceListenHost(), m.InstancePort()),
		PeerAddr:         stringutil.JoinHostPort(m.InstanceListenHost(), m.InstancePeerPort()),
		OS:               stringutil.StringLower(m.OSVersion),
		InstantClientDir: filepath.Join(m.topo.GlobalOptions.DeployDir, InstantClientDir),
		DeployDir:        m.DeployDir,
		DataDir:          m.DataDir,
		LogDir:           m.LogDir,
		InstanceNumaNode: m.NumaNode,
		Join:             strings.Join(masters, ","),
	}

	fp := filepath.Join(cacheDir, fmt.Sprintf("run_dbms-master_%s_%d.sh", m.InstanceHost(), m.InstancePort()))
	if err := cfg.ConfigToFile(fp); err != nil {
		return err
	}

	dst := filepath.Join(m.DeployDir, ScriptDirName, "run_dbms-master.sh")
	if err := e.Transfer(ctx, fp, dst, false, 0); err != nil {
		return err
	}
	if _, _, err := e.Execute(ctx, "chmod +x "+dst, false); err != nil {
		return err
	}
	return m.MergeServerConfig(ctx, e, topo.ServerConfigs.Master, m.GetInstanceConfig(), cacheDir)
}

// WorkerInstance represent the dbms-worker instance
type WorkerInstance struct {
	BaseInstance
	topo *Topology
}

// InstanceInitConfig implements Instance interface
func (w *WorkerInstance) InstanceInitConfig(ctx context.Context, e executor.Executor, deployUser string, cacheDir string) error {
	err := w.BaseInstance.InitSystemdConfig(ctx, e, w.topo.GlobalOptions, deployUser, cacheDir)
	if err != nil {
		return err
	}

	var masters []string
	for _, masterspec := range w.topo.MasterServers {
		masters = append(masters, stringutil.JoinHostPort(masterspec.Host, masterspec.Port))
	}

	cfg := &script.DBMSWorkerScript{
		WorkerAddr:       stringutil.JoinHostPort(w.InstanceListenHost(), w.InstancePort()),
		Join:             stringutil.StringJoin(masters, ","),
		OS:               stringutil.StringLower(w.OSVersion),
		InstantClientDir: filepath.Join(w.topo.GlobalOptions.DeployDir, InstantClientDir),
		DeployDir:        w.DeployDir,
		LogDir:           w.LogDir,
		InstanceNumaNode: w.NumaNode,
	}

	fp := filepath.Join(cacheDir, fmt.Sprintf("run_dbms-worker_%s_%d.sh", w.InstanceHost(), w.InstancePort()))
	if err = cfg.ConfigToFile(fp); err != nil {
		return err
	}
	dst := filepath.Join(w.DeployDir, ScriptDirName, "run_dbms-worker.sh")

	if err := e.Transfer(ctx, fp, dst, false, 0); err != nil {
		return err
	}

	_, _, err = e.Execute(ctx, "chmod +x "+dst, false)
	if err != nil {
		return err
	}

	return w.MergeServerConfig(ctx, e, w.topo.ServerConfigs.Worker, w.GetInstanceConfig(), cacheDir)
}

// InstanceScaleConfig implements Instance interface
func (w *WorkerInstance) InstanceScaleConfig(ctx context.Context, e executor.Executor, topo *Topology, deployUser, cacheDir string) error {
	// global options from exist topo file
	err := w.BaseInstance.InitSystemdConfig(ctx, e, topo.GlobalOptions, deployUser, cacheDir)
	if err != nil {
		return err
	}

	var masters []string
	for _, masterspec := range topo.MasterServers {
		masters = append(masters, stringutil.JoinHostPort(masterspec.Host, masterspec.Port))
	}

	cfg := &script.DBMSWorkerScript{
		WorkerAddr:       stringutil.JoinHostPort(w.InstanceListenHost(), w.InstancePort()),
		Join:             stringutil.StringJoin(masters, ","),
		OS:               stringutil.StringLower(w.OSVersion),
		InstantClientDir: filepath.Join(w.topo.GlobalOptions.DeployDir, InstantClientDir),
		DeployDir:        w.DeployDir,
		LogDir:           w.LogDir,
		InstanceNumaNode: w.NumaNode,
	}

	fp := filepath.Join(cacheDir, fmt.Sprintf("run_dbms-worker_%s_%d.sh", w.InstanceHost(), w.InstancePort()))
	if err := cfg.ConfigToFile(fp); err != nil {
		return err
	}
	dst := filepath.Join(w.DeployDir, ScriptDirName, "run_dbms-worker.sh")

	if err := e.Transfer(ctx, fp, dst, false, 0); err != nil {
		return err
	}

	_, _, err = e.Execute(ctx, "chmod +x "+dst, false)
	if err != nil {
		return err
	}
	return w.MergeServerConfig(ctx, e, topo.ServerConfigs.Worker, w.GetInstanceConfig(), cacheDir)
}

/*
	implements Instance interface
*/

// BaseInstance implements some method of Instance interface
type BaseInstance struct {
	InstanceTopology
	Host       string
	ManageHost string
	ListenHost string
	Port       int
	SSHPort    int
	PeerPort   int
	NumaNode   string

	Patched   bool
	DeployDir string
	DataDir   string
	LogDir    string
	OSVersion string
	OSArch    string

	StatusFn func(ctx context.Context, tlsCfg *tls.Config, masterAddrs ...string) (string, error)

	Config    map[string]any
	Component Component
}

// InstanceName implement Instance interface
func (b *BaseInstance) InstanceName() string {
	return stringutil.JoinHostPort(b.Host, b.Port)
}

// InstanceManageHost implement Instance interface
func (b *BaseInstance) InstanceManageHost() string {
	if b.ManageHost != "" {
		return b.ManageHost
	}
	return b.Host
}

// InstanceHost implement Instance interface
func (b *BaseInstance) InstanceHost() string {
	return b.Host
}

// InstancePort implement Instance interface
func (b *BaseInstance) InstancePort() int {
	return b.Port
}

// InstancePeerPort implement Instance interface
func (b *BaseInstance) InstancePeerPort() int {
	return b.PeerPort
}

// InstanceSshPort implement Instance interface
func (b *BaseInstance) InstanceSshPort() int {
	return b.SSHPort
}

// InstanceDeployDir implement Instance interface
func (b *BaseInstance) InstanceDeployDir() string {
	return b.DeployDir
}

// InstanceDataDir implement Instance interface
func (b *BaseInstance) InstanceDataDir() string {
	return b.DataDir
}

// InstanceLogDir implement Instance interface
func (b *BaseInstance) InstanceLogDir() string {
	return b.LogDir
}

// InstanceListenHost implement Instance interface
func (b *BaseInstance) InstanceListenHost() string {
	if b.ListenHost == "" {
		// ipv6 address
		if strings.Contains(b.Host, ":") {
			return "::"
		}
		return "0.0.0.0"
	}
	return b.ListenHost
}

// InstanceNumaNode implement Instance interface
func (b *BaseInstance) InstanceNumaNode() string {
	return b.NumaNode
}

// OS implement Instance interface
func (b *BaseInstance) OS() string {
	return b.OSVersion
}

// Arch implement Instance interface
func (b *BaseInstance) Arch() string {
	return b.OSArch
}

// UsedPort implement Instance interface
func (b *BaseInstance) UsedPort() []int {
	if b.ComponentRole() == ComponentDBMSWorker {
		return append([]int{}, b.Port)
	}
	return append(append([]int{}, b.Port), b.PeerPort)
}

// UsedDir implement Instance interface
func (b *BaseInstance) UsedDir() []string {
	if b.ComponentRole() == ComponentDBMSWorker {
		return append(append([]string{}, b.DeployDir), b.LogDir)
	}
	return append(append(append([]string{}, b.DeployDir), b.DataDir), b.LogDir)
}

// ComponentName implement Instance interface
func (b *BaseInstance) ComponentName() string {
	return b.Component.ComponentName()
}

// ComponentRole implement Instance interface
func (b *BaseInstance) ComponentRole() string {
	return b.Component.ComponentRole()
}

// GetInstanceConfig implement Instance interface
func (b *BaseInstance) GetInstanceConfig() map[string]any {
	return b.Config
}

// ServiceName implement Instance interface
func (b *BaseInstance) ServiceName() string {
	if strings.EqualFold(b.OSVersion, "darwin") {
		return fmt.Sprintf("%s-%d.plist", b.Component.ComponentName(), b.Port)
	}
	return fmt.Sprintf("%s-%d.service", b.Component.ComponentName(), b.Port)
}

// ServiceReady implement Instance interface
func (b *BaseInstance) ServiceReady(ctx context.Context, e executor.Executor, timeout uint64) error {
	return PortStarted(ctx, e, b.OSVersion, b.Port, timeout)
}

// Status implement Instance interface
func (b *BaseInstance) Status(ctx context.Context, tlsCfg *tls.Config, addrs ...string) (string, error) {
	return b.StatusFn(ctx, tlsCfg, addrs...)
}

// IsPatched implement Instance interface
func (b *BaseInstance) IsPatched() bool {
	return b.Patched
}

// SetPatched implement Instance interface
func (b *BaseInstance) SetPatched(patch bool) {
	b.Patched = patch
	v := reflect.Indirect(reflect.ValueOf(b.InstanceTopology)).FieldByName("Patched")
	if !v.CanSet() {
		return
	}
	v.SetBool(patch)
}

// InitSystemdConfig init the service configuration.
func (b *BaseInstance) InitSystemdConfig(ctx context.Context, e executor.Executor, opt *GlobalOptions, user, cacheDir string) error {
	compName := b.Component.ComponentName()
	instHost := b.InstanceHost()
	instPort := b.InstancePort()

	if strings.EqualFold(b.OS(), "darwin") {
		serviceCfgName := filepath.Join(cacheDir, fmt.Sprintf("%s-%s-%d.plist", compName, instHost, instPort))

		systemMode := opt.SystemdMode
		if len(systemMode) == 0 {
			systemMode = SystemMode
		}

		if err := launchd.NewLaunchdConfig(stringutil.StringBuilder(compName, "-", strconv.Itoa(instPort)), compName, user, b.InstanceDeployDir()).ConfigToFile(serviceCfgName); err != nil {
			return err
		}

		tgt := filepath.Join("/tmp", compName+"_"+uuid.New().String()+".plist")
		if err := e.Transfer(ctx, serviceCfgName, tgt, false, 0); err != nil {
			return errors.Annotatef(err, "transfer from %s to %s failed", serviceCfgName, tgt)
		}
		systemdDir := "/Library/LaunchAgents/"
		sudo := true
		if opt.SystemdMode == UserMode {
			systemdDir = "~/Library/LaunchAgents"
			sudo = false
		}
		var cmd string
		if sudo {
			plistFile := fmt.Sprintf("%s%s-%d.plist", systemdDir, compName, instPort)
			cmd = fmt.Sprintf("mv %s %s && chown root %s", tgt, plistFile, plistFile)
		} else {
			cmd = fmt.Sprintf("mv %s %s%s-%d.plist", tgt, systemdDir, compName, instPort)
		}
		if _, _, err := e.Execute(ctx, cmd, sudo); err != nil {
			return errors.Annotatef(err, "execute: %s", cmd)
		}

		return nil
	}

	serviceCfgName := filepath.Join(cacheDir, fmt.Sprintf("%s-%s-%d.service", compName, instHost, instPort))

	systemdMode := opt.SystemdMode
	if len(systemdMode) == 0 {
		systemdMode = SystemMode
	}

	if err := systemd.NewSystemdConfig(compName, user, b.InstanceDeployDir()).
		WithSystemdMode(string(systemdMode)).
		ConfigToFile(serviceCfgName); err != nil {
		return err
	}

	tgt := filepath.Join("/tmp", compName+"_"+uuid.New().String()+".service")
	if err := e.Transfer(ctx, serviceCfgName, tgt, false, 0); err != nil {
		return errors.Annotatef(err, "transfer from %s to %s failed", serviceCfgName, tgt)
	}
	systemdDir := "/etc/systemd/system/"
	sudo := true
	if opt.SystemdMode == UserMode {
		systemdDir = "~/.config/systemd/user/"
		sudo = false
	}
	cmd := fmt.Sprintf("mv %s %s%s-%d.service", tgt, systemdDir, compName, instPort)
	if _, _, err := e.Execute(ctx, cmd, sudo); err != nil {
		return errors.Annotatef(err, "execute: %s", cmd)
	}
	return nil
}

// MergeServerConfig merges the server configuration and overwrite the global configuration
func (b *BaseInstance) MergeServerConfig(ctx context.Context, e executor.Executor, globalCfg, instanceCfg map[string]any, cacheDir string) error {
	fp := filepath.Join(cacheDir, fmt.Sprintf("%s-%s-%d.toml", b.Component.ComponentName(), b.InstanceHost(), b.InstancePort()))
	cfg, err := stringutil.Merge2TomlConfig(b.Component.ComponentName(), globalCfg, instanceCfg)
	if err != nil {
		return err
	}
	err = stringutil.WriteFile(fp, cfg, os.ModePerm)
	if err != nil {
		return err
	}

	dst := filepath.Join(b.DeployDir, ConfDirName, fmt.Sprintf("%s.toml", b.Component.ComponentName()))
	// transfer config
	return e.Transfer(ctx, fp, dst, false, 0)
}
