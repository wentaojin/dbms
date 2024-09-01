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
	clientv3 "go.etcd.io/etcd/client/v3"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/utils/etcdutil"
)

// SystemdMode is the mode used by systemctl
type SystemdMode string

const (
	// SystemMode system mode
	SystemMode SystemdMode = "system"
	// UserMode user mode
	UserMode SystemdMode = "user"
)

// FullHostType is the type of fullhost operations
type FullHostType string

const (
	// FullArchType cpu-arch type
	FullArchType FullHostType = "Arch"
	// FullOSType kernel-name
	FullOSType FullHostType = "OS"
)

type Topology struct {
	GlobalOptions *GlobalOptions   `yaml:"global,omitempty" validate:"global:editable"`
	ServerConfigs *ServerConfig    `yaml:"server_configs,omitempty" validate:"server_configs:ignore"`
	MasterServers []*MasterOptions `yaml:"master_servers"`
	WorkerServers []*WorkerOptions `yaml:"worker_servers"`
}

type ServerConfig struct {
	Master map[string]any `yaml:"master"`
	Worker map[string]any `yaml:"worker"`
}

type GlobalOptions struct {
	User        string      `yaml:"user,omitempty" default:"dbms"`
	Group       string      `yaml:"group,omitempty"`
	SSHPort     int         `yaml:"ssh_port,omitempty" default:"22" validate:"ssh_port:editable"`
	SSHType     string      `yaml:"ssh_type,omitempty" default:"builtin"`
	ListenHost  string      `yaml:"listen_host,omitempty" validate:"listen_host:editable"`
	DeployDir   string      `yaml:"deploy_dir,omitempty" default:"deploy"`
	DataDir     string      `yaml:"data_dir,omitempty" default:"data"`
	LogDir      string      `yaml:"log_dir,omitempty"`
	OS          string      `yaml:"os,omitempty" default:"linux"`
	Arch        string      `yaml:"arch,omitempty"`
	SystemdMode SystemdMode `yaml:"systemd_mode,omitempty" default:"system"`
}

// MasterOptions represents the Master topology specification in topology.yaml
type MasterOptions struct {
	Host             string         `yaml:"host"`
	ManageHost       string         `yaml:"manage_host,omitempty" validate:"manage_host:editable"`
	SSHPort          int            `yaml:"ssh_port,omitempty" validate:"ssh_port:editable"`
	Patched          bool           `yaml:"patched,omitempty"`
	Port             int            `yaml:"port,omitempty" default:"2379"`
	PeerPort         int            `yaml:"peer_port,omitempty" default:"2380"`
	DeployDir        string         `yaml:"deploy_dir,omitempty"`
	DataDir          string         `yaml:"data_dir,omitempty"`
	LogDir           string         `yaml:"log_dir,omitempty"`
	InstanceNumaNode string         `yaml:"numa_node,omitempty" validate:"numa_node:editable"`
	Config           map[string]any `yaml:"config,omitempty" validate:"config:ignore"`
	Arch             string         `yaml:"arch,omitempty"`
	OS               string         `yaml:"os,omitempty"`
}

func (s *MasterOptions) InstanceRole() string {
	return ComponentDBMSMaster
}

func (s *MasterOptions) InstanceName() string {
	return stringutil.JoinHostPort(s.Host, s.Port)
}

// Status queries current status of the instance
func (s *MasterOptions) Status(ctx context.Context, tlsCfg *tls.Config, addrs ...string) (string, error) {
	if len(addrs) < 1 {
		return "N/A", fmt.Errorf("the cluster master instance cannot be zero, please contact author or reselect")
	}
	cli, err := etcdutil.CreateClient(ctx, addrs, tlsCfg)
	if err != nil {
		return "N/A", err
	}

	keyResp, err := etcdutil.GetKey(cli, stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, fmt.Sprintf("%s:%d", s.Host, s.Port)))
	if err != nil {
		return "N/A", err
	}
	if len(keyResp.Kvs) == 0 {
		return "DOWN", nil
	} else if len(keyResp.Kvs) > 1 {
		return "N/A", fmt.Errorf("the cluster master member [%s] are over than one, please contact author or reselect", fmt.Sprintf("%s:%d", s.Host, s.Port))
	}

	leaderResp, err := etcdutil.GetKey(cli, etcdutil.DefaultMasterLeaderPrefixKey, clientv3.WithPrefix())
	if err != nil {
		return "N/A", err
	}
	if len(leaderResp.Kvs) == 0 {
		return "N/A", nil
	} else if len(leaderResp.Kvs) > 1 {
		return "N/A", fmt.Errorf("the cluster leader are over than one, please contact author or reselect, detail: %v", leaderResp.Kvs)
	}

	if strings.EqualFold(stringutil.BytesToString(leaderResp.Kvs[0].Value), fmt.Sprintf("%s:%d", s.Host, s.Port)) {
		return "Healthy|L", nil
	} else {
		return "Healthy", nil
	}
}

// WorkerOptions represents the Master topology specification in topology.yaml
type WorkerOptions struct {
	Host             string         `yaml:"host"`
	ManageHost       string         `yaml:"manage_host,omitempty" validate:"manage_host:editable"`
	SSHPort          int            `yaml:"ssh_port,omitempty" validate:"ssh_port:editable"`
	Patched          bool           `yaml:"patched,omitempty"`
	Port             int            `yaml:"port,omitempty" default:"8262"`
	DeployDir        string         `yaml:"deploy_dir,omitempty"`
	LogDir           string         `yaml:"log_dir,omitempty"`
	InstanceNumaNode string         `yaml:"numa_node,omitempty" validate:"numa_node:editable"`
	Config           map[string]any `yaml:"config,omitempty" validate:"config:ignore"`
	Arch             string         `yaml:"arch,omitempty"`
	OS               string         `yaml:"os,omitempty"`
}

func (w *WorkerOptions) InstanceRole() string {
	return ComponentDBMSWorker
}

func (w *WorkerOptions) InstanceName() string {
	return stringutil.JoinHostPort(w.Host, w.Port)
}

// Status queries current status of the instance
func (w *WorkerOptions) Status(ctx context.Context, tlsCfg *tls.Config, addrs ...string) (string, error) {
	if len(addrs) < 1 {
		return "N/A", fmt.Errorf("the cluster master instance cannot be zero, please contact author or reselect")
	}
	cli, err := etcdutil.CreateClient(ctx, addrs, tlsCfg)
	if err != nil {
		return "N/A", err
	}

	keyResp, err := etcdutil.GetKey(cli, stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, fmt.Sprintf("%s:%d", w.Host, w.Port)))
	if err != nil {
		return "N/A", err
	}
	if len(keyResp.Kvs) == 0 {
		return "DOWN", nil
	} else if len(keyResp.Kvs) > 1 {
		return "N/A", fmt.Errorf("the cluster worker member [%s] are over than one, please contact author or reselect", fmt.Sprintf("%s:%d", w.Host, w.Port))
	}

	stateResp, err := etcdutil.GetKey(cli, stringutil.StringBuilder(stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, fmt.Sprintf("%s:%d", w.Host, w.Port))))
	if err != nil {
		return "N/A", err
	}
	if len(stateResp.Kvs) == 0 {
		return "FREE", nil
	} else if len(stateResp.Kvs) > 1 {
		return "N/A", fmt.Errorf("the cluster worker state [%s] are over than one, please contact author or reselect", fmt.Sprintf("%s:%d", w.Host, w.Port))
	}

	var ws *etcdutil.Instance
	err = stringutil.UnmarshalJSON(stateResp.Kvs[0].Value, &ws)
	if err != nil {
		return "", err
	}
	if strings.EqualFold(ws.State, "") {
		return "FREE", nil
	}
	return ws.State, nil
}

// IterInstance iterates all instances in component starting order
func (t *Topology) IterInstance(fn func(inst Instance), concurrency ...int) {
	maxWorkers := 1
	wg := sync.WaitGroup{}
	if len(concurrency) > 0 && concurrency[0] > 1 {
		maxWorkers = concurrency[0]
	}
	workerPool := make(chan struct{}, maxWorkers)

	for _, comp := range t.ComponentsByStartOrder() {
		for _, inst := range comp.Instances() {
			wg.Add(1)
			workerPool <- struct{}{}
			go func(inst Instance) {
				defer func() {
					<-workerPool
					wg.Done()
				}()
				fn(inst)
			}(inst)
		}
	}
	wg.Wait()
}

// IterComponent iterates all components in component starting order
func (t *Topology) IterComponent(fn func(c Component)) {
	for _, comp := range t.ComponentsByStartOrder() {
		fn(comp)
	}
}

// ComponentsByStartOrder return component in the order need to start.
func (t *Topology) ComponentsByStartOrder() (comps []Component) {
	// "dbms-master", "dbms-worker"
	comps = append(comps, &DBMSMasterComponent{Topology: t})
	comps = append(comps, &DBMSWorkerComponent{Topology: t})
	return
}

// ComponentsByStopOrder return component in the order need to stop.
func (t *Topology) ComponentsByStopOrder() (comps []Component) {
	comps = t.ComponentsByStartOrder()
	// revert order
	i := 0
	j := len(comps) - 1
	for i < j {
		comps[i], comps[j] = comps[j], comps[i]
		i++
		j--
	}
	return
}

// ComponentsByUpdateOrder return component in the order need to be updated.
func (t *Topology) ComponentsByUpdateOrder() (comps []Component) {
	// "dm-master", "dm-worker"
	comps = append(comps, &DBMSMasterComponent{t})
	comps = append(comps, &DBMSWorkerComponent{t})
	return
}

// FillHostArchOrOS fills the topology with the given host->arch
func (t *Topology) FillHostArchOrOS(hostArch map[string]string, fullType FullHostType) error {
	if err := FillHostArchOrOS(t, hostArch, fullType); err != nil {
		return err
	}
	return nil
}

// CountDir counts for dir paths used by any instance in the cluster with the same
// prefix, useful to find potential path conflicts
func (t *Topology) CountDir(targetHost, dirPrefix string) int {
	dirTypes := []string{
		"DataDir",
		"DeployDir",
		"LogDir",
	}

	// host-path -> count
	dirStats := make(map[string]int)
	count := 0
	topoSpec := reflect.ValueOf(t).Elem()
	dirPrefix = stringutil.Abs(t.GlobalOptions.User, dirPrefix)

	for i := 0; i < topoSpec.NumField(); i++ {
		if isSkipField(topoSpec.Field(i)) {
			continue
		}

		compSpecs := topoSpec.Field(i)
		for index := 0; index < compSpecs.Len(); index++ {
			compSpec := reflect.Indirect(compSpecs.Index(index))
			// Directory conflicts
			for _, dirType := range dirTypes {
				if j, found := findField(compSpec, dirType); found {
					dir := compSpec.Field(j).String()
					host := compSpec.FieldByName("Host").String()

					switch dirType { // the same as in logic.go for (*instance)
					case "DataDir":
						deployDir := compSpec.FieldByName("DeployDir").String()
						// the default data_dir is relative to deploy_dir
						if dir != "" && !strings.HasPrefix(dir, "/") {
							dir = filepath.Join(deployDir, dir)
						}
					case "LogDir":
						deployDir := compSpec.FieldByName("DeployDir").String()
						field := compSpec.FieldByName("LogDir")
						if field.IsValid() {
							dir = field.Interface().(string)
						}

						if dir == "" {
							dir = "log"
						}
						if !strings.HasPrefix(dir, "/") {
							dir = filepath.Join(deployDir, dir)
						}
					}
					dir = stringutil.Abs(t.GlobalOptions.User, dir)
					dirStats[host+dir]++
				}
			}
		}
	}

	for k, v := range dirStats {
		if k == targetHost+dirPrefix || strings.HasPrefix(k, targetHost+dirPrefix+"/") {
			count += v
		}
	}

	return count
}

// FillHostArchOrOS fills the topology with the given host->arch
func FillHostArchOrOS(s any, hostArchOrOS map[string]string, fullType FullHostType) error {
	for host, arch := range hostArchOrOS {
		switch arch {
		case "x86_64":
			hostArchOrOS[host] = "amd64"
		case "aarch64":
			hostArchOrOS[host] = "arm64"
		default:
			hostArchOrOS[host] = strings.ToLower(arch)
		}
	}

	v := reflect.ValueOf(s).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := v.Field(i)
		if field.Kind() != reflect.Slice {
			continue
		}
		for j := 0; j < field.Len(); j++ {
			if err := setHostArchOrOS(field.Index(j), hostArchOrOS, fullType); err != nil {
				return err
			}
		}
	}
	return nil
}

func setHostArchOrOS(field reflect.Value, hostArchOrOS map[string]string, fullType FullHostType) error {
	if !field.CanSet() || isSkipField(field) {
		return nil
	}

	if field.Kind() == reflect.Ptr {
		return setHostArchOrOS(field.Elem(), hostArchOrOS, fullType)
	}

	if field.Kind() != reflect.Struct {
		return nil
	}

	host := field.FieldByName("Host")

	arch := field.FieldByName("Arch")
	os := field.FieldByName("OS")

	// set arch only if not set before
	if fullType == FullOSType {
		if !host.IsZero() && os.CanSet() && len(os.String()) == 0 {
			os.Set(reflect.ValueOf(hostArchOrOS[host.String()]))
		}
	} else {
		if !host.IsZero() && arch.CanSet() && len(arch.String()) == 0 {
			arch.Set(reflect.ValueOf(hostArchOrOS[host.String()]))
		}
	}

	return nil
}

func (t *Topology) String() string {
	jsonStr, _ := stringutil.MarshalIndentJSON(t)
	return jsonStr
}

// Skip global/monitored/job options
func isSkipField(field reflect.Value) bool {
	if field.Kind() == reflect.Ptr {
		if field.IsZero() {
			return true
		}
		field = field.Elem()
	}
	tp := field.Type().Name()
	return tp == "GlobalOptions" || tp == "ServerConfig"
}

func findField(v reflect.Value, fieldName string) (int, bool) {
	for i := 0; i < v.NumField(); i++ {
		if v.Type().Field(i).Name == fieldName {
			return i, true
		}
	}
	return -1, false
}
