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
package manager

import (
	"context"
	"fmt"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"

	"github.com/wentaojin/dbms/utils/cluster"
	"github.com/wentaojin/dbms/utils/cluster/operator"
	"github.com/wentaojin/dbms/utils/ctxt"
	"github.com/wentaojin/dbms/utils/executor"
	"github.com/wentaojin/dbms/utils/stringutil"
)

// DisplayOption represents option of display command
type DisplayOption struct {
	ClusterName    string
	ShowUptime     bool
	ShowManageHost bool
	ShowNuma       bool
	ShowDataDir    bool
}

// InstInfo represents an instance info
type InstInfo struct {
	ID            string `json:"id"`
	Role          string `json:"role"`
	Host          string `json:"host"`
	ManageHost    string `json:"manage_host"`
	Ports         string `json:"ports"`
	OsArch        string `json:"os_arch"`
	Status        string `json:"status"`
	DataDir       string `json:"data_dir"`
	DeployDir     string `json:"deploy_dir"`
	NumaNode      string `json:"numa_node"`
	NumaCores     string `json:"numa_cores"`
	ComponentName string `json:"component_name"`
	Port          int    `json:"port"`
	Version       string `json:"version"`
	Since         string `json:"since"`
}

// ClusterMetaInfo hold the structure for the JSON output of the dashboard info
type ClusterMetaInfo struct {
	ClusterType    string `json:"cluster_type"`
	ClusterName    string `json:"cluster_name"`
	ClusterVersion string `json:"cluster_version"`
	DeployUser     string `json:"deploy_user"`
	SSHType        string `json:"ssh_type"`
}

type JSONOutput struct {
	ClusterMetaInfo ClusterMetaInfo `json:"cluster_meta"`
	InstanceInfos   []InstInfo      `json:"instances,omitempty"`
}

func (c *Controller) GetClusterTopology(dopt *DisplayOption, opt *operator.Options) ([]InstInfo, map[string]string, error) {
	workerTaskRefrence := make(map[string]string)

	ctx := ctxt.New(
		context.Background(),
		opt.Concurrency,
		c.Logger,
	)

	topo := c.Meta.GetTopology()

	err := SetSSHKeySet(ctx, c.Path(dopt.ClusterName, "ssh", "id_rsa"), c.Path(dopt.ClusterName, "ssh", "id_rsa.pub"))
	if err != nil {
		return nil, nil, err
	}

	err = SetClusterSSH(ctx, topo, c.Meta.GetUser(), opt.SSHTimeout, opt.SSHType, executor.SSHType(topo.GlobalOptions.SSHType))
	if err != nil {
		return nil, nil, err
	}

	filterRoles := stringutil.NewStringSet(opt.Roles...)
	filterNodes := stringutil.NewStringSet(opt.Nodes...)

	var masterList []string
	for _, master := range topo.MasterServers {
		host := master.Host
		if master.ManageHost != "" {
			host = master.ManageHost
		}
		masterList = append(masterList, stringutil.JoinHostPort(host, master.Port))
	}

	client, err := etcdutil.CreateClient(context.TODO(), masterList, nil)
	if err != nil {
		return nil, nil, err
	}

	keys, err := etcdutil.GetKey(client, constant.DefaultInstanceTaskReferencesPrefixKey, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}

	for _, v := range keys.Kvs {
		keySli := stringutil.StringSplit(stringutil.BytesToString(v.Key), constant.StringSeparatorSlash)
		workerTaskRefrence[stringutil.BytesToString(v.Value)] = keySli[len(keySli)-1]
	}

	masterActive := make([]string, 0)
	masterStatus := make(map[string]string)

	var mu sync.Mutex
	topo.IterInstance(func(ins cluster.Instance) {
		if ins.ComponentName() != cluster.ComponentDBMSMaster {
			return
		}

		status, err := ins.Status(ctx, nil, masterList...)
		if err != nil {
			c.Logger.Errorf("get instance %s status failed: %v", ins.InstanceName(), err)
			masterStatus[ins.InstanceName()] = status
			return
		}
		mu.Lock()
		if strings.HasPrefix(status, "Up") || strings.HasPrefix(status, "Healthy") {
			instAddr := stringutil.JoinHostPort(ins.InstanceManageHost(), ins.InstancePort())
			masterActive = append(masterActive, instAddr)
		}
		masterStatus[ins.InstanceName()] = status
		mu.Unlock()
	}, opt.Concurrency)

	var clusterInstInfos []InstInfo
	systemdMode := string(topo.GlobalOptions.SystemdMode)
	topo.IterInstance(func(ins cluster.Instance) {
		// apply role filter
		if len(filterRoles) > 0 && !filterRoles.Exist(ins.ComponentName()) {
			return
		}
		// apply node filter
		if len(filterNodes) > 0 && !filterNodes.Exist(ins.InstanceName()) {
			return
		}

		dataDir := "-"
		insDirs := ins.UsedDir()
		deployDir := insDirs[0]
		if ins.ComponentRole() == cluster.ComponentDBMSMaster {
			if len(insDirs) > 1 {
				dataDir = insDirs[1]
			}
		}

		var status string
		switch ins.ComponentName() {
		case cluster.ComponentDBMSMaster:
			status = masterStatus[ins.InstanceName()]
		default:
			status, err = ins.Status(ctx, nil, masterActive...)
			if err != nil {
				c.Logger.Errorf("get instance %s status failed: %v", ins.InstanceName(), err)
			}
		}

		since := "-"
		// Query the service status and uptime
		if status == "-" || dopt.ShowUptime {
			e, found := ctxt.GetInner(ctx).GetExecutor(ins.InstanceManageHost())
			if found {
				var active string
				var systemdSince time.Duration
				active, _, systemdSince, _ = operator.GetServiceStatus(ctx, e, ins.ServiceName(), systemdMode, systemdMode)
				if status == "-" {
					if active == "active" {
						status = "Up"
					} else {
						status = active
					}
				}
				if dopt.ShowUptime {
					since = formatInstanceSince(systemdSince)
				}
			}
		}

		// check if the role is patched
		roleName := ins.ComponentRole()
		if ins.IsPatched() {
			roleName += " (patched)"
		}
		mu.Lock()
		clusterInstInfos = append(clusterInstInfos, InstInfo{
			ID:            ins.InstanceName(),
			Role:          roleName,
			Host:          ins.InstanceHost(),
			ManageHost:    ins.InstanceManageHost(),
			Ports:         stringutil.JoinInt(ins.UsedPort(), "/"),
			OsArch:        stringutil.OsArch(ins.OS(), ins.Arch()),
			Status:        status,
			DataDir:       dataDir,
			DeployDir:     deployDir,
			ComponentName: ins.ComponentName(),
			Port:          ins.InstancePort(),
			Since:         since,
			NumaNode:      stringutil.Ternary(ins.InstanceNumaNode() == "", "-", ins.InstanceNumaNode()).(string),
			Version:       c.Meta.GetVersion(),
		})
		mu.Unlock()
	}, opt.Concurrency)

	// Sort by role,host,ports
	sort.Slice(clusterInstInfos, func(i, j int) bool {
		lhs, rhs := clusterInstInfos[i], clusterInstInfos[j]
		if lhs.Role != rhs.Role {
			return lhs.Role < rhs.Role
		}
		if lhs.Host != rhs.Host {
			return lhs.Host < rhs.Host
		}
		return lhs.Ports < rhs.Ports
	})

	return clusterInstInfos, workerTaskRefrence, nil

}

// SetSSHKeySet set ssh key set.
func SetSSHKeySet(ctx context.Context, privateKeyPath string, publicKeyPath string) error {
	ctxt.GetInner(ctx).PrivateKeyPath = privateKeyPath
	ctxt.GetInner(ctx).PublicKeyPath = publicKeyPath
	return nil
}

// SetClusterSSH set cluster user ssh executor in context.
func SetClusterSSH(ctx context.Context, topo *cluster.Topology, deployUser string, sshTimeout uint64, sshType, defaultSSHType executor.SSHType) error {
	if sshType == "" {
		sshType = defaultSSHType
	}
	if len(ctxt.GetInner(ctx).PrivateKeyPath) == 0 {
		return fmt.Errorf("context has no PrivateKeyPath")
	}

	for _, com := range topo.ComponentsByStartOrder() {
		for _, in := range com.Instances() {
			cf := executor.SSHConfig{
				Host:           in.InstanceManageHost(),
				Port:           in.InstanceSshPort(),
				KeyFile:        ctxt.GetInner(ctx).PrivateKeyPath,
				User:           deployUser,
				ExecuteTimeout: time.Second * time.Duration(sshTimeout),
			}

			e, err := executor.New(sshType, false, cf)
			if err != nil {
				return err
			}
			ctxt.GetInner(ctx).SetExecutor(in.InstanceManageHost(), e)
		}
	}

	return nil
}

func formatInstanceSince(uptime time.Duration) string {
	if uptime == 0 {
		return "-"
	}

	d := int64(uptime.Hours() / 24)
	h := int64(math.Mod(uptime.Hours(), 24))
	m := int64(math.Mod(uptime.Minutes(), 60))
	s := int64(math.Mod(uptime.Seconds(), 60))

	chunks := []struct {
		unit  string
		value int64
	}{
		{"d", d},
		{"h", h},
		{"m", m},
		{"s", s},
	}

	parts := []string{}

	for _, chunk := range chunks {
		switch chunk.value {
		case 0:
			continue
		default:
			parts = append(parts, fmt.Sprintf("%d%s", chunk.value, chunk.unit))
		}
	}

	return strings.Join(parts, "")
}

func FormatInstanceStatus(status string) string {
	lowercaseStatus := strings.ToLower(status)

	startsWith := func(prefixs ...string) bool {
		for _, prefix := range prefixs {
			if strings.HasPrefix(lowercaseStatus, prefix) {
				return true
			}
		}
		return false
	}

	switch {
	case startsWith("up|l", "healthy|l"): // up|l, up|l|ui, healthy|l
		return color.HiGreenString(status)
	case startsWith("up", "healthy", "free", "bound"):
		return color.GreenString(status)
	case startsWith("down", "err", "inactive", "failed"): // down, down|ui
		return color.RedString(status)
	case startsWith("tombstone", "disconnected", "n/a", "stopped"), strings.Contains(strings.ToLower(status), "offline"):
		return color.YellowString(status)
	default:
		return status
	}
}
