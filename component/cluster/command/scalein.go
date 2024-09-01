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
package command

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/wentaojin/dbms/utils/configutil"
	"strings"

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/ctxt"

	"github.com/wentaojin/dbms/utils/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/errors"

	"github.com/wentaojin/dbms/utils/executor"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/utils/cluster"
	"github.com/wentaojin/dbms/utils/cluster/manager"
	"github.com/wentaojin/dbms/utils/cluster/operator"
	"github.com/wentaojin/dbms/utils/cluster/task"
)

type AppScaleIn struct {
	*App
}

func (a *App) AppScaleIn() component.Cmder {
	return &AppScaleIn{App: a}
}

func (a *AppScaleIn) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:              "scale-in <cluster-name>",
		Short:            "Scale in a DBMS cluster",
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			switch len(args) {
			case 0:
				return shellCompGetClusterName(a.MetaDir, toComplete)
			default:
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
		},
	}
	c.Flags().StringSliceVarP(&gOpt.Nodes, "node", "N", nil, "Specify the nodes (required)")
	c.Flags().BoolVar(&gOpt.Force, "force", false, "Force will ignore remote error while destroy the cluster")
	return c
}

func (a *AppScaleIn) RunE(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return cmd.Help()
	}
	clusterName := args[0]

	gOpt.Operation = operator.ScaleInOperation

	return a.ScaleIn(clusterName, gOpt)
}

func (a *AppScaleIn) ScaleIn(clusterName string, gOpt *operator.Options) error {
	if err := cluster.ValidateClusterNameOrError(clusterName); err != nil {
		return err
	}

	mg := manager.New(gOpt.MetaDir, logger)

	// check the scale out file lock is exist
	locked, err := mg.IsScaleOutLocked(clusterName)
	if err != nil {
		return err
	}
	if locked {
		return mg.ScaleOutLockedErr(clusterName)
	}

	// read
	metadata, err := cluster.ParseMetadataYaml(mg.GetMetaFilePath(clusterName))
	if err != nil {
		return err
	}

	mg.SetMetadata(metadata)
	topo := metadata.GetTopology()

	if !gOpt.SkipConfirm {
		cyan := color.New(color.FgCyan, color.Bold)

		fmt.Printf("Cluster type:       %s\n", cyan.Sprint("DBMS"))
		fmt.Printf("Cluster name:       %s\n", cyan.Sprint(clusterName))
		fmt.Printf("Cluster version:    %s\n", cyan.Sprint(metadata.GetVersion()))
		fmt.Printf("Deploy user:        %s\n", cyan.Sprint(metadata.GetUser()))
		fmt.Printf("SSH type:           %s\n", cyan.Sprint(topo.GlobalOptions.SSHType))
		if err = stringutil.PromptForConfirmOrAbortError(
			"This operation will delete the %s nodes in `%s` and all their data.\nDo you want to continue? [y/N]:",
			strings.Join(gOpt.Nodes, ","),
			color.HiYellowString(clusterName)); err != nil {
			return err
		}
	}

	var p *operator.SSHConnectionProps = &operator.SSHConnectionProps{}
	if gOpt.SSHType != executor.SSHTypeNone && len(gOpt.SSHProxyHost) != 0 {
		var err error
		p, err = operator.ReadIdentityFileOrPassword(gOpt.SSHProxyIdentity, gOpt.SSHProxyUsePassword)
		if err != nil {
			return err
		}
	}

	b := task.NewBuilder(mg.Logger).
		SSHKeySet(mg.Path(clusterName, cluster.SshDirName, "id_rsa"), mg.Path(clusterName, cluster.SshDirName, "id_rsa.pub")).
		ClusterSSH(
			topo,
			metadata.User,
			gOpt.SSHTimeout,
			gOpt.OptTimeout,
			gOpt.SSHProxyHost,
			gOpt.SSHProxyPort,
			gOpt.SSHProxyUser,
			p.Password,
			p.IdentityFile,
			p.IdentityFilePassphrase,
			gOpt.SSHProxyTimeout,
			gOpt.SSHType,
			executor.SSHType(topo.GlobalOptions.SSHType),
		).Func(fmt.Sprintf("ScaleInCluster: options=%+v", gOpt), func(ctx context.Context) error {
		return scaleInDBMSCluster(ctx, topo, gOpt, nil)
	}).Serial(task.NewUpdateMetadata(clusterName, a.MetaDir, mg.NewMetadata(), gOpt.Nodes)).Build()

	ctx := ctxt.New(
		context.Background(),
		gOpt.Concurrency,
		mg.Logger,
	)
	if err = b.Execute(ctx); err != nil {
		return err
	}

	// get latest metadata
	metadata, err = cluster.ParseMetadataYaml(mg.GetMetaFilePath(clusterName))
	if err != nil {
		return err
	}

	var (
		refreshConfigTasks []*task.StepDisplay // tasks which are used to refresh config tasks
	)

	latestTopo := metadata.GetTopology()
	globalOptions := latestTopo.GlobalOptions

	deletedNodes := stringutil.NewStringSet(gOpt.Nodes...)
	// Refresh config
	latestTopo.IterInstance(func(inst cluster.Instance) {
		if deletedNodes.Exist(inst.InstanceName()) {
			return
		}
		compName := inst.ComponentName()
		tb := task.NewBuilder(logger).InitConfig(
			clusterName,
			inst,
			globalOptions.User,
			mg.Path(clusterName, cluster.CacheDirName),
		).
			BuildAsStep(fmt.Sprintf("  - Generate config %s -> %s", compName, inst.InstanceName()))
		refreshConfigTasks = append(refreshConfigTasks, tb)
	})

	// clusterssh
	b = task.NewBuilder(mg.Logger).
		SSHKeySet(mg.Path(clusterName, cluster.SshDirName, "id_rsa"), mg.Path(clusterName, cluster.SshDirName, "id_rsa.pub")).
		ClusterSSH(
			topo,
			metadata.User,
			gOpt.SSHTimeout,
			gOpt.OptTimeout,
			gOpt.SSHProxyHost,
			gOpt.SSHProxyPort,
			gOpt.SSHProxyUser,
			p.Password,
			p.IdentityFile,
			p.IdentityFilePassphrase,
			gOpt.SSHProxyTimeout,
			gOpt.SSHType,
			executor.SSHType(topo.GlobalOptions.SSHType),
		).ParallelStep("+ Refresh instance configs", gOpt.Force, refreshConfigTasks...).Build()

	if err = b.Execute(ctx); err != nil {
		return err
	}
	mg.Logger.Infof("Scaled cluster `%s` in successfully", clusterName)
	return nil
}

func scaleInDBMSCluster(ctx context.Context, topo *cluster.Topology, gOpt *operator.Options, tlsCfg *tls.Config) error {
	insts := map[string]cluster.Instance{}
	instCount := map[string]int{}

	// make sure all nodeIds exists in topology
	for _, comp := range topo.ComponentsByStartOrder() {
		for _, inst := range comp.Instances() {
			insts[inst.InstanceName()] = inst
			instCount[inst.InstanceManageHost()]++
		}
	}

	// Clean components
	deletedDiff := map[string][]cluster.Instance{}
	deletedNodes := stringutil.NewStringSet(gOpt.Nodes...)
	for nodeID := range deletedNodes {
		inst, found := insts[nodeID]
		if !found {
			return fmt.Errorf("cannot find node id '%s' in topology", nodeID)
		}
		deletedDiff[inst.ComponentName()] = append(deletedDiff[inst.ComponentName()], inst)
	}

	// Cannot delete all DMMaster servers
	if len(deletedDiff[cluster.ComponentDBMSMaster]) == len(topo.MasterServers) {
		return fmt.Errorf("cannot delete all dbms-master servers")
	}

	// At least a DMMaster server exists
	var etcdCli *clientv3.Client
	var masterEndpoints []string
	for _, instance := range (&cluster.DBMSMasterComponent{Topology: topo}).Instances() {
		if !deletedNodes.Exist(instance.InstanceName()) {
			masterEndpoints = append(masterEndpoints, stringutil.JoinHostPort(instance.InstanceManageHost(), instance.InstancePort()))
		}
	}

	if len(masterEndpoints) == 0 {
		return fmt.Errorf("cannot find available dbms-master instance")
	}

	etcdCli, err := etcdutil.CreateClient(ctx, masterEndpoints, tlsCfg)
	if err != nil {
		return err
	}

	// Delete member from cluster
	for _, comp := range topo.ComponentsByStartOrder() {
		for _, instance := range comp.Instances() {
			if !deletedNodes.Exist(instance.InstanceName()) {
				continue
			}

			if err = operator.StopComponent(
				ctx,
				topo,
				[]cluster.Instance{instance},
				gOpt,
			); err != nil {
				return errors.Annotatef(err, "failed to stop instance %s", instance.InstanceName())
			}

			switch comp.ComponentName() {
			case cluster.ComponentDBMSMaster:
				members, err := etcdutil.ListMembers(etcdCli)
				if err != nil {
					return err
				}

				for _, mem := range members.Members {
					// member name format: master_{ipAddr 10_10_10_21}_{ipPort}
					ipPorts := stringutil.StringSplit(instance.InstanceName(), ":")
					if mem.Name == stringutil.WrapPrefixIPName(ipPorts[0], configutil.DefaultMasterNamePrefix, instance.InstanceName()) {
						_, err = etcdutil.RemoveMember(etcdCli, mem.ID)
						if err != nil {
							return fmt.Errorf("the dbms-cluster can't scale-in dbms-master instance, remove the instance failed: %v", err)
						}
					}
				}

				_, err = etcdutil.DeleteKey(etcdCli, stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, instance.InstanceName()))
				if err != nil {
					return err
				}

			case cluster.ComponentDBMSWorker:
				_, err = etcdutil.DeleteKey(etcdCli, stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, instance.InstanceName()))
				if err != nil {
					return err
				}
			}

			if err := operator.DestroyComponent(ctx, []cluster.Instance{instance}, topo, gOpt); err != nil {
				return errors.Annotatef(err, "failed to destroy instance %s", instance.InstanceName())
			}

			instCount[instance.InstanceManageHost()]--
			if instCount[instance.InstanceManageHost()] == 0 {
				if err := operator.DeletePublicKey(ctx, instance.InstanceManageHost()); err != nil {
					return errors.Annotatef(err, "failed to delete instance %s public key", instance.InstanceName())
				}
			}
		}
	}
	return nil
}
