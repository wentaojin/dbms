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
	"fmt"
	"path/filepath"
	"strings"

	"github.com/wentaojin/dbms/utils/ctxt"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/cluster/operator"
	"github.com/wentaojin/dbms/utils/cluster/task"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/utils/cluster"
	"github.com/wentaojin/dbms/utils/cluster/manager"
	"github.com/wentaojin/dbms/utils/executor"
)

type AppScaleOut struct {
	*App
	User           string // username to login to the SSH server
	SkipCreateUser bool   // don't create the user
	IdentityFile   string // path to the private key file
	UsePassword    bool   // use password instead of identity file for ssh connection
}

func (a *App) AppScaleOut() component.Cmder {
	return &AppScaleOut{App: a}
}

func (a *AppScaleOut) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:              "scale-out <cluster-name> [topology.yaml]",
		Short:            "Scale out a DBMS cluster",
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			switch len(args) {
			case 0:
				return shellCompGetClusterName(a.MetaDir, toComplete)
			case 1:
				return nil, cobra.ShellCompDirectiveDefault
			default:
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
		},
	}
	c.Flags().StringVarP(&a.User, "user", "u", cluster.CurrentUser(), "The user name to login via SSH. The user must has root (or sudo) privilege.")
	c.Flags().BoolVarP(&a.SkipCreateUser, "skip-create-user", "", false, "(EXPERIMENTAL) Skip creating the user specified in topology.")
	c.Flags().StringVarP(&a.IdentityFile, "identity-file", "i", a.IdentityFile, "The path of the SSH identity file. If specified, public key authentication c be used.")
	c.Flags().BoolVarP(&a.UsePassword, "password", "p", false, "Use password of target hosts. If specified, password authentication will be used.")
	return c
}

func (a *AppScaleOut) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(gOpt.MirrorDir, "") {
		return fmt.Errorf("the flag parameters cannot be null, please configure --mirror-dir")
	}
	if len(args) != 2 {
		return cmd.Help()
	}
	clusterName := args[0]
	topoFile := args[1]

	gOpt.Operation = operator.ScaleOutOperation

	return a.ScaleOut(clusterName, topoFile, gOpt)
}

func (a *AppScaleOut) ScaleOut(clusterName, fileName string, gOpt *operator.Options) error {
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
	scaleOutTopo, err := cluster.ParseTopologyYaml(fileName)
	if err != nil {
		return err
	}

	meta := mg.NewMetadata()
	metadata, err := meta.ParseMetadata(mg.GetMetaFilePath(clusterName))
	if err != nil {
		return err
	}

	var (
		sshConnProps  *operator.SSHConnectionProps = &operator.SSHConnectionProps{}
		sshProxyProps *operator.SSHConnectionProps = &operator.SSHConnectionProps{}
	)
	if gOpt.SSHType != executor.SSHTypeNone {
		var err error
		if sshConnProps, err = operator.ReadIdentityFileOrPassword(a.IdentityFile, a.UsePassword); err != nil {
			return err
		}
		if len(gOpt.SSHProxyHost) != 0 {
			if sshProxyProps, err = operator.ReadIdentityFileOrPassword(gOpt.SSHProxyIdentity, gOpt.SSHProxyUsePassword); err != nil {
				return err
			}
		}
	}

	var sudo bool
	systemdMode := metadata.GetTopology().GlobalOptions.SystemdMode
	if systemdMode == cluster.UserMode {
		sudo = false
		hint := fmt.Sprintf("loginctl enable-linger %s", a.User)
		msg := "The value of systemd_mode is set to `user` in the topology, please note that you'll need to manually execute the following command using root or sudo on the host(s) to enable lingering for the systemd user instance.\n"
		msg += color.GreenString(hint)
		msg += "\nYou can read the systemd documentation for reference: https://wiki.archlinux.org/title/Systemd/User#Automatic_start-up_of_systemd_user_instances."
		mg.Logger.Warnf(msg)
		err = stringutil.PromptForConfirmOrAbortError("Do you want to continue? [y/N]: ")
		if err != nil {
			return err
		}
	} else {
		sudo = a.User != "root"
	}

	if err = mg.FillHostArchOrOS(sshConnProps, sshProxyProps, scaleOutTopo, gOpt, a.User, sudo); err != nil {
		return err
	}

	// patched
	patchedComp := stringutil.NewStringSet()
	scaleOutTopo.IterInstance(func(inst cluster.Instance) {
		if stringutil.IsPathExist(mg.Path(clusterName, cluster.PatchDirName, inst.ComponentName()+".tar.gz")) {
			patchedComp.Insert(inst.ComponentName())
			inst.SetPatched(true)
		}
	})

	// scaleout topology
	metadata.ScaleOutTopology(scaleOutTopo)

	newTopo := metadata.GetTopology()
	cluster.ExpandRelativeDir(newTopo)

	mg.FillTopologyDir(newTopo)

	clusters, err := mg.GetAllClusters()
	if err != nil {
		return err
	}
	err = mg.CheckClusterPortConflict(clusters, clusterName, newTopo)
	if err != nil {
		return err
	}
	err = mg.CheckClusterDirConflict(clusters, clusterName, newTopo)
	if err != nil {
		return err
	}

	if !gOpt.SkipConfirm {
		// patchedComponents are components that have been patched and overwrited
		if err = mg.ConfirmTopology(clusterName, metadata.GetVersion(), newTopo, patchedComp); err != nil {
			return err
		}
	}

	// Build the scale out tasks
	var (
		envInitTasks       []*task.StepDisplay // tasks which are used to initialize environment
		deployCompTasks    []*task.StepDisplay // tasks which are used to copy components to remote host
		refreshConfigTasks []*task.StepDisplay // tasks which are used to refresh config to remote host
		scaleConfigTasks   []*task.StepDisplay // tasks which are used to copy certificate to remote host
	)

	// Initialize the environments
	globalOptions := metadata.GetTopology().GlobalOptions

	initializedHosts := stringutil.NewStringSet()
	metadata.GetTopology().IterInstance(func(inst cluster.Instance) {
		initializedHosts.Insert(inst.InstanceManageHost())
	})

	// uninitializedHosts are hosts which haven't been initialized yet
	uninitializedHosts := make(map[string]int) // host -> ssh-port
	scaleOutNodes := make(map[string]struct{})

	scaleOutTopo.IterInstance(func(inst cluster.Instance) {
		scaleOutNodes[inst.InstanceName()] = struct{}{}

		host := inst.InstanceManageHost()
		if initializedHosts.Exist(host) {
			return
		}
		if _, found := uninitializedHosts[host]; found {
			return
		}

		uninitializedHosts[host] = inst.InstanceSshPort()

		var dirs []string
		for _, dir := range []string{globalOptions.DeployDir, globalOptions.DataDir, globalOptions.LogDir} {
			for _, dirname := range strings.Split(dir, ",") {
				if dirname == "" {
					continue
				}
				dirs = append(dirs, stringutil.Abs(globalOptions.User, dirname))
			}
		}
		if systemdMode == cluster.UserMode {
			dirs = append(dirs, stringutil.Abs(globalOptions.User, ".config/systemd/user"))
		}
		t := task.NewBuilder(mg.Logger).
			RootSSH(
				inst.InstanceManageHost(),
				inst.InstanceSshPort(),
				a.User,
				sshConnProps.Password,
				sshConnProps.IdentityFile,
				sshConnProps.IdentityFilePassphrase,
				gOpt.SSHTimeout,
				gOpt.OptTimeout,
				gOpt.SSHProxyHost,
				gOpt.SSHProxyPort,
				gOpt.SSHProxyUser,
				sshProxyProps.Password,
				sshProxyProps.IdentityFile,
				sshProxyProps.IdentityFilePassphrase,
				gOpt.SSHProxyTimeout,
				gOpt.SSHType,
				executor.SSHType(globalOptions.SSHType),
				a.User != "root" && systemdMode != cluster.UserMode,
			).
			EnvInit(inst.InstanceManageHost(), globalOptions.User, globalOptions.Group, a.SkipCreateUser || globalOptions.User == a.User, sudo).
			Mkdir(globalOptions.User, inst.InstanceManageHost(), sudo, dirs...).
			BuildAsStep(fmt.Sprintf("  - Initialized host %s ", host))

		envInitTasks = append(envInitTasks, t)
	})

	// Deploy the new topology and refresh the configuration
	sshType := executor.SSHType(newTopo.GlobalOptions.SSHType)

	scaleOutTopo.IterInstance(func(inst cluster.Instance) {
		deployDir := stringutil.Abs(globalOptions.User, inst.InstanceDeployDir())
		// data dir would be empty for components which don't need it
		dataDirs := stringutil.MultiDirAbs(globalOptions.User, inst.InstanceDataDir())
		// log dir will always be with values, but might not used by the component
		logDir := stringutil.Abs(globalOptions.User, inst.InstanceLogDir())

		deployDirs := []string{
			deployDir,
			filepath.Join(deployDir, cluster.BinDirName),
			filepath.Join(deployDir, cluster.ConfDirName),
			filepath.Join(deployDir, cluster.ScriptDirName),
		}
		// Deploy component
		tb := task.NewSimpleUerSSH(mg.Logger, inst.InstanceManageHost(), inst.InstanceSshPort(), globalOptions.User, gOpt, sshConnProps, sshType).
			Mkdir(globalOptions.User, inst.InstanceManageHost(), sudo, deployDirs...).
			Mkdir(globalOptions.User, inst.InstanceManageHost(), sudo, dataDirs...).
			Mkdir(globalOptions.User, inst.InstanceManageHost(), sudo, logDir)

		srcPath := ""
		if patchedComp.Exist(inst.ComponentName()) {
			srcPath = mg.Path(clusterName, cluster.PatchDirName, inst.ComponentName()+".tar.gz")
		} else {
			srcPath = gOpt.MirrorDir
		}

		tb.CopyComponent(
			inst.ComponentName(),
			inst.OS(),
			inst.Arch(),
			metadata.GetVersion(),
			srcPath,
			inst.InstanceManageHost(),
			filepath.Join(deployDir, cluster.BinDirName))

		deployCompTasks = append(deployCompTasks, tb.BuildAsStep(fmt.Sprintf("  - Deploy instance %s -> %s", inst.ComponentName(), inst.InstanceName())))
	})

	// init scale out config
	// generate config
	scaleOutTopo.IterInstance(func(inst cluster.Instance) {
		t := task.NewSimpleUerSSH(mg.Logger, inst.InstanceManageHost(), inst.InstanceSshPort(), globalOptions.User, gOpt, sshConnProps, sshType).
			ScaleConfig(
				clusterName,
				newTopo,
				inst,
				globalOptions.User,
				mg.Path(clusterName, cluster.CacheDirName),
			).BuildAsStep(fmt.Sprintf("  - Generate scale-out config %s -> %s", inst.ComponentName(), inst.InstanceName()))
		scaleConfigTasks = append(scaleConfigTasks, t)
	})

	// refresh config
	newTopo.IterInstance(func(inst cluster.Instance) {
		// exclude scale-out node config
		if _, ok := scaleOutNodes[inst.InstanceName()]; ok {
			return
		}

		compName := inst.ComponentName()
		// Download and copy the latest component to remote if the cluster is imported from Ansible
		tb := task.NewBuilder(logger).InitConfig(
			clusterName,
			inst,
			globalOptions.User,
			mg.Path(clusterName, cluster.CacheDirName),
		).
			BuildAsStep(fmt.Sprintf("  - Generate config %s -> %s", compName, inst.InstanceName()))
		refreshConfigTasks = append(refreshConfigTasks, tb)
	})

	// add scale-out locked
	err = mg.NewScaleOutLock(clusterName, newTopo)
	if err != nil {
		return err
	}
	defer mg.ReleaseScaleOutLock(clusterName)

	// task prepare
	bf := task.NewBuilder(mg.Logger).
		Step("+ Generate SSH keys",
			task.NewBuilder(mg.Logger).
				SSHKeyGen(mg.Path(clusterName, cluster.SshDirName, "id_rsa")).
				Build(),
			mg.Logger).
		ParallelStep("+ Initialize target host environments", gOpt.Force, envInitTasks...).
		ParallelStep("+ Deploy TiDB instance", gOpt.Force, deployCompTasks...).
		ParallelStep("+ Generate scale-out config", gOpt.Force, scaleConfigTasks...).
		ParallelStep("+ Refresh components config", gOpt.Force, refreshConfigTasks...).Func("Start new instances", func(ctx context.Context) error {
		return operator.Start(ctx, newTopo, gOpt, nil)
	}).Build()

	if err = bf.Execute(ctxt.New(context.Background(), gOpt.Concurrency, mg.Logger)); err != nil {
		return err
	}

	// save metadata
	err = mg.SaveMetadata(clusterName, metadata)
	if err != nil {
		return err
	}

	mg.Logger.Infof("Scaled cluster `%s` out successfully", color.YellowString(clusterName))
	return nil
}
