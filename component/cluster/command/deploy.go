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
	"github.com/wentaojin/dbms/utils/constant"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/wentaojin/dbms/utils/ctxt"

	"github.com/wentaojin/dbms/utils/cluster/manager"

	"github.com/wentaojin/dbms/utils/cluster/operator"
	"github.com/wentaojin/dbms/utils/cluster/task"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/fatih/color"

	"github.com/wentaojin/dbms/utils/executor"

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/utils/cluster"
)

type AppDeploy struct {
	*App
	User           string // username to login to the SSH server
	SkipCreateUser bool   // don't create the user
	IdentityFile   string // path to the private key file
	UsePassword    bool   // use password instead of identity file for ssh connection
}

func (a *App) AppDeploy() component.Cmder {
	return &AppDeploy{App: a}
}

func (a *AppDeploy) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:              "deploy <cluster-name> <version> <topology.yaml>",
		Short:            "Deploy a cluster for production",
		Long:             "Deploy a cluster for production. SSH connection will be used to deploy files, as well as creating system users for running the service.",
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			switch len(args) {
			case 2:
				return nil, cobra.ShellCompDirectiveDefault
			default:
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
		},
	}

	c.Flags().StringVarP(&a.User, "user", "u", cluster.CurrentUser(), "The user name to login via SSH. The user must has root (or sudo) privilege.")
	c.Flags().BoolVarP(&a.SkipCreateUser, "skip-create-user", "", false, "(EXPERIMENTAL) Skip creating the user specified in topology.")
	c.Flags().StringVarP(&a.IdentityFile, "identity-file", "i", filepath.Join(cluster.UserHome(), ".ssh", ".id_rsa"), "The path of the SSH identity file. If specified, public key authentication will be used.")
	c.Flags().BoolVarP(&a.UsePassword, "password", "p", false, "Use password of target hosts. If specified, password authentication will be used.")
	return c
}

func (a *AppDeploy) RunE(cmd *cobra.Command, args []string) error {
	scotinue, err := cluster.CheckCommandArgsAndMayPrintHelp(cmd, args, 3)
	if err != nil {
		return err
	}
	if !scotinue {
		return nil
	}

	clusterName := args[0]

	clusterVersion, err := cluster.FmtVer(args[1])
	if err != nil {
		return err
	}
	topoFile := args[2]

	return a.Deploy(clusterName, clusterVersion, topoFile, gOpt)
}

func (a *AppDeploy) Deploy(clusterName, clusterVersion, topoFile string, gOpt *operator.Options) error {
	if strings.EqualFold(gOpt.MirrorDir, "") {
		return fmt.Errorf("the flag parameters cannot be null, please configure --mirror-dir")
	}
	err := cluster.ValidateClusterNameOrError(clusterName)
	if err != nil {
		return err
	}

	mg := manager.New(gOpt.MetaDir, logger)

	conflict, err := mg.CheckClusterNameConflict(clusterName)
	if err != nil {
		return err
	}
	if conflict {
		return fmt.Errorf("cluster name [%s] is duplicated. please specify another cluster name", clusterName)
	}

	clusters, err := mg.GetAllClusters()
	if err != nil {
		return err
	}

	metadata := mg.NewMetadata()

	topo, err := cluster.ParseTopologyYaml(topoFile)
	if err != nil {
		return err
	}

	err = mg.CheckClusterPortConflict(clusters, clusterName, topo)
	if err != nil {
		return err
	}

	instCnt := 0
	topo.IterInstance(func(inst cluster.Instance) {
		instCnt++
	})
	if instCnt < 1 {
		return fmt.Errorf("no valid instance found in the input topology, please check your config")
	}

	cluster.ExpandRelativeDir(topo)

	if len(gOpt.SSHType) != 0 {
		topo.GlobalOptions.SSHType = string(gOpt.SSHType)
	}

	err = mg.CheckClusterDirConflict(clusters, clusterName, topo)
	if err != nil {
		return err
	}

	var (
		sshConnProps  *operator.SSHConnectionProps = &operator.SSHConnectionProps{}
		sshProxyProps *operator.SSHConnectionProps = &operator.SSHConnectionProps{}
	)

	if gOpt.SSHType != executor.SSHTypeNone {
		sshConnProps, err = operator.ReadIdentityFileOrPassword(a.IdentityFile, a.UsePassword)
		if err != nil {
			return err
		}
		if len(gOpt.SSHProxyHost) != 0 {
			if sshProxyProps, err = operator.ReadIdentityFileOrPassword(gOpt.SSHProxyIdentity, gOpt.SSHProxyUsePassword); err != nil {
				return err
			}
		}
	}

	var sudo bool
	systemMode := topo.GlobalOptions.SystemdMode

	if systemMode == cluster.UserMode {
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
		sudo = true
	}

	err = mg.FillHostArchOrOS(sshConnProps, sshProxyProps, topo, gOpt, a.User, sudo)
	if err != nil {
		return err
	}

	mg.FillTopologyDir(topo)

	if stringutil.StringUpper(gOpt.DisplayMode) != "JSON" {
		err = mg.ConfirmTopology(clusterName, clusterVersion, topo, stringutil.NewStringSet())
		if err != nil {
			return err
		}
	}

	err = mg.InitClusterMetadataDir(clusterName)
	if err != nil {
		return err
	}

	var (
		envInitTasks             []*task.StepDisplay // tasks which are used to initialize environment
		deployCompTasks          []*task.StepDisplay // tasks which are used to copy components to remote host
		deployInstantClientTasks []*task.StepDisplay // tasks which are used to copy instantclient to remote host
		refreshConfigTasks       []*task.StepDisplay // tasks which are used to refresh config tasks
	)

	// Initialize environment
	metadata.SetVersion(clusterVersion)
	metadata.SetUser(topo.GlobalOptions.User)
	metadata.SetTopology(topo)

	globalOptions := topo.GlobalOptions

	uniqueHosts := make(map[string]int)     // host -> ssh-port
	uniqueHostOS := make(map[string]string) // host -> os
	topo.IterInstance(func(inst cluster.Instance) {
		if _, found := uniqueHosts[inst.InstanceHost()]; !found {
			uniqueHosts[inst.InstanceHost()] = inst.InstanceSshPort()
			uniqueHostOS[inst.InstanceHost()] = inst.OS()
		}
	})

	// Get instantClient package information
	var instantFileNames []string
	err = filepath.Walk(filepath.Join(a.MirrorDir, cluster.InstantClientDir), func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			// remove filename suffix
			instantFileNames = append(instantFileNames, strings.TrimSuffix(info.Name(), ".tar.gz"))
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(instantFileNames) != 1 {
		return fmt.Errorf("the mirror-dir [%s] instantClient dir file counts are over than one, please contact author check the download offline package or remove repeat the instantClient file", a.MirrorDir)
	}

	instantClientSplits := stringutil.StringSplit(instantFileNames[0], constant.StringSeparatorCenterLine)
	if len(instantClientSplits) != 4 {
		return fmt.Errorf("the mirror-dir [%s] instantClient dir filename does not comply with the rules, normal is instantclient-{version}-{os}-{arch}.tar.gz, current filename is [%s]", a.MirrorDir, instantFileNames[0])
	}

	for host, sshPort := range uniqueHosts {
		var dirs []string
		for _, dir := range []string{globalOptions.DeployDir, globalOptions.LogDir} {
			if dir == "" {
				continue
			}

			dirs = append(dirs, stringutil.Abs(globalOptions.User, dir))
		}
		// the default, relative path of data dir is under deploy dir
		if strings.HasPrefix(globalOptions.DataDir, "/") {
			dirs = append(dirs, globalOptions.DataDir)
		}
		if systemMode == cluster.UserMode {
			dirs = append(dirs, stringutil.Abs(globalOptions.User, ".config/systemd/user"))
		}

		t := task.NewBuilder(logger).
			RootSSH(
				host,
				sshPort,
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
				executor.SSHType(topo.GlobalOptions.SSHType),
				gOpt.SSHType,
				sudo,
			).EnvInit(uniqueHostOS[host], host, globalOptions.User, globalOptions.Group, a.SkipCreateUser, sudo).
			Mkdir(globalOptions.User, host, sudo, dirs...).
			BuildAsStep(fmt.Sprintf("  - Prepare %s:%d", host, sshPort))
		envInitTasks = append(envInitTasks, t)

		c := task.NewSimpleUerSSH(mg.Logger, host, sshPort, globalOptions.User, gOpt, sshProxyProps, executor.SSHType(globalOptions.SSHType)).CopyComponent(
			instantClientSplits[0],
			instantClientSplits[2],
			instantClientSplits[3],
			instantClientSplits[1],
			filepath.Join(gOpt.MirrorDir, cluster.InstantClientDir),
			host,
			globalOptions.DeployDir)

		deployInstantClientTasks = append(deployInstantClientTasks,
			c.BuildAsStep(fmt.Sprintf("  - Copy %s -> %s", instantClientSplits[0], host)),
		)
	}

	// Deploy components to remote
	topo.IterInstance(func(inst cluster.Instance) {
		deployDir := stringutil.Abs(globalOptions.User, inst.InstanceDeployDir())
		// data dir would be empty for components which don't need it
		dataDirs := stringutil.MultiDirAbs(globalOptions.User, inst.InstanceDataDir())
		// log dir will always be with values, but might not used by the component
		logDir := stringutil.Abs(globalOptions.User, inst.InstanceLogDir())
		// Deploy component
		// prepare deployment server
		deployDirs := []string{
			deployDir, logDir,
			filepath.Join(deployDir, cluster.BinDirName),
			filepath.Join(deployDir, cluster.ConfDirName),
			filepath.Join(deployDir, cluster.ScriptDirName),
		}

		t := task.NewSimpleUerSSH(mg.Logger, inst.InstanceManageHost(), inst.InstanceSshPort(), globalOptions.User, gOpt, sshProxyProps, executor.SSHType(globalOptions.SSHType)).
			Mkdir(globalOptions.User, inst.InstanceManageHost(), sudo, deployDirs...).
			Mkdir(globalOptions.User, inst.InstanceManageHost(), sudo, dataDirs...).CopyComponent(
			inst.ComponentName(),
			inst.OS(),
			inst.Arch(),
			clusterVersion,
			gOpt.MirrorDir,
			inst.InstanceManageHost(),
			filepath.Join(deployDir, cluster.BinDirName),
		)

		deployCompTasks = append(deployCompTasks,
			t.BuildAsStep(fmt.Sprintf("  - Copy %s -> %s", inst.ComponentName(), inst.InstanceManageHost())),
		)
	})

	// Refresh config
	topo.IterInstance(func(inst cluster.Instance) {
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

	// task run
	bf := task.NewBuilder(mg.Logger).
		Step("+ Generate SSH keys",
			task.NewBuilder(mg.Logger).
				SSHKeyGen(mg.Path(clusterName, cluster.SshDirName, "id_rsa")).
				Build(),
			mg.Logger).
		ParallelStep("+ Initialize target host environments", false, envInitTasks...).
		ParallelStep("+ Deploy target host instantClients", false, deployInstantClientTasks...).
		ParallelStep("+ Deploy TiDB instance", false, deployCompTasks...).
		ParallelStep("+ Init instance configs", gOpt.Force, refreshConfigTasks...).Build()

	if err = bf.Execute(ctxt.New(context.Background(), gOpt.Concurrency, mg.Logger)); err != nil {
		return err
	}

	// save metadata
	err = mg.SaveMetadata(clusterName, metadata.GenMetadata())
	if err != nil {
		return err
	}

	mg.Logger.Infof("Cluster `%s` deployed successfully, you can start it with command: `%s`", clusterName, color.New(color.Bold).Sprintf("dbms-cluster start %s", clusterName))
	return nil
}
