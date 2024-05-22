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
	"strings"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/utils/cluster"
	"github.com/wentaojin/dbms/utils/cluster/manager"
	"github.com/wentaojin/dbms/utils/cluster/operator"
	"github.com/wentaojin/dbms/utils/cluster/task"
	"github.com/wentaojin/dbms/utils/ctxt"
	"github.com/wentaojin/dbms/utils/executor"
)

type AppStop struct {
	*App
}

func (a *App) AppStop() component.Cmder {
	return &AppStop{App: a}
}

func (a *AppStop) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:              "stop <cluster-name>",
		Short:            "Stop a DBMS cluster",
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
	c.Flags().StringSliceVarP(&gOpt.Roles, "role", "R", nil, "Only start specified roles")
	c.Flags().StringSliceVarP(&gOpt.Nodes, "node", "N", nil, "Only start specified nodes")
	return c
}

func (a *AppStop) RunE(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return cmd.Help()
	}
	clusterName := args[0]
	return a.Stop(clusterName, gOpt)
}

func (a *AppStop) Stop(clusterName string, gOpt *operator.Options) error {
	logger.Infof("Stopping cluster %s....", clusterName)

	mg := manager.New(gOpt.MetaDir, logger)
	metadata, err := cluster.ParseMetadataYaml(mg.GetMetaFilePath(clusterName))
	if err != nil {
		return err
	}
	topo := metadata.GetTopology()

	// valid roles
	var compRoles []string
	topo.IterComponent(func(c cluster.Component) {
		compRoles = append(compRoles, c.ComponentRole())
	})

	for _, r := range gOpt.Roles {
		match := false
		for _, has := range compRoles {
			if r == has {
				match = true
				break
			}
		}
		if !match {
			return fmt.Errorf("not valid role: %s, should be one of: %v", r, compRoles)
		}
	}

	err = mg.ScaleOutLockedErr(clusterName)
	if err != nil {
		return err
	}

	if !gOpt.SkipConfirm {
		cyan := color.New(color.FgCyan, color.Bold)

		fmt.Printf("Cluster type:       %s\n", cyan.Sprint("DBMS"))
		fmt.Printf("Cluster name:       %s\n", cyan.Sprint(clusterName))
		fmt.Printf("Cluster version:    %s\n", cyan.Sprint(metadata.GetVersion()))
		fmt.Printf("Deploy user:        %s\n", cyan.Sprint(metadata.GetUser()))
		fmt.Printf("SSH type:           %s\n", cyan.Sprint(topo.GlobalOptions.SSHType))
		if err := stringutil.PromptForConfirmOrAbortError(
			fmt.Sprintf("Will stop the cluster %s with nodes: %s, roles: %s.\nDo you want to continue? [y/N]:",
				color.HiYellowString(clusterName),
				color.HiRedString(strings.Join(gOpt.Nodes, ",")),
				color.HiRedString(strings.Join(gOpt.Roles, ",")),
			),
		); err != nil {
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
		).Func("StopCluster", func(ctx context.Context) error {
		return operator.Stop(ctx, topo, gOpt)
	}).Build()

	ctx := ctxt.New(
		context.Background(),
		gOpt.Concurrency,
		mg.Logger,
	)
	if err = b.Execute(ctx); err != nil {
		return err
	}

	mg.Logger.Infof("Stopped cluster `%s` successfully", clusterName)
	return nil
}
