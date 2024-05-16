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

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/utils/cluster"
	"github.com/wentaojin/dbms/utils/cluster/manager"
	"github.com/wentaojin/dbms/utils/cluster/operator"
	"github.com/wentaojin/dbms/utils/cluster/task"
	"github.com/wentaojin/dbms/utils/ctxt"
	"github.com/wentaojin/dbms/utils/executor"
)

type AppEnable struct {
	*App
}

func (a *App) AppEnable() component.Cmder {
	return &AppEnable{App: a}
}

func (a *AppEnable) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:              "enable <cluster-name>",
		Short:            "Enable automatic enabling of DBMS clusters at boot",
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
	c.Flags().StringSliceVarP(&gOpt.Roles, "role", "R", nil, "Only display specified roles")
	c.Flags().StringSliceVarP(&gOpt.Nodes, "node", "N", nil, "Only display specified nodes")
	return c
}

func (a *AppEnable) RunE(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return cmd.Help()
	}

	clusterName := args[0]
	return a.Enable(clusterName, gOpt)
}

func (a *AppEnable) Enable(clusterName string, gOpt *operator.Options) error {
	mg := manager.New(gOpt.MetaDir, logger)
	meta := mg.NewMetadata()
	metadata, err := meta.ParseMetadata(mg.GetMetaFilePath(clusterName))
	if err != nil {
		return err
	}
	topo := metadata.GetTopology()
	deployUser := metadata.GetUser()

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

	mg.Logger.Infof("Enabling cluster %s...", clusterName)

	var p *operator.SSHConnectionProps = &operator.SSHConnectionProps{}
	if gOpt.SSHType != executor.SSHTypeNone {
		var err error
		if len(gOpt.SSHProxyHost) != 0 {
			if p, err = operator.ReadIdentityFileOrPassword(gOpt.SSHProxyIdentity, gOpt.SSHProxyUsePassword); err != nil {
				return err
			}
		}
	}

	b := task.NewBuilder(mg.Logger).
		SSHKeySet(mg.Path(clusterName, cluster.SshDirName, "id_rsa"), mg.Path(clusterName, cluster.SshDirName, "id_rsa.pub")).
		ClusterSSH(
			topo,
			deployUser,
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
		).Func("EnableCluster", func(ctx context.Context) error {
		return operator.Enable(ctx, topo, gOpt, false)
	}).Build()

	ctx := ctxt.New(
		context.Background(),
		gOpt.Concurrency,
		mg.Logger,
	)
	if err = b.Execute(ctx); err != nil {
		return err
	}

	mg.Logger.Infof("Enabled cluster `%s` successfully", clusterName)
	return nil
}
