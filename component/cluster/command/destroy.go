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

	"github.com/fatih/color"

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/utils/cluster"
	"github.com/wentaojin/dbms/utils/cluster/manager"
	"github.com/wentaojin/dbms/utils/cluster/operator"
	"github.com/wentaojin/dbms/utils/cluster/task"
	"github.com/wentaojin/dbms/utils/ctxt"
	"github.com/wentaojin/dbms/utils/executor"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type AppDestroy struct {
	*App
	RetainDataNodes []string
	RetainDataRoles []string
}

func (a *App) AppDestroy() component.Cmder {
	return &AppDestroy{App: a}
}

func (a *AppDestroy) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:              "destroy <cluster-name>",
		Short:            "Destroy a specified DBMS cluster",
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
	c.Flags().StringArrayVar(&a.RetainDataNodes, "retain-node-data", nil, "Specify the nodes or hosts whose data will be retained")
	c.Flags().StringArrayVar(&a.RetainDataRoles, "retain-role-data", nil, "Specify the roles whose data will be retained")
	c.Flags().BoolVar(&gOpt.Force, "force", false, "Force will ignore remote error while destroy the cluster")
	return c
}

func (a *AppDestroy) RunE(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return cmd.Help()
	}
	clusterName := args[0]

	gOpt.Operation = operator.DestroyOperation

	return a.Destroy(clusterName, gOpt)
}

func (a *AppDestroy) Destroy(clusterName string, gOpt *operator.Options) error {
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

	// Validate the retained roles to prevent unexpected deleting data
	if len(gOpt.RetainDataRoles) > 0 {
		validRoles := stringutil.NewStringSet(compRoles...)
		for _, role := range gOpt.RetainDataRoles {
			if !validRoles.Exist(role) {
				return fmt.Errorf("role name `%s` invalid", role)
			}
		}
	}

	if err = cluster.ValidateClusterNameOrError(clusterName); err != nil {
		return err
	}

	if !gOpt.SkipConfirm {
		mg.Logger.Warnf(color.HiRedString(stringutil.ASCIIArtWarning))
		if err := stringutil.PromptForAnswerOrAbortError(
			"Yes, I know my cluster and data will be deleted.",
			fmt.Sprintf("This operation will destroy DBMS %s cluster %s and its data.",
				color.HiYellowString(metadata.Version),
				color.HiYellowString(clusterName),
			)+"\nAre you sure to continue?",
		); err != nil {
			return err
		}
		mg.Logger.Infof("Destroying cluster...")
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
	}).Func("DestroyCluster", func(ctx context.Context) error {
		return operator.Destroy(ctx, topo, gOpt)
	}).Build()

	ctx := ctxt.New(
		context.Background(),
		gOpt.Concurrency,
		mg.Logger,
	)
	if err = b.Execute(ctx); err != nil {
		return err
	}

	err = mg.Remove(clusterName)
	if err != nil {
		return err
	}
	mg.Logger.Infof("Destroyed cluster `%s` successfully", clusterName)
	return nil
}
