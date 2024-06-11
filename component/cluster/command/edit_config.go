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
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/utils/cluster"
	"github.com/wentaojin/dbms/utils/cluster/manager"
	"github.com/wentaojin/dbms/utils/cluster/operator"
	"gopkg.in/yaml.v2"
)

type AppEditConfig struct {
	*App
	NewTopoFile string
}

func (a *App) AppEditConfig() component.Cmder {
	return &AppEditConfig{App: a}
}

func (a *AppEditConfig) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:              "edit-config <cluster-name>",
		Short:            "Edit DBMS cluster config",
		Long:             "Edit DBMS cluster config. Will use editor from environment variable `EDITOR`, default use vi",
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
	c.Flags().StringVar(&a.NewTopoFile, "topology-file", "", "Use provided topology file to substitute the original one instead of editing it.")
	return c
}

func (a *AppEditConfig) RunE(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return cmd.Help()
	}

	clusterName := args[0]

	return a.EditConfig(clusterName, gOpt)
}

func (a *AppEditConfig) EditConfig(clusterName string, gOpt *operator.Options) error {
	if err := cluster.ValidateClusterNameOrError(clusterName); err != nil {
		return err
	}

	mg := manager.New(a.MetaDir, logger)
	metadata, err := cluster.ParseMetadataYaml(mg.GetMetaFilePath(clusterName))
	if err != nil {
		return err
	}
	originTopo := metadata.GetTopology()

	data, err := yaml.Marshal(originTopo)
	if err != nil {
		return err
	}
	newTopo, err := mg.EditTopology(originTopo, data, a.NewTopoFile, gOpt.SkipConfirm)
	if err != nil {
		return err
	}

	if newTopo == nil {
		return nil
	}

	mg.Logger.Infof("Applying changes...")
	metadata.SetTopology(newTopo)
	err = mg.SaveMetadata(clusterName, metadata)
	if err != nil {
		return fmt.Errorf("failed to save meta, error detail: %v", err)
	}

	mg.Logger.Infof("Applied successfully, please use `dbms-cluster reload %s [-N <nodes>] [-R <roles>]` to reload config.", clusterName)
	return nil
}
