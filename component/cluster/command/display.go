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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/logger/printer"

	"github.com/wentaojin/dbms/utils/cluster/operator"

	"github.com/wentaojin/dbms/utils/cluster/manager"

	"github.com/fatih/color"

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/utils/cluster"
)

type AppDisplay struct {
	*App
}

func (a *App) AppDisplay() component.Cmder {
	return &AppDisplay{App: a}
}

func (a *AppDisplay) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:              "display <cluster-name>",
		Short:            "Display information of a DBMS cluster",
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

func (a *AppDisplay) RunE(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return cmd.Help()
	}

	clusterName := args[0]

	dOpt := &manager.DisplayOption{ClusterName: clusterName}
	return a.Display(dOpt, gOpt)
}

func (a *AppDisplay) Display(dOpt *manager.DisplayOption, gOpt *operator.Options) error {
	clusterName := dOpt.ClusterName
	if err := cluster.ValidateClusterNameOrError(clusterName); err != nil {
		return err
	}
	mg := manager.New(a.MetaDir, logger)

	clusterInstInfos, err := mg.GetClusterTopology(dOpt, gOpt)
	if err != nil {
		return err
	}

	metadata := mg.NewMetadata()
	topo := metadata.GetTopology()

	cyan := color.New(color.FgCyan, color.Bold)

	// check if managehost is set
	if !dOpt.ShowManageHost {
		topo.IterInstance(func(inst cluster.Instance) {
			if inst.InstanceHost() != inst.InstanceManageHost() {
				dOpt.ShowManageHost = true
				return
			}
		})
	}

	// display cluster meta
	var j *manager.JSONOutput
	if mg.Logger.GetDisplayMode() == printer.DisplayModeJSON {
		j = &manager.JSONOutput{
			ClusterMetaInfo: manager.ClusterMetaInfo{
				ClusterType:    "DBMS",
				ClusterName:    clusterName,
				ClusterVersion: metadata.GetVersion(),
				DeployUser:     metadata.GetUser(),
				SSHType:        topo.GlobalOptions.SSHType,
			},
			InstanceInfos: clusterInstInfos,
		}

	} else {
		fmt.Printf("Cluster type:       %s\n", cyan.Sprint("DBMS"))
		fmt.Printf("Cluster name:       %s\n", cyan.Sprint(clusterName))
		fmt.Printf("Cluster version:    %s\n", cyan.Sprint(metadata.GetVersion()))
		fmt.Printf("Deploy user:        %s\n", cyan.Sprint(metadata.GetUser()))
		fmt.Printf("SSH type:           %s\n", cyan.Sprint(topo.GlobalOptions.SSHType))
	}

	// display topology
	var clusterTable [][]string
	rowHead := []string{"ID", "Role", "Host"}

	if dOpt.ShowManageHost {
		rowHead = append(rowHead, "Manage Host")
	}

	rowHead = append(rowHead, "Ports", "OS/Arch", "Status")

	if dOpt.ShowUptime {
		rowHead = append(rowHead, "Since")
	}
	if dOpt.ShowNuma {
		rowHead = append(rowHead, "Numa Node", "Numa Cores")
	}

	rowHead = append(rowHead, "Data Dir", "Deploy Dir")
	clusterTable = append(clusterTable, rowHead)

	masterActive := make([]string, 0)
	for _, v := range clusterInstInfos {
		row := []string{
			color.CyanString(v.ID),
			v.Role,
			v.Host,
		}

		if dOpt.ShowManageHost {
			row = append(row, v.ManageHost)
		}

		row = append(row,
			v.Ports,
			v.OsArch,
			manager.FormatInstanceStatus(v.Status))

		if dOpt.ShowUptime {
			row = append(row, v.Since)
		}
		if dOpt.ShowNuma {
			row = append(row, v.NumaNode, v.NumaCores)
		}

		row = append(row, v.DataDir, v.DeployDir)
		clusterTable = append(clusterTable, row)

		if v.ComponentName != cluster.ComponentDBMSMaster {
			continue
		}
		if strings.HasPrefix(v.Status, "Up") || strings.HasPrefix(v.Status, "Healthy") {
			instAddr := stringutil.JoinHostPort(v.ManageHost, v.Port)
			masterActive = append(masterActive, instAddr)
		}
	}

	if mg.Logger.GetDisplayMode() == printer.DisplayModeJSON {
		d, err := json.MarshalIndent(j, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(d))
		return nil
	}

	stringutil.PrintTable(clusterTable, true)
	fmt.Printf("Total nodes: %d\n", len(clusterTable)-1)

	return nil
}
