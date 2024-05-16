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

	"github.com/wentaojin/dbms/logger/printer"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/utils/cluster/manager"
)

type AppList struct {
	*App
}

func (a *App) AppList() component.Cmder {
	return &AppList{App: a}
}

func (a *AppList) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "list",
		Short: "List all clusters",
		RunE:  a.RunE,
	}

	return c
}

func (a *AppList) RunE(cmd *cobra.Command, args []string) error {
	return a.List()
}

func (a *AppList) List() error {
	mg := manager.New(a.MetaDir, logger)

	clusters, err := mg.GetClusterList()
	if err != nil {
		return err
	}

	switch mg.Logger.GetDisplayMode() {
	case printer.DisplayModeJSON:
		clusterObj := struct {
			Clusters []manager.Cluster `json:"clusters"`
		}{
			Clusters: clusters,
		}
		data, err := json.Marshal(clusterObj)
		if err != nil {
			return err
		}
		fmt.Println(string(data))
	default:
		clusterTable := [][]string{
			// Header
			{"Name", "User", "Version", "Path", "PrivateKey"},
		}
		for _, v := range clusters {
			clusterTable = append(clusterTable, []string{
				v.Name,
				v.User,
				v.Version,
				v.Path,
				v.PrivateKey,
			})
		}
		stringutil.PrintTable(clusterTable, true)
	}
	return nil
}
