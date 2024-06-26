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
	"strings"

	"github.com/wentaojin/dbms/version"

	"github.com/spf13/cobra"
)

type App struct {
	Server  string
	Version bool
	Args    []string
}

func (a *App) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:               "dbms-ctl",
		Short:             "CLI dbmsctl app for dbms cluster",
		PersistentPreRunE: a.PersistentPreRunE,
		RunE:              a.RunE,
		SilenceUsage:      true,
	}
	c.PersistentFlags().StringVarP(&a.Server, "server", "s", "", "server addr for app server")
	c.Flags().BoolVarP(&a.Version, "version", "v", false, "version for app client")
	return c
}

func (a *App) PersistentPreRunE(cmd *cobra.Command, args []string) error {
	if !strings.EqualFold(cmd.Name(), "decrypt") {
		if strings.EqualFold(a.Server, "") {
			err := cmd.Help()
			if err != nil {
				return err
			}
			return fmt.Errorf("flag parameter [server] are requirement, can not null")
		}
	}
	return nil
}

func (a *App) RunE(cmd *cobra.Command, args []string) error {
	if a.Version {
		fmt.Println(version.GetRawVersionInfo())
	}
	return nil
}
