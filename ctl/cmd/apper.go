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
package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

type App struct {
	Server  string
	Version string
	Args    []string
}

func (a *App) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:               "dbmsctl",
		Short:             "CLI dbmsctl app for dbms cluster",
		PersistentPreRunE: a.PersistentPreRunE,
		SilenceUsage:      true,
	}
	c.PersistentFlags().StringVarP(&a.Server, "server", "s", "", "server addr for app server")
	c.PersistentFlags().StringVarP(&a.Version, "version", "v", "", "version for app client")
	return c
}

func (a *App) PersistentPreRunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.Server, "") {
		err := cmd.Help()
		if err != nil {
			return err
		}
		return fmt.Errorf("flag parameter [server] are requirement, can not null")
	}

	return nil
}
