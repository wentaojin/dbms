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
	"github.com/wentaojin/dbms/utils/cluster"
	"path/filepath"
	"strings"

	"github.com/wentaojin/dbms/version"

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/logger/printer"
	"github.com/wentaojin/dbms/utils/cluster/manager"
	"github.com/wentaojin/dbms/utils/cluster/operator"
	"github.com/wentaojin/dbms/utils/executor"
)

var (
	logger *printer.Logger
	gOpt   *operator.Options
)

func init() {
	logger = printer.NewLogger("")
	gOpt = &operator.Options{}

}

type App struct {
	Concurrency int
	Format      string
	Ssh         string
	SshTimeout  uint64
	WaitTimeout uint64
	Version     bool
	MetaDir     string
	MirrorDir   string
	SkipConfirm bool
}

func (a *App) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:               "dbms",
		Short:             "the application for the dbms cluster",
		PersistentPreRunE: a.PersistentPreRunE,
		RunE:              a.RunE,
		SilenceUsage:      true,
	}
	c.PersistentFlags().IntVarP(&a.Concurrency, "concurrency", "c", 5, "max number of parallel tasks allowed")
	c.PersistentFlags().StringVar(&a.Ssh, "ssh", "", "(EXPERIMENTAL) The executor type: 'builtin', 'none'")
	c.PersistentFlags().StringVar(&a.Format, "format", "default", "(EXPERIMENTAL) The format of output, available values are [default, json]")
	c.PersistentFlags().Uint64Var(&a.SshTimeout, "ssh-timeout", 5, "Timeout in seconds to connect host via SSH, ignored for operations that don't need an SSH connection")
	c.PersistentFlags().Uint64Var(&a.WaitTimeout, "wait-timeout", 120, "Timeout in seconds to wait for an operation to complete, ignored for operations that don't fit")
	c.PersistentFlags().StringVar(&a.MetaDir, "meta-dir", filepath.Join(cluster.UserHome(), ".dbms"), "The meta dir is used to storage dbms meta information")
	c.PersistentFlags().StringVar(&a.MirrorDir, "mirror-dir", "", "The mirror dir is used to storage dbms component tar package")
	c.PersistentFlags().BoolVar(&a.SkipConfirm, "skip-confirm", false, "the operation skip confirm, always yes")
	c.Flags().BoolVarP(&a.Version, "version", "v", false, "version for app client")
	return c
}

func (a *App) PersistentPreRunE(cmd *cobra.Command, args []string) error {
	gOpt.SSHType = executor.SSHType(a.Ssh)
	gOpt.Concurrency = a.Concurrency
	gOpt.DisplayMode = a.Format
	gOpt.SSHTimeout = a.SshTimeout
	gOpt.OptTimeout = a.WaitTimeout
	gOpt.MetaDir = a.MetaDir
	gOpt.MirrorDir = a.MirrorDir
	gOpt.SkipConfirm = a.SkipConfirm
	printer.SetDisplayModeFromString(gOpt.DisplayMode)
	return nil
}

func (a *App) RunE(cmd *cobra.Command, args []string) error {
	if a.Version {
		fmt.Println(version.GetRawVersionInfo())
	}
	return nil
}

func shellCompGetClusterName(basePath, toComplete string) ([]string, cobra.ShellCompDirective) {
	var result []string
	clusters, _ := manager.GetClusterNameList(basePath)
	for _, c := range clusters {
		if strings.HasPrefix(c, toComplete) {
			result = append(result, c)
		}
	}
	return result, cobra.ShellCompDirectiveNoFileComp
}
