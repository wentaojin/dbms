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
	"github.com/wentaojin/dbms/service"
	"strings"

	"github.com/wentaojin/dbms/component"

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type AppDecrypt struct {
	*App
	task    string
	schema  string
	table   string
	chunk   string
	decrypt string
}

func (a *App) AppDecrypt() component.Cmder {
	return &AppDecrypt{App: a}
}

func (a *AppDecrypt) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "decrypt",
		Short:            "Operator cluster decrypt data",
		Long:             `Operator cluster decrypt data`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "the task name")
	cmd.Flags().StringVarP(&a.schema, "schema", "D", "", "the upstream schema name")
	cmd.Flags().StringVarP(&a.table, "table", "T", "", "the upstream table name")
	cmd.Flags().StringVarP(&a.chunk, "chunk", "c", "", "the upstream table chunk string")
	cmd.Flags().StringVarP(&a.decrypt, "decrypt", "e", "", "the decrypt string")
	return cmd
}

func (a *AppDecrypt) RunE(cmd *cobra.Command, args []string) error {
	cyan := color.New(color.FgCyan, color.Bold)
	fmt.Printf("Component:    %s\n", cyan.Sprint("dbms-ctl"))
	fmt.Printf("Command:      %s\n", cyan.Sprint("decrypt"))
	fmt.Printf("Task:         %s\n", cyan.Sprint(a.task))
	fmt.Printf("Action:       %s\n", cyan.Sprint("decrypt"))

	if !strings.EqualFold(a.decrypt, "") {
		decString, err := stringutil.Decrypt(a.decrypt, []byte(constant.DefaultDataEncryptDecryptKey))
		if err != nil {
			fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
			fmt.Printf("Response:     %s\n", color.RedString("error decrypt failed: %v", err))
			return nil
		}
		fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
		fmt.Printf("Response:     %s\n", color.GreenString("the database table decrypt: %v\n", decString))
		return nil
	} else {
		decrypts, err := service.Decrypt(context.TODO(), a.Server, a.task, a.schema, a.table, a.chunk)
		if err != nil {
			fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
			fmt.Printf("Response:     %s\n", color.RedString("error decrypt failed: %v", err))
			return nil
		}
		if len(decrypts) == 0 {
			fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
			fmt.Printf("Response:     %s\n", color.RedString("the database table chunk aren't exist, decrypt no record"))
			return nil
		}

		fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
		fmt.Printf("Response:     %s\n\n", fmt.Sprintf("the database table chunk decrypt records"))

		stringutil.PrintTable(decrypts, true)
		return nil
	}
}
