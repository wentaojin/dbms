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
	"github.com/fatih/color"
	"strings"

	"github.com/wentaojin/dbms/component"

	"github.com/golang/snappy"
	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type AppDecrypt struct {
	*App
	data     string
	category string
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

	cmd.Flags().StringVarP(&a.data, "data", "d", "", "data string")
	cmd.Flags().StringVarP(&a.category, "category", "g", "chunk", "data category")
	return cmd
}

func (a *AppDecrypt) RunE(cmd *cobra.Command, args []string) error {
	cyan := color.New(color.FgCyan, color.Bold)
	fmt.Printf("Component:    %s\n", cyan.Sprint("dbms-ctl"))
	fmt.Printf("Command:      %s\n", cyan.Sprint("decrypt"))
	fmt.Printf("Category:     %s\n", cyan.Sprint(a.category))
	fmt.Printf("Data:         %s\n", cyan.Sprint(a.data))

	if strings.EqualFold(a.category, "chunk") {
		desChunkDetailS, err := stringutil.Decrypt(a.data, []byte(constant.DefaultDataEncryptDecryptKey))
		if err != nil {
			fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
			fmt.Printf("Response:     %s\n", color.RedString("error decrypt failed: %v", err))
			return nil
		}
		decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
		if err != nil {
			fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
			fmt.Printf("Response:     %s\n", color.RedString("error decode failed: %v", err))
			return nil
		}
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.GreenString("the database table chunk decrypt: %v\n", stringutil.BytesToString(decChunkDetailS)))
		return nil
	} else {
		decString, err := stringutil.Decrypt(a.data, []byte(constant.DefaultDataEncryptDecryptKey))
		if err != nil {
			fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
			fmt.Printf("Response:     %s\n", color.RedString("error decrypt failed: %v", err))
			return nil
		}
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.GreenString("the database table decrypt: %v\n", decString))
		return nil
	}
}
