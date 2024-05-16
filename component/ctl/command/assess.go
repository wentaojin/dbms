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
	"strings"

	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/component/ctl/migrate"

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/service"
)

type AppAssess struct {
	*App
}

func (a *App) AppAssess() component.Cmder {
	return &AppAssess{App: a}
}

func (a *AppAssess) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "assess",
		Short:            "Operator cluster data assess",
		Long:             `Operator cluster data assess`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppAssess) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppAssessUpsert struct {
	*AppAssess
	config string
}

func (a *AppAssess) AppAssessUpsert() component.Cmder {
	return &AppAssessUpsert{AppAssess: a}
}

func (a *AppAssessUpsert) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "upsert",
		Short:            "upsert cluster assess migrate task",
		Long:             `upsert cluster assess migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.config, "config", "c", "config.toml", "config")
	return cmd
}

func (a *AppAssessUpsert) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.config, "") {
		return fmt.Errorf("flag parameter [config] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.UpsertAssessMigrate(a.Server, a.config)
	if err != nil {
		return err
	}
	return nil
}

type AppAssessDelete struct {
	*AppAssess
	task string
}

func (a *AppAssess) AppAssessDelete() component.Cmder {
	return &AppAssessDelete{AppAssess: a}
}

func (a *AppAssessDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster assess migrate task",
		Long:             `delete cluster assess migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "delete assess migrate task")
	return cmd
}

func (a *AppAssessDelete) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("flag parameter [task] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.DeleteAssessMigrate(a.Server, a.task)
	if err != nil {
		return err
	}
	fmt.Printf("Success Delete assess migrate Task [%v]！！！\n", a.task)
	return nil
}

type AppAssessGet struct {
	*AppAssess
	task string
}

func (a *AppAssess) AppAssessGet() component.Cmder {
	return &AppAssessGet{AppAssess: a}
}

func (a *AppAssessGet) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "get",
		Short:            "get cluster assess migrate task",
		Long:             `get cluster assess migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppAssessGet) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.GetAssessMigrate(a.Server, a.task)
	if err != nil {
		return err
	}

	return nil
}

type AppAssessGen struct {
	*AppAssess
	task      string
	outputDir string
}

func (a *AppAssess) AppAssessGen() component.Cmder {
	return &AppAssessGen{AppAssess: a}
}

func (a *AppAssessGen) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "gen",
		Short:            "gen cluster assess migrate task detail",
		Long:             `gen cluster assess migrate task detail`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "the assess migrate task")
	cmd.Flags().StringVarP(&a.outputDir, "outputDir", "o", "/tmp", "the assess migrate task output file dir")
	return cmd
}

func (a *AppAssessGen) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}
	if strings.EqualFold(a.task, "") || strings.EqualFold(a.outputDir, "") {
		return fmt.Errorf("flag parameter [task] and [outputDir] are requirement, can not null")
	}

	err := service.GenAssessMigrateTask(context.Background(), a.Server, a.task, a.outputDir)
	if err != nil {
		return err
	}
	return nil
}
