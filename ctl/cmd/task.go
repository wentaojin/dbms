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

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/openapi"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type AppTask struct {
	*App
}

func (a *App) AppTask() Cmder {
	return &AppTask{App: a}
}

func (a *AppTask) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "task",
		Short:            "Operator cluster task",
		Long:             `Operator cluster task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppTask) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppTaskStart struct {
	*AppTask
	task string
}

func (a *AppTask) AppTaskStart() Cmder {
	return &AppTaskStart{AppTask: a}
}

func (a *AppTaskStart) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "start",
		Short:            "start cluster task",
		Long:             `start cluster task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "operate task name")
	return cmd
}

func (a *AppTaskStart) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("operate task flag [task] can't be null, please setting")
	}

	bodyReq := make(map[string]interface{})
	bodyReq["taskName"] = a.task
	bodyReq["operate"] = constant.TaskOperationStart
	bodyReq["express"] = ""

	jsonStr, err := stringutil.MarshalJSON(bodyReq)
	if err != nil {
		return err
	}
	resp, err := openapi.Request(openapi.RequestPOSTMethod, stringutil.StringBuilder(stringutil.WrapScheme(a.Server, false), openapi.DBMSAPIBasePath, openapi.APITaskPath), []byte(jsonStr))
	if err != nil {
		return err
	}

	var jsonData map[string]interface{}
	err = stringutil.UnmarshalJSON(resp, &jsonData)
	if err != nil {
		return fmt.Errorf("error decoding JSON: %v", err)
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		return fmt.Errorf("error encoding JSON: %v", err)
	}

	fmt.Println(formattedJSON)
	return nil
}

type AppTaskStop struct {
	*AppTask
	task string
}

func (a *AppTask) AppTaskStop() Cmder {
	return &AppTaskStop{AppTask: a}
}

func (a *AppTaskStop) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "stop",
		Short:            "stop cluster task",
		Long:             `stop cluster task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "operate task name")
	return cmd
}

func (a *AppTaskStop) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("operate task flag [task] can't be null, please setting")
	}

	bodyReq := make(map[string]interface{})
	bodyReq["taskName"] = a.task
	bodyReq["operate"] = constant.TaskOperationStop
	bodyReq["express"] = ""

	jsonStr, err := stringutil.MarshalJSON(bodyReq)
	if err != nil {
		return err
	}
	resp, err := openapi.Request(openapi.RequestPOSTMethod, stringutil.StringBuilder(stringutil.WrapScheme(a.Server, false), openapi.DBMSAPIBasePath, openapi.APITaskPath), []byte(jsonStr))
	if err != nil {
		return err
	}

	var jsonData map[string]interface{}
	err = stringutil.UnmarshalJSON(resp, &jsonData)
	if err != nil {
		return fmt.Errorf("error decoding JSON: %v", err)
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		return fmt.Errorf("error encoding JSON: %v", err)
	}

	fmt.Println(formattedJSON)
	return nil
}

type AppTaskDelete struct {
	*AppTask
	task string
}

func (a *AppTask) AppTaskDelete() Cmder {
	return &AppTaskDelete{AppTask: a}
}

func (a *AppTaskDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster non-crontab task",
		Long:             `delete cluster non-crontab task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "operate task name")
	return cmd
}

func (a *AppTaskDelete) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("operate task flag [task] can't be null, please setting")
	}

	bodyReq := make(map[string]interface{})
	bodyReq["taskName"] = a.task
	bodyReq["operate"] = constant.TaskOperationDelete
	bodyReq["express"] = ""

	jsonStr, err := stringutil.MarshalJSON(bodyReq)
	if err != nil {
		return err
	}
	resp, err := openapi.Request(openapi.RequestPOSTMethod, stringutil.StringBuilder(stringutil.WrapScheme(a.Server, false), openapi.DBMSAPIBasePath, openapi.APITaskPath), []byte(jsonStr))
	if err != nil {
		return err
	}

	var jsonData map[string]interface{}
	err = stringutil.UnmarshalJSON(resp, &jsonData)
	if err != nil {
		return fmt.Errorf("error decoding JSON: %v", err)
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		return fmt.Errorf("error encoding JSON: %v", err)
	}

	fmt.Println(formattedJSON)
	return nil
}

type AppTaskCrontab struct {
	*AppTask
	task    string
	express string
}

func (a *AppTask) AppTaskCrontab() Cmder {
	return &AppTaskCrontab{AppTask: a}
}

func (a *AppTaskCrontab) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "crontab",
		Short:            "crontab cluster task",
		Long:             `crontab cluster task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "operate task name")
	cmd.Flags().StringVarP(&a.express, "express", "e", "", "crontab task setting")
	return cmd
}

func (a *AppTaskCrontab) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("operate task flag [task] can't be null, please setting")
	}

	if strings.EqualFold(a.express, "") {
		return fmt.Errorf("operate task flag [express] can't be null, please setting")
	}

	bodyReq := make(map[string]interface{})
	bodyReq["taskName"] = a.task
	bodyReq["operate"] = constant.TaskOperationCrontab
	bodyReq["express"] = a.express

	jsonStr, err := stringutil.MarshalJSON(bodyReq)
	if err != nil {
		return err
	}
	resp, err := openapi.Request(openapi.RequestPOSTMethod, stringutil.StringBuilder(stringutil.WrapScheme(a.Server, false), openapi.DBMSAPIBasePath, openapi.APITaskPath), []byte(jsonStr))
	if err != nil {
		return err
	}

	var jsonData map[string]interface{}
	err = stringutil.UnmarshalJSON(resp, &jsonData)
	if err != nil {
		return fmt.Errorf("error decoding JSON: %v", err)
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		return fmt.Errorf("error encoding JSON: %v", err)
	}

	fmt.Println(formattedJSON)
	return nil
}

type AppTaskClear struct {
	*AppTask
	task string
}

func (a *AppTask) AppTaskClear() Cmder {
	return &AppTaskClear{AppTask: a}
}

func (a *AppTaskClear) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "clear",
		Short:            "clear cluster crontab task",
		Long:             `clear cluster crontab task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "operate task name")
	return cmd
}

func (a *AppTaskClear) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("operate task flag [task] can't be null, please setting")
	}

	bodyReq := make(map[string]interface{})
	bodyReq["taskName"] = a.task
	bodyReq["operate"] = constant.TaskOperationClear
	bodyReq["express"] = ""

	jsonStr, err := stringutil.MarshalJSON(bodyReq)
	if err != nil {
		return err
	}
	resp, err := openapi.Request(openapi.RequestPOSTMethod, stringutil.StringBuilder(stringutil.WrapScheme(a.Server, false), openapi.DBMSAPIBasePath, openapi.APITaskPath), []byte(jsonStr))
	if err != nil {
		return err
	}

	var jsonData map[string]interface{}
	err = stringutil.UnmarshalJSON(resp, &jsonData)
	if err != nil {
		return fmt.Errorf("error decoding JSON: %v", err)
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		return fmt.Errorf("error encoding JSON: %v", err)
	}

	fmt.Println(formattedJSON)
	return nil
}

type AppTaskGet struct {
	*AppTask
	task string
}

func (a *AppTask) AppTaskGet() Cmder {
	return &AppTaskGet{AppTask: a}
}

func (a *AppTaskGet) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "get",
		Short:            "get cluster task",
		Long:             `get cluster task information`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "operate task name")
	return cmd
}

func (a *AppTaskGet) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("operate task flag [task] can't be null, please setting")
	}

	bodyReq := make(map[string]interface{})
	bodyReq["taskName"] = a.task
	bodyReq["operate"] = constant.TaskOperationGet
	bodyReq["express"] = ""

	jsonStr, err := stringutil.MarshalJSON(bodyReq)
	if err != nil {
		return err
	}
	resp, err := openapi.Request(openapi.RequestPOSTMethod, stringutil.StringBuilder(stringutil.WrapScheme(a.Server, false), openapi.DBMSAPIBasePath, openapi.APITaskPath), []byte(jsonStr))
	if err != nil {
		return err
	}

	var jsonData map[string]interface{}
	err = stringutil.UnmarshalJSON(resp, &jsonData)
	if err != nil {
		return fmt.Errorf("error decoding JSON: %v", err)
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		return fmt.Errorf("error encoding JSON: %v", err)
	}

	fmt.Println(formattedJSON)
	return nil
}
