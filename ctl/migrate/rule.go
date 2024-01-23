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
package migrate

import (
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/wentaojin/dbms/openapi"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type RuleConfig struct {
	TaskRuleName    string `toml:"task-rule-name" json:"taskRuleName"`
	DatasourceNameS string `toml:"datasource-name-s" json:"datasourceNameS"`
	DatasourceNameT string `toml:"datasource-name-t" json:"datasourceNameT"`
	Comment         string `toml:"comment" json:"comment"`

	SchemaRouteRules []SchemaRouteRule `toml:"schema-route-rules" json:"schemaRouteRules"`
}

type SchemaRouteRule struct {
	SourceSchema       string   `toml:"source-schema" json:"sourceSchema"`
	TargetSchema       string   `toml:"target-schema" json:"targetSchema"`
	CaseFieldRule      string   `toml:"case-field-rule" json:"caseFieldRule" `
	SourceIncludeTable []string `toml:"source-include-table" json:"sourceIncludeTable"`
	SourceExcludeTable []string `toml:"source-exclude-table" json:"sourceExcludeTable"`

	TableRouteRules []TableRouteRule `toml:"table-route-rules" json:"tableRouteRules"`
}

type TableRouteRule struct {
	SourceTable      string            `toml:"source-table" json:"sourceTable"`
	TargetTable      string            `toml:"target-table" json:"targetTable"`
	CaseFieldRule    string            `toml:"case-field-rule" json:"caseFieldRule" `
	ColumnRouteRules map[string]string `toml:"column-route-rules" json:"columnRouteRules"`
}

func (s *RuleConfig) String() string {
	jsonStr, _ := stringutil.MarshalJSON(s)
	return jsonStr
}

func UpsertTaskRule(serverAddr string, file string) error {
	var cfg = &RuleConfig{}
	if _, err := toml.DecodeFile(file, cfg); err != nil {
		return fmt.Errorf("failed decode toml config file %s: %v", file, err)
	}
	resp, err := openapi.Request(openapi.RequestPUTMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APITaskRulePath), []byte(cfg.String()))
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

func DeleteTaskRule(serverAddr string, name string) error {
	resp, err := openapi.Request(openapi.RequestDELETEMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APITaskRulePath), []byte(name))
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

func GetTaskRule(serverAddr string, name string) error {
	resp, err := openapi.Request(openapi.RequestGETMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APITaskRulePath), []byte(name))
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
