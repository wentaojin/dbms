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
	TaskRuleName    string `json:"taskRuleName"`
	DatasourceNameS string `json:"datasourceNameS"`
	DatasourceNameT string `json:"datasourceNameT"`
	Comment         string `json:"comment"`

	SchemaRouteRules []SchemaRouteRule `json:"schemaRouteRules"`
}

type SchemaRouteRule struct {
	SourceSchema       string   `json:"sourceSchema"`
	TargetSchema       string   `json:"targetSchema"`
	SourceIncludeTable []string `json:"sourceIncludeTable"`
	SourceExcludeTable []string `json:"sourceExcludeTable"`

	TableRouteRules []TableRouteRule `json:"tableRouteRules"`
}

type TableRouteRule struct {
	SourceTable      string            `json:"sourceTable"`
	TargetTable      string            `json:"targetTable"`
	ColumnRouteRules map[string]string `json:"columnRouteRules"`
}

func (s *RuleConfig) String() string {
	jsonStr, _ := stringutil.MarshalJSON(s)
	return jsonStr
}

func UpsertTaskRule(serverAddr string, file string) error {
	var cfg = &StructConfig{}
	if _, err := toml.DecodeFile(file, cfg); err != nil {
		return fmt.Errorf("failed decode toml config file %s: %v", file, err)
	}

	_, err := openapi.Request(openapi.RequestPUTMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APITaskRulePath), []byte(cfg.String()))
	if err != nil {
		return err
	}

	return nil
}

func DeleteTaskRule(serverAddr string, name string) error {
	_, err := openapi.Request(openapi.RequestDELETEMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APITaskRulePath), []byte(name))
	if err != nil {
		return err
	}
	return nil
}

func GetTaskRule(serverAddr string, name string) (string, error) {
	resp, err := openapi.Request(openapi.RequestGETMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APITaskRulePath), []byte(name))
	if err != nil {
		return "", err
	}

	jsonStr, err := stringutil.MarshalIndentJSON(resp)
	if err != nil {
		return "", err
	}

	return jsonStr, nil
}
