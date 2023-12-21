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

type StructConfig struct {
	TaskName     string `json:"taskName"`
	TaskRuleName string `json:"taskRuleName"`

	StructMigrateParam StructMigrateParam `json:"structMigrateParam"`
	StructMigrateRule  StructMigrateRule  `json:"structMigrateRule"`
}

type StructMigrateParam struct {
	CaseFieldRule string `json:"caseFieldRule"`
	MigrateThread int64  `json:"migrateThread"`
	TaskQueueSize int64  `json:"taskQueueSize"`
	DirectWrite   bool   `json:"directWrite"`
	OutputDir     string `json:"outputDir"`
}

type StructMigrateRule struct {
	TaskStructRules   []TaskStructRule   `json:"taskStructRules"`
	SchemaStructRules []SchemaStructRule `json:"schemaStructRules"`
	TableStructRules  []TableStructRule  `json:"tableStructRules"`
	ColumnStructRules []ColumnStructRule `json:"columnStructRules"`
	TableAttrsRules   []TableAttrsRule   `json:"tableAttrsRules"`
}

type TaskStructRule struct {
	ColumnTypeS   string `json:"columnTypeS"`
	ColumnTypeT   string `json:"columnTypeT"`
	DefaultValueS string `json:"defaultValueS"`
	DefaultValueT string `json:"defaultValueT"`
}

type SchemaStructRule struct {
	SourceSchema  string `json:"sourceSchema"`
	ColumnTypeS   string `json:"columnTypeS"`
	ColumnTypeT   string `json:"columnTypeT"`
	DefaultValueS string `json:"defaultValueS"`
	DefaultValueT string `json:"defaultValueT"`
}

type TableStructRule struct {
	SourceSchema  string `json:"sourceSchema"`
	SourceTable   string `json:"sourceTable"`
	ColumnTypeS   string `json:"columnTypeS"`
	ColumnTypeT   string `json:"columnTypeT"`
	DefaultValueS string `json:"defaultValueS"`
	DefaultValueT string `json:"defaultValueT"`
}

type ColumnStructRule struct {
	SourceSchema  string `json:"sourceSchema"`
	SourceTable   string `json:"sourceTable"`
	SourceColumn  string `json:"sourceColumn"`
	ColumnTypeS   string `json:"columnTypeS"`
	ColumnTypeT   string `json:"columnTypeT"`
	DefaultValueS string `json:"defaultValueS"`
	DefaultValueT string `json:"defaultValueT"`
}

type TableAttrsRule struct {
	SourceSchema string   `json:"sourceSchema"`
	SourceTables []string `json:"sourceTables"`
	TableAttrsT  string   `json:"tableAttrsT"`
}

func (s *StructConfig) String() string {
	jsonStr, _ := stringutil.MarshalJSON(s)
	return jsonStr
}

func UpsertStructMigrate(serverAddr string, file string) error {
	var cfg = &StructConfig{}
	if _, err := toml.DecodeFile(file, cfg); err != nil {
		return fmt.Errorf("failed decode toml config file %s: %v", file, err)
	}

	_, err := openapi.Request(openapi.RequestPUTMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APIStructMigratePath), []byte(cfg.String()))
	if err != nil {
		return err
	}

	return nil
}

func DeleteStructMigrate(serverAddr string, name string) error {
	_, err := openapi.Request(openapi.RequestDELETEMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APIStructMigratePath), []byte(name))
	if err != nil {
		return err
	}
	return nil
}

func GetStructMigrate(serverAddr string, name string) (string, error) {
	resp, err := openapi.Request(openapi.RequestGETMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APIStructMigratePath), []byte(name))
	if err != nil {
		return "", err
	}

	jsonStr, err := stringutil.MarshalIndentJSON(resp)
	if err != nil {
		return "", err
	}

	return jsonStr, nil
}
