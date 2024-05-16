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

type StatementConfig struct {
	TaskName        string `toml:"task-name" json:"taskName"`
	DatasourceNameS string `toml:"datasource-name-s" json:"datasourceNameS"`
	DatasourceNameT string `toml:"datasource-name-t" json:"datasourceNameT"`
	Comment         string `toml:"comment" json:"comment"`

	CaseFieldRule         CaseFieldRule         `toml:"case-field-rule" json:"caseFieldRule"`
	SchemaRouteRule       SchemaRouteRule       `toml:"schema-route-rule" json:"schemaRouteRule"`
	DataMigrateRules      []DataMigrateRule     `toml:"data-migrate-rules" json:"dataMigrateRules"`
	StatementMigrateParam StatementMigrateParam `toml:"statement-migrate-param" json:"statementMigrateParam"`
}

type StatementMigrateParam struct {
	TableThread          uint64 `toml:"table-thread" json:"tableThread"`
	BatchSize            uint64 `toml:"batch-size" json:"batchSize"`
	ChunkSize            uint64 `toml:"chunk-size" json:"chunkSize"`
	SqlThreadS           uint64 `toml:"sql-thread-s" json:"sqlThreadS"`
	SqlHintS             string `toml:"sql-hint-s" json:"sqlHintS"`
	SqlThreadT           uint64 `toml:"sql-thread-t" json:"sqlThreadT"`
	SqlHintT             string `toml:"sql-hint-t" json:"sqlHintT"`
	CallTimeout          uint64 `toml:"call-timeout" json:"callTimeout"`
	EnableCheckpoint     bool   `toml:"enable-checkpoint" json:"enableCheckpoint"`
	EnableConsistentRead bool   `toml:"enable-consistent-read" json:"enableConsistentRead"`
}

func (d *StatementConfig) String() string {
	jsonStr, _ := stringutil.MarshalJSON(d)
	return jsonStr
}

func UpsertStmtMigrate(serverAddr string, file string) error {
	var cfg = &StatementConfig{}
	if _, err := toml.DecodeFile(file, cfg); err != nil {
		return fmt.Errorf("failed decode toml config file %s: %v", file, err)
	}
	resp, err := openapi.Request(openapi.RequestPUTMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APITaskPath, "/", openapi.APIStmtMigratePath), []byte(cfg.String()))
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

func DeleteStmtMigrate(serverAddr string, name string) error {
	resp, err := openapi.Request(openapi.RequestDELETEMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APITaskPath, "/", openapi.APIStmtMigratePath), []byte(name))
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

func GetStmtMigrate(serverAddr string, name string) error {
	resp, err := openapi.Request(openapi.RequestGETMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APITaskPath, "/", openapi.APIStmtMigratePath), []byte(name))
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
