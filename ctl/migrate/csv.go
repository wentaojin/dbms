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

type CsvConfig struct {
	TaskName        string `toml:"task-name" json:"taskName"`
	DatasourceNameS string `toml:"datasource-name-s" json:"datasourceNameS"`
	DatasourceNameT string `toml:"datasource-name-t" json:"datasourceNameT"`
	Comment         string `toml:"comment" json:"comment"`

	CaseFieldRule   CaseFieldRule   `toml:"case-field-rule" json:"caseFieldRule"`
	SchemaRouteRule SchemaRouteRule `toml:"schema-route-rule" json:"schemaRouteRule"`
	CsvMigrateParam CsvMigrateParam `toml:"csv-migrate-param" json:"csvMigrateParam"`
}

type CsvMigrateParam struct {
	TableThread          uint64 `toml:"table-thread" json:"tableThread"`
	BatchSize            uint64 `toml:"batch-size" json:"batchSize"`
	DiskUsageFactor      string `toml:"disk-usage-factor" json:"diskUsageFactor"`
	Header               bool   `toml:"header" json:"header"`
	Separator            string `toml:"separator" json:"separator"`
	Terminator           string `toml:"terminator" json:"terminator"`
	DataCharsetT         string `toml:"data-charset-t" json:"dataCharsetT"`
	Delimiter            string `toml:"delimiter" json:"delimiter"`
	NullValue            string `toml:"null-value" json:"nullValue"`
	EscapeBackslash      bool   `toml:"escape-backslash" json:"escapeBackslash"`
	ChunkSize            uint64 `toml:"chunk-size" json:"chunkSize"`
	OutputDir            string `toml:"output-dir" json:"outputDir"`
	SqlThreadS           uint64 `toml:"sql-thread-s" json:"sqlThreadS"`
	SqlHintS             string `toml:"sql-hint-s" json:"sqlHintS"`
	CallTimeout          uint64 `toml:"call-timeout" json:"callTimeout"`
	EnableCheckpoint     bool   `toml:"enable-checkpoint" json:"enableCheckpoint"`
	EnableConsistentRead bool   `toml:"enable-consistent-read" json:"enableConsistentRead"`
}

func (c *CsvConfig) String() string {
	jsonStr, _ := stringutil.MarshalJSON(c)
	return jsonStr
}

func UpsertCsvMigrate(serverAddr string, file string) error {
	var cfg = &CsvConfig{}
	if _, err := toml.DecodeFile(file, cfg); err != nil {
		return fmt.Errorf("failed decode toml config file %s: %v", file, err)
	}
	resp, err := openapi.Request(openapi.RequestPUTMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APITaskPath, "/", openapi.APICsvMigratePath), []byte(cfg.String()))
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

func DeleteCsvMigrate(serverAddr string, name string) error {
	resp, err := openapi.Request(openapi.RequestDELETEMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APITaskPath, "/", openapi.APICsvMigratePath), []byte(name))
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

func GetCsvMigrate(serverAddr string, name string) error {
	resp, err := openapi.Request(openapi.RequestGETMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APITaskPath, "/", openapi.APICsvMigratePath), []byte(name))
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
