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

import "github.com/wentaojin/dbms/utils/stringutil"

type CsvConfig struct {
	TaskName        string `toml:"task-name" json:"taskName"`
	DatasourceNameS string `toml:"datasource-name-s" json:"datasourceNameS"`
	DatasourceNameT string `toml:"datasource-name-t" json:"datasourceNameT"`
	Comment         string `toml:"comment" json:"comment"`

	CaseFieldRule         CaseFieldRule         `toml:"case-field-rule" json:"caseFieldRule"`
	SchemaRouteRule       SchemaRouteRule       `toml:"schema-route-rule" json:"schemaRouteRule"`
	StatementMigrateParam StatementMigrateParam `toml:"statement-migrate-param" json:"statementMigrateParam"`
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
