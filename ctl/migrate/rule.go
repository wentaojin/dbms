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

type CaseFieldRule struct {
	CaseFieldRuleS string `toml:"case-field-rule-s" json:"caseFieldRuleS"`
	CaseFieldRuleT string `toml:"case-field-rule-t" json:"CaseFieldRuleT"`
}

type SchemaRouteRule struct {
	SchemaNameS   string   `toml:"schema-name-s" json:"schemaNameS"`
	SchemaNameT   string   `toml:"schema-name-t" json:"schemaNameT"`
	IncludeTableS []string `toml:"include-table-s" json:"includeTableS"`
	ExcludeTableS []string `toml:"exclude-table-s" json:"excludeTableS"`

	TableRouteRules   []TableRouteRule   `toml:"table-route-rules" json:"tableRouteRules"`
	TableMigrateRules []TableMigrateRule `toml:"table-migrate-rules" json:"tableMigrateRules"`
}

type TableRouteRule struct {
	TableNameS       string            `toml:"table-name-s" json:"tableNameS"`
	TableNameT       string            `toml:"table-name-t" json:"tableNameT"`
	ColumnRouteRules map[string]string `toml:"column-route-rules" json:"columnRouteRules"`
}

type TableMigrateRule struct {
	TableNameS          string `toml:"table-name-s" json:"tableNameS"`
	EnableChunkStrategy bool   `toml:"enable-chunk-strategy" json:"enableChunkStrategy"`
	WhereRange          string `toml:"where-range" json:"WhereRange"`
	SqlHintS            string `toml:"sql-hint-s" json:"sqlHintS"`
}

type SqlRouteRule struct {
	SqlQueryS        string            `toml:"sql-query-s" json:"sqlQueryS"`
	SchemaNameT      string            `toml:"schema-name-t" json:"schemaNameT"`
	TableNameT       string            `toml:"table-name-t" json:"tableNameT"`
	SqlHintT         string            `toml:"sql-hint-t" json:"sqlHintT"`
	ColumnRouteRules map[string]string `toml:"column-route-rules" json:"columnRouteRules"`
}
