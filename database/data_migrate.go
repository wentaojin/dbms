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
package database

import (
	"golang.org/x/sync/errgroup"
)

// IDatabaseDataMigrate used for database table data migrate
type IDatabaseDataMigrate interface {
	GetDatabaseVersion() (string, error)
	GetDatabaseCharset() (string, error)
	GetDatabaseConsistentPos() (uint64, error)
	GetDatabaseTableType(schemaName string) (map[string]string, error)
	GetDatabaseTableColumnInfo(schemaName string, tableName string, collation bool) ([]map[string]string, error)
	GetDatabaseTableColumnNameTableDimensions(schemaName, tableName string) ([]string, error)
	GetDatabaseTableColumnNameSqlDimensions(sqlStr string) ([]string, map[string]string, map[string]string, error)
	GetDatabaseTableRows(schemaName, tableName string) (uint64, error)
	GetDatabaseTableSize(schemaName, tableName string) (float64, error)
	GetDatabaseTableChunkTask(taskName, schemaName, tableName string, chunkSize uint64, callTimeout uint64) ([]map[string]string, error)
	GetDatabaseTableChunkData(querySQL string, batchSize, callTimeout int, dbCharsetS, dbCharsetT, columnDetailO string, dataChan chan []interface{}) error
	GetDatabaseTableCsvData(querySQL string, callTimeout int, taskFlow, dbCharsetS, dbCharsetT, columnDetailO string, escapeBackslash bool, nullValue, separator, delimiter string, dataChan chan []string) error
}

// IDataMigrateRuleInitializer used for database table rule initializer
type IDataMigrateRuleInitializer interface {
	GenSchemaTableCustomRule() (bool, string, string, error)
	IDatabaseSchemaTableRule
}

type DataMigrateAttributesRule struct {
	SchemaNameS         string `json:"schemaNameS"`
	SchemaNameT         string `json:"schemaNameT"`
	TableNameS          string `json:"tableNameS"`
	TableTypeS          string `json:"tableTypeS"`
	TableNameT          string `json:"tableNameT"`
	ColumnDetailO       string `json:"columnDetailO"`
	ColumnDetailS       string `json:"columnDetailS"`
	ColumnDetailT       string `json:"columnDetailT"`
	EnableChunkStrategy bool   `json:"enableChunkStrategy"`
	SqlHintS            string `json:"sqlHintS"`
	WhereRange          string `json:"whereRange"`
}

func IDataMigrateAttributesRule(i IDataMigrateRuleInitializer) (*DataMigrateAttributesRule, error) {
	sourceSchema, targetSchema, err := i.GenSchemaNameRule()
	if err != nil {
		return &DataMigrateAttributesRule{}, err
	}
	sourceTable, targetTable, err := i.GenSchemaTableNameRule()
	if err != nil {
		return &DataMigrateAttributesRule{}, err
	}
	sourceColumnO, sourceColumnS, _, targetColumnT, err := i.GenSchemaTableColumnRule()
	if err != nil {
		return &DataMigrateAttributesRule{}, err
	}
	enableChunkStrategy, whereRange, sqlHintS, err := i.GenSchemaTableCustomRule()
	if err != nil {
		return nil, err
	}
	return &DataMigrateAttributesRule{
		SchemaNameS:         sourceSchema,
		SchemaNameT:         targetSchema,
		TableNameS:          sourceTable,
		TableNameT:          targetTable,
		TableTypeS:          i.GenSchemaTableTypeRule(),
		ColumnDetailO:       sourceColumnO,
		ColumnDetailS:       sourceColumnS,
		ColumnDetailT:       targetColumnT,
		EnableChunkStrategy: enableChunkStrategy,
		WhereRange:          whereRange,
		SqlHintS:            sqlHintS,
	}, nil
}

type IDataMigrateProcessor interface {
	MigrateRead() error
	MigrateProcess() error
	MigrateApply() error
}

func IDataMigrateProcess(p IDataMigrateProcessor) error {
	g := errgroup.Group{}

	g.Go(func() error {
		return p.MigrateRead()
	})

	g.Go(func() error {
		return p.MigrateProcess()
	})

	g.Go(func() error {
		return p.MigrateApply()
	})

	err := g.Wait()
	if err != nil {
		return err
	}
	return nil
}

// ISqlMigrateRuleInitializer used for database table rule initializer
type ISqlMigrateRuleInitializer interface {
	GenSqlMigrateSchemaNameRule() (string, error)
	GenSqlMigrateTableNameRule() (string, error)
	GenSqlMigrateTableColumnRule() (string, string, string, error)
	GenSqlMigrateTableCustomRule() (string, string)
}

type SqlMigrateAttributesRule struct {
	SchemaNameT   string `json:"schemaNameT"`
	TableNameT    string `json:"tableNameT"`
	ColumnDetailO string `json:"columnDetailO"`
	ColumnDetailS string `json:"columnDetailS"`
	ColumnDetailT string `json:"columnDetailT"`
	SqlHintT      string `json:"sqlHintS"`
	SqlQueryS     string `json:"sqlQueryS"`
}

func ISqlMigrateAttributesRule(i ISqlMigrateRuleInitializer) (*SqlMigrateAttributesRule, error) {
	schemaNameT, err := i.GenSqlMigrateSchemaNameRule()
	if err != nil {
		return nil, err
	}
	tableNameT, err := i.GenSqlMigrateTableNameRule()
	if err != nil {
		return nil, err
	}
	columnDetailO, columnDetailS, columnDetailT, err := i.GenSqlMigrateTableColumnRule()
	if err != nil {
		return nil, err
	}
	sqlHintT, sqlQueryS := i.GenSqlMigrateTableCustomRule()

	return &SqlMigrateAttributesRule{
		SchemaNameT:   schemaNameT,
		TableNameT:    tableNameT,
		ColumnDetailO: columnDetailO,
		ColumnDetailS: columnDetailS,
		ColumnDetailT: columnDetailT,
		SqlHintT:      sqlHintT,
		SqlQueryS:     sqlQueryS,
	}, nil
}
