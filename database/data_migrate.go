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
	GetDatabaseCurrentSCN() (uint64, error)
	GetDatabaseTableType(schemaName string) (map[string]string, error)
	GetDatabaseTableColumns(schemaName string, tableName string, collation bool) ([]map[string]string, error)
	GetDatabaseTableColumnNameTableDimensions(schemaName, tableName string) ([]string, error)
	GetDatabaseTableColumnNameSqlDimensions(sqlStr string) ([]string, map[string]string, map[string]string, error)
	GetDatabaseTableRowsByStatistics(schemaName, tableName string) (uint64, error)
	GetDatabaseTableSizeBySegment(schemaName, tableName string) (float64, error)
	CreateDatabaseTableChunkTask(taskName string) error
	StartDatabaseTableChunkTask(taskName, schemaName, tableName string, chunkSize uint64, callTimeout uint64) error
	GetDatabaseTableChunkData(taskName string) ([]map[string]string, error)
	CloseDatabaseTableChunkTask(taskName string) error
	QueryDatabaseTableChunkData(querySQL string, batchSize, callTimeout int, dbCharsetS, dbCharsetT string, dataChan chan []map[string]interface{}) error
}

// IDataMigrateRuleInitializer used for database table rule initializer
type IDataMigrateRuleInitializer interface {
	GenTableTypeRule() string
	GenSchemaNameRule() (string, string, error)
	GenTableNameRule() (string, string, error)
	GenTableColumnRule() (string, string, error)
	GenTableCustomRule() (bool, string, string, error)
}

type DataMigrateAttributesRule struct {
	SchemaNameS         string `json:"schemaNameS"`
	SchemaNameT         string `json:"schemaNameT"`
	TableNameS          string `json:"tableNameS"`
	TableTypeS          string `json:"tableTypeS"`
	TableNameT          string `json:"tableNameT"`
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
	sourceTable, targetTable, err := i.GenTableNameRule()
	if err != nil {
		return &DataMigrateAttributesRule{}, err
	}
	sourceColumn, targetColumn, err := i.GenTableColumnRule()
	if err != nil {
		return &DataMigrateAttributesRule{}, err
	}
	enableChunkStrategy, whereRange, sqlHintS, err := i.GenTableCustomRule()
	if err != nil {
		return nil, err
	}
	return &DataMigrateAttributesRule{
		SchemaNameS:         sourceSchema,
		SchemaNameT:         targetSchema,
		TableNameS:          sourceTable,
		TableNameT:          targetTable,
		TableTypeS:          i.GenTableTypeRule(),
		ColumnDetailS:       sourceColumn,
		ColumnDetailT:       targetColumn,
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
		err := p.MigrateProcess()
		if err != nil {
			return err
		}
		return nil
	})

	g.Go(func() error {
		err := p.MigrateApply()
		if err != nil {
			return err
		}
		return nil
	})

	err := p.MigrateRead()
	if err != nil {
		return err
	}

	err = g.Wait()
	if err != nil {
		return err
	}
	return nil
}

// ISqlMigrateRuleInitializer used for database table rule initializer
type ISqlMigrateRuleInitializer interface {
	GenSchemaNameRule() (string, error)
	GenTableNameRule() (string, error)
	GenTableColumnRule() (string, string, error)
	GenTableCustomRule() (string, string)
}

type SqlMigrateAttributesRule struct {
	SchemaNameT   string `json:"schemaNameT"`
	TableNameT    string `json:"tableNameT"`
	ColumnDetailS string `json:"columnDetailS"`
	ColumnDetailT string `json:"columnDetailT"`
	SqlHintT      string `json:"sqlHintS"`
	SqlQueryS     string `json:"sqlQueryS"`
}

func ISqlMigrateAttributesRule(i ISqlMigrateRuleInitializer) (*SqlMigrateAttributesRule, error) {
	schemaNameT, err := i.GenSchemaNameRule()
	if err != nil {
		return nil, err
	}
	tableNameT, err := i.GenTableNameRule()
	if err != nil {
		return nil, err
	}
	columnDetailS, columnDetailT, err := i.GenTableColumnRule()
	if err != nil {
		return nil, err
	}
	sqlHintT, sqlQueryS := i.GenTableCustomRule()

	return &SqlMigrateAttributesRule{
		SchemaNameT:   schemaNameT,
		TableNameT:    tableNameT,
		ColumnDetailS: columnDetailS,
		ColumnDetailT: columnDetailT,
		SqlHintT:      sqlHintT,
		SqlQueryS:     sqlQueryS,
	}, nil
}
