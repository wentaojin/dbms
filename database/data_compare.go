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
	"fmt"
	"github.com/wentaojin/dbms/utils/structure"

	"github.com/wentaojin/dbms/utils/constant"
)

type IDatabaseDataCompare interface {
	GetDatabaseTableConstraintIndexColumn(schemaNameS, tableNameS string) (map[string]string, error)
	GetDatabaseTableStatisticsBucket(schemeNameS, tableNameS string, consColumns map[string]string) (map[string][]structure.Bucket, error)
	GetDatabaseTableStatisticsHistogram(schemeNameS, tableNameS string, consColumns map[string]string) (map[string]structure.Histogram, error)
	GetDatabaseTableColumnProperties(schemaNameS, tableNameS string, columnNameSli []string) ([]map[string]string, error)
	GetDatabaseTableHighestSelectivityIndex(schemaNameS, tableNameS string, compareCondField string, ignoreCondFields []string) (*structure.HighestBucket, error)
	GetDatabaseTableRandomValues(schemaNameS, tableNameS string, columns []string, conditions string, limit int, collations []string) ([][]string, error)
	GetDatabaseTableCompareData(querySQL string, callTimeout int, dbCharsetS, dbCharsetT string) ([]string, uint32, map[string]int64, error)
}

// IDataCompareRuleInitializer used for database table rule initializer
type IDataCompareRuleInitializer interface {
	GenSchemaTableCompareMethodRule() string
	GenSchemaTableCustomRule() (string, string, []string, error)
	IDatabaseSchemaTableRule
}

type DataCompareAttributesRule struct {
	SchemaNameS            string            `json:"schemaNameS"`
	SchemaNameT            string            `json:"schemaNameT"`
	TableNameS             string            `json:"tableNameS"`
	TableTypeS             string            `json:"tableTypeS"`
	TableNameT             string            `json:"tableNameT"`
	ColumnDetailSO         string            `json:"columnDetailSO"`
	ColumnDetailS          string            `json:"columnDetailS"`
	ColumnDetailT          string            `json:"columnDetailT"`
	ColumnDetailTO         string            `json:"columnDetailTO"`
	CompareMethod          string            `json:"compareMethod"`
	ColumnNameRouteRule    map[string]string `json:"columnNameRouteRule"` // keep the column name upstream and downstream mapping, a -> b
	CompareConditionFieldC string            `json:"compareConditionFieldC"`
	CompareConditionRangeC string            `json:"compareConditionRangeC"`
	IgnoreConditionFields  []string          `json:"ignoreConditionFields"`
}

func IDataCompareAttributesRule(i IDataCompareRuleInitializer) (*DataCompareAttributesRule, error) {
	columnFields, compareRange, ignoreConditionFields, err := i.GenSchemaTableCustomRule()
	if err != nil {
		return &DataCompareAttributesRule{}, err
	}
	sourceSchema, targetSchema, err := i.GenSchemaNameRule()
	if err != nil {
		return &DataCompareAttributesRule{}, err
	}
	sourceTable, targetTable, err := i.GenSchemaTableNameRule()
	if err != nil {
		return &DataCompareAttributesRule{}, err
	}
	sourceColumnSO, sourceColumnS, targetColumnTO, targetColumnT, err := i.GenSchemaTableColumnSelectRule()
	if err != nil {
		return &DataCompareAttributesRule{}, err
	}
	columnRouteRule, err := i.GetSchemaTableColumnNameRule()
	if err != nil {
		return nil, err
	}
	return &DataCompareAttributesRule{
		SchemaNameS:            sourceSchema,
		SchemaNameT:            targetSchema,
		TableNameS:             sourceTable,
		TableNameT:             targetTable,
		TableTypeS:             i.GenSchemaTableTypeRule(),
		ColumnDetailSO:         sourceColumnSO,
		ColumnDetailS:          sourceColumnS,
		ColumnDetailTO:         targetColumnTO,
		ColumnDetailT:          targetColumnT,
		CompareMethod:          i.GenSchemaTableCompareMethodRule(),
		CompareConditionFieldC: columnFields,
		CompareConditionRangeC: compareRange,
		IgnoreConditionFields:  ignoreConditionFields,
		ColumnNameRouteRule:    columnRouteRule,
	}, nil
}

type IDataCompareProcessor interface {
	CompareMethod() string
	CompareRows() error
	CompareMD5() error
	CompareCRC32() error
}

func IDataCompareProcess(p IDataCompareProcessor) error {
	switch p.CompareMethod() {
	case constant.DataCompareMethodCheckRows:
		return p.CompareRows()
	case constant.DataCompareMethodCheckCRC32:
		return p.CompareCRC32()
	case constant.DataCompareMethodCheckMD5:
		return p.CompareMD5()
	default:
		return fmt.Errorf("not support compare method [%s]", p.CompareMethod())
	}
}

type IFileWriter interface {
	InitFile() error
	SyncFile() error
	Close() error
}
