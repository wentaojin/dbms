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

type IDatabaseDataCompare interface {
	FilterDatabaseTableColumnDatatype(columnType string) bool
	FindDatabaseTableColumnName(schemaNameS, tableNameS, columnNameS string) ([]string, error)
	GetDatabaseTableColumnBucket(schemaNameS, tableNameS string, columnNameS, datatypeS string) ([]string, error)
	GetDatabaseTableColumnAttribute(schemaNameS, tableNameS, columnNameS string) ([]map[string]string, error)
	GetDatabaseTableColumnDataCompare(querySQL string, callTimeout int, dbCharsetS, dbCharsetT string) ([]string, map[string]int64, error)
}

// IDataCompareRuleInitializer used for database table rule initializer
type IDataCompareRuleInitializer interface {
	GenSchemaTableCompareMethodRule() string
	GenSchemaTableCustomRule() (string, string, error)
	IDatabaseSchemaTableRule
}

type DataCompareAttributesRule struct {
	SchemaNameS    string `json:"schemaNameS"`
	SchemaNameT    string `json:"schemaNameT"`
	TableNameS     string `json:"tableNameS"`
	TableTypeS     string `json:"tableTypeS"`
	TableNameT     string `json:"tableNameT"`
	ColumnDetailSO string `json:"columnDetailSO"`
	ColumnDetailS  string `json:"columnDetailS"`
	ColumnDetailT  string `json:"columnDetailT"`
	ColumnDetailTO string `json:"columnDetailTO"`
	CompareMethod  string `json:"compareMethod"`
	ColumnFieldC   string `json:"columnFieldC"`
	CompareRangeC  string `json:"compareRangeC"`
}

func IDataCompareAttributesRule(i IDataCompareRuleInitializer) (*DataCompareAttributesRule, error) {
	sourceSchema, targetSchema, err := i.GenSchemaNameRule()
	if err != nil {
		return &DataCompareAttributesRule{}, err
	}
	sourceTable, targetTable, err := i.GenSchemaTableNameRule()
	if err != nil {
		return &DataCompareAttributesRule{}, err
	}
	sourceColumnSO, sourceColumnS, targetColumnTO, targetColumnT, err := i.GenSchemaTableColumnRule()
	if err != nil {
		return &DataCompareAttributesRule{}, err
	}
	columnFields, compareRange, err := i.GenSchemaTableCustomRule()
	if err != nil {
		return &DataCompareAttributesRule{}, err
	}
	return &DataCompareAttributesRule{
		SchemaNameS:    sourceSchema,
		SchemaNameT:    targetSchema,
		TableNameS:     sourceTable,
		TableNameT:     targetTable,
		TableTypeS:     i.GenSchemaTableTypeRule(),
		ColumnDetailSO: sourceColumnSO,
		ColumnDetailS:  sourceColumnS,
		ColumnDetailTO: targetColumnTO,
		ColumnDetailT:  targetColumnT,
		CompareMethod:  i.GenSchemaTableCompareMethodRule(),
		ColumnFieldC:   columnFields,
		CompareRangeC:  compareRange,
	}, nil
}

type IDataCompareProcessor interface {
	CompareRows() (bool, error)
	CompareDiff() error
}

func IDataCompareProcess(p IDataCompareProcessor) error {
	isEqual, err := p.CompareRows()
	if err != nil {
		return err
	}
	if !isEqual {
		err = p.CompareDiff()
		if err != nil {
			return err
		}
	}
	return nil
}

type IFileWriter interface {
	InitFile() error
	SyncFile() error
	Close() error
}
