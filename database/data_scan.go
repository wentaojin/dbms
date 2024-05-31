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

// IDataScanRuleInitializer used for database table rule initializer
type IDataScanRuleInitializer interface {
	GenSchemaNameRule() (string, error)
	GenTableNameRule() (string, error)
	GenTableColumnNameRule() (string, string, error)
	GenSchemaTableCustomRule() (string, string, error)
	GenSchemaTableTypeRule() string
}

type DataScanAttributesRule struct {
	SchemaNameS      string `json:"schemaNameS"`
	TableNameS       string `json:"tableNameS"`
	ColumnDetailS    string `json:"columnDetailS"`
	GroupColumnS     string `json:"groupColumnS"`
	TableTypeS       string `json:"tableTypeS"`
	TableSamplerateS string `json:"tableSamplerateS"`
	SqlHintS         string `json:"sqlHintS"`
}

func IDataScanAttributesRule(i IDataScanRuleInitializer) (*DataScanAttributesRule, error) {
	sourceSchema, err := i.GenSchemaNameRule()
	if err != nil {
		return &DataScanAttributesRule{}, err
	}
	sourceTable, err := i.GenTableNameRule()
	if err != nil {
		return nil, err
	}
	columnDetailS, groupColumnS, err := i.GenTableColumnNameRule()
	if err != nil {
		return nil, err
	}
	samplerateS, sqlHintS, err := i.GenSchemaTableCustomRule()
	if err != nil {
		return nil, err
	}

	return &DataScanAttributesRule{
		SchemaNameS:      sourceSchema,
		TableNameS:       sourceTable,
		ColumnDetailS:    columnDetailS,
		GroupColumnS:     groupColumnS,
		TableTypeS:       i.GenSchemaTableTypeRule(),
		TableSamplerateS: samplerateS,
		SqlHintS:         sqlHintS,
	}, nil
}

type IDataScanProcessor interface {
	ScanRows() error
}

func IDataScanProcess(p IDataScanProcessor) error {
	return p.ScanRows()
}
