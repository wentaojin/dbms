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
package structure

import (
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type Table struct {
	SchemaName         string
	TableName          string
	TableComment       string
	TableCharset       string
	TableCollation     string
	NewColumns         map[string]NewColumn            // columnNameNew -> NewColumn
	OldColumns         map[string]map[string]OldColumn // originColumnName -> columnNameNew -> OldColumn
	Indexes            map[string]Index                // indexName -> Index
	PrimaryConstraints map[string]ConstraintPrimary    // constraintName -> ConstraintPrimary
	UniqueConstraints  map[string]ConstraintUnique     // constraintName -> ConstraintPrimary
	ForeignConstraints map[string]ConstraintForeign    // constraintName -> ConstraintPrimary
	CheckConstraints   map[string]ConstraintCheck      // constraintName -> ConstraintPrimary
	Partitions         []Partition
}

type NewColumn struct {
	Datatype    string // custom column datatype
	NULLABLE    string
	DataDefault string
	Charset     string
	Collation   string
	Comment     string
}

type OldColumn struct {
	DatatypeName      string
	DataLength        string
	DataPrecision     string
	DatetimePrecision string // only mysql compatible database
	DataScale         string
	NULLABLE          string
	DataDefault       string
	Charset           string
	Collation         string
	Comment           string
}

type Column struct {
	NewColumns map[string]NewColumn            `json:"newColumn"`
	OldColumns map[string]map[string]OldColumn `json:"oldColumn"`
}

type Index struct {
	IndexType        string
	Uniqueness       string
	IndexColumn      string
	DomainIndexOwner string
	DomainIndexName  string
	DomainParameters string
}

type ConstraintPrimary struct {
	ConstraintColumn string
}

type ConstraintUnique struct {
	ConstraintColumn string
}

type ConstraintForeign struct {
	ConstraintColumn      string
	ReferencedTableSchema string
	ReferencedTableName   string
	ReferencedColumnName  string
	DeleteRule            string
	UpdateRule            string
}

type ConstraintCheck struct {
	ConstraintExpression string
}

type Partition struct {
	PartitionKey     string
	PartitionType    string
	SubPartitionKey  string
	SubPartitionType string
}

func (t *Table) String(jsonType string) string {
	var jsonStr string
	switch jsonType {
	case constant.StructCompareColumnsStructureJSONFormat:
		jsonStr = t.StringColumn()
	case constant.StructComparePrimaryStructureJSONFormat:
		jsonStr, _ = stringutil.MarshalJSON(t.PrimaryConstraints)
	case constant.StructCompareUniqueStructureJSONFormat:
		jsonStr, _ = stringutil.MarshalJSON(t.UniqueConstraints)
	case constant.StructCompareForeignStructureJSONFormat:
		jsonStr, _ = stringutil.MarshalJSON(t.ForeignConstraints)
	case constant.StructCompareCheckStructureJSONFormat:
		jsonStr, _ = stringutil.MarshalJSON(t.CheckConstraints)
	case constant.StructCompareIndexStructureJSONFormat:
		jsonStr, _ = stringutil.MarshalJSON(t.Indexes)
	case constant.StructComparePartitionStructureJSONFormat:
		jsonStr, _ = stringutil.MarshalJSON(t.Partitions)
	}
	return jsonStr
}

func (t *Table) StringColumn() string {
	jsonStr, _ := stringutil.MarshalJSON(Column{
		NewColumns: t.NewColumns,
		OldColumns: t.OldColumns,
	})
	return jsonStr
}
