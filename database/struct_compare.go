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
	"strings"

	"github.com/wentaojin/dbms/utils/structure"
)

// IDatabaseStructCompare used for database table struct migrate
type IDatabaseStructCompare interface {
	GetDatabaseTablePartitionExpress(schemaName string, tableName string) ([]map[string]string, error)
}

type IDatabaseStructCompareProcessor interface {
	GenDatabaseSchemaTable() (string, string)
	GenDatabaseTableCollation() (string, error)
	GenDatabaseTableCharset() (string, error)
	GenDatabaseTableComment() (string, error)
	GenDatabaseTableColumnDetail() (map[string]structure.NewColumn, map[string]map[string]structure.OldColumn, error)
	GenDatabaseTableIndexDetail() (map[string]structure.Index, error)
	GenDatabaseTablePrimaryConstraintDetail() (map[string]structure.ConstraintPrimary, error)
	GenDatabaseTableUniqueConstraintDetail() (map[string]structure.ConstraintUnique, error)
	GenDatabaseTableForeignConstraintDetail() (map[string]structure.ConstraintForeign, error)
	GenDatabaseTableCheckConstraintDetail() (map[string]structure.ConstraintCheck, error)
	GenDatabaseTablePartitionDetail() ([]structure.Partition, error)
}

type IDatabaseStructCompareTable interface {
	ComparePartitionTableType() string
	CompareTableComment() string
	CompareTableCharsetCollation() string
	CompareTableColumnCharsetCollation() string
	CompareTableColumnCounts() string
	CompareTablePrimaryConstraint() (string, error)
	CompareTableUniqueConstraint() (string, error)
	CompareTableForeignConstraint() (string, error)
	CompareTableCheckConstraint() (string, error)
	CompareTableIndexDetail() (string, error)
	CompareTableColumnDetail() (string, error)
	CompareTablePartitionDetail() (string, error)
}

func IStructCompareProcessor(p IDatabaseStructCompareProcessor) (*structure.Table, error) {
	schemaName, tableName := p.GenDatabaseSchemaTable()
	collation, err := p.GenDatabaseTableCollation()
	if err != nil {
		return nil, err
	}
	charset, err := p.GenDatabaseTableCharset()
	if err != nil {
		return nil, err
	}
	comment, err := p.GenDatabaseTableComment()
	if err != nil {
		return nil, err
	}
	newColumnDetail, oldColumnDetails, err := p.GenDatabaseTableColumnDetail()
	if err != nil {
		return nil, err
	}
	indexDetail, err := p.GenDatabaseTableIndexDetail()
	if err != nil {
		return nil, err
	}
	primaryConstraintDetail, err := p.GenDatabaseTablePrimaryConstraintDetail()
	if err != nil {
		return nil, err
	}
	uniqueConstraintDetail, err := p.GenDatabaseTableUniqueConstraintDetail()
	if err != nil {
		return nil, err
	}
	foreignConstraintDetail, err := p.GenDatabaseTableForeignConstraintDetail()
	if err != nil {
		return nil, err
	}
	checkConstraintDetail, err := p.GenDatabaseTableCheckConstraintDetail()
	if err != nil {
		return nil, err
	}
	partitionDetail, err := p.GenDatabaseTablePartitionDetail()
	if err != nil {
		return nil, err
	}
	return &structure.Table{
		SchemaName:         schemaName,
		TableName:          tableName,
		TableComment:       comment,
		TableCharset:       charset,
		TableCollation:     collation,
		NewColumns:         newColumnDetail,
		OldColumns:         oldColumnDetails,
		Indexes:            indexDetail,
		PrimaryConstraints: primaryConstraintDetail,
		UniqueConstraints:  uniqueConstraintDetail,
		ForeignConstraints: foreignConstraintDetail,
		CheckConstraints:   checkConstraintDetail,
		Partitions:         partitionDetail,
	}, nil
}

func IStructCompareTable(dsct IDatabaseStructCompareTable) (string, error) {
	var b strings.Builder

	if !strings.EqualFold(dsct.ComparePartitionTableType(), "") {
		b.WriteString(dsct.ComparePartitionTableType())
	}
	if !strings.EqualFold(dsct.CompareTableComment(), "") {
		b.WriteString(dsct.CompareTableComment())
	}
	if !strings.EqualFold(dsct.CompareTableCharsetCollation(), "") {
		b.WriteString(dsct.CompareTableCharsetCollation())
	}
	if !strings.EqualFold(dsct.CompareTableColumnCounts(), "") {
		b.WriteString(dsct.CompareTableColumnCounts())
	}
	constraintPK, err := dsct.CompareTablePrimaryConstraint()
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(constraintPK, "") {
		b.WriteString(constraintPK)
	}
	constraintUK, err := dsct.CompareTableUniqueConstraint()
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(constraintUK, "") {
		b.WriteString(constraintUK)
	}
	constraintFK, err := dsct.CompareTableForeignConstraint()
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(constraintFK, "") {
		b.WriteString(constraintFK)
	}
	constraintCK, err := dsct.CompareTableCheckConstraint()
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(constraintCK, "") {
		b.WriteString(constraintCK)
	}
	detailIndex, err := dsct.CompareTableIndexDetail()
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(detailIndex, "") {
		b.WriteString(detailIndex)
	}
	detailPart, err := dsct.CompareTablePartitionDetail()
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(detailPart, "") {
		b.WriteString(detailPart)
	}
	detailColumn, err := dsct.CompareTableColumnDetail()
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(detailColumn, "") {
		b.WriteString(detailColumn)
	}
	return b.String(), nil
}
