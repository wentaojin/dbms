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
	"github.com/wentaojin/dbms/utils/stringutil"
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

// ignoreCase used to control whether to ignore case comparison, and convert to uppercase comparison
// ignoreCase control object:
// TableComment       string
// NewColumns         map[string]NewColumn            // columnNameNew -> NewColumn
// OldColumns         map[string]map[string]OldColumn // originColumnName -> columnNameNew -> OldColumn
// Indexes            map[string]Index                // indexName -> Index
// PrimaryConstraints map[string]ConstraintPrimary    // constraintName -> ConstraintPrimary
// UniqueConstraints  map[string]ConstraintUnique     // constraintName -> ConstraintUnique
// ForeignConstraints map[string]ConstraintForeign    // constraintName -> ConstraintForeign
// CheckConstraints   map[string]ConstraintCheck      // constraintName -> ConstraintCheck
// Partitions         []Partition

// default upper object:
// TableCharset       string
// TableCollation     string

// default origin object:
//
//	SchemaName         string
//	TableName          string
func IStructCompareProcessor(p IDatabaseStructCompareProcessor, ignoreCase bool) (*structure.Table, error) {
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

	var (
		commentNew         string
		partitionDetailNew []structure.Partition
	)
	newColumnDetailNew := make(map[string]structure.NewColumn)
	indexDetailNew := make(map[string]structure.Index)
	oldColumnDetailsNew := make(map[string]map[string]structure.OldColumn)
	primaryConstraintDetailNew := make(map[string]structure.ConstraintPrimary)
	uniqueConstraintDetailNew := make(map[string]structure.ConstraintUnique)
	foreignConstraintDetailNew := make(map[string]structure.ConstraintForeign)
	checkConstraintDetailNew := make(map[string]structure.ConstraintCheck)

	if ignoreCase {
		comment = stringutil.StringUpper(comment)
		newColumnDetailNew = stringutil.UppercaseMap(newColumnDetail).(map[string]structure.NewColumn)

		for k, v := range oldColumnDetails {
			oldColumnDetailsNew[stringutil.StringUpper(k)] = stringutil.UppercaseMap(v).(map[string]structure.OldColumn)
		}
		indexDetailNew = stringutil.UppercaseMap(indexDetail).(map[string]structure.Index)
		primaryConstraintDetailNew = stringutil.UppercaseMap(primaryConstraintDetail).(map[string]structure.ConstraintPrimary)
		uniqueConstraintDetailNew = stringutil.UppercaseMap(uniqueConstraintDetail).(map[string]structure.ConstraintUnique)
		foreignConstraintDetailNew = stringutil.UppercaseMap(foreignConstraintDetail).(map[string]structure.ConstraintForeign)
		checkConstraintDetailNew = stringutil.UppercaseMap(checkConstraintDetail).(map[string]structure.ConstraintCheck)
		partitionDetailNew = stringutil.UppercaseMap(partitionDetail).([]structure.Partition)
	} else {
		commentNew = comment
		newColumnDetailNew = newColumnDetail
		oldColumnDetailsNew = oldColumnDetails
		indexDetailNew = indexDetail
		primaryConstraintDetailNew = primaryConstraintDetail
		uniqueConstraintDetailNew = uniqueConstraintDetail
		foreignConstraintDetailNew = foreignConstraintDetail
		checkConstraintDetailNew = checkConstraintDetail
		partitionDetailNew = partitionDetail
	}

	return &structure.Table{
		SchemaName:         schemaName,
		TableName:          tableName,
		TableCharset:       stringutil.StringUpper(charset),
		TableCollation:     stringutil.StringUpper(collation),
		TableComment:       commentNew,
		NewColumns:         newColumnDetailNew,
		OldColumns:         oldColumnDetailsNew,
		Indexes:            indexDetailNew,
		PrimaryConstraints: primaryConstraintDetailNew,
		UniqueConstraints:  uniqueConstraintDetailNew,
		ForeignConstraints: foreignConstraintDetailNew,
		CheckConstraints:   checkConstraintDetailNew,
		Partitions:         partitionDetailNew,
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
