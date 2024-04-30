/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed p. in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package processor

import (
	"context"
	"fmt"
	"strings"

	"github.com/wentaojin/dbms/database/mapping"

	"github.com/wentaojin/dbms/model/buildin"

	"github.com/wentaojin/dbms/model/migrate"

	"github.com/wentaojin/dbms/database"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"github.com/wentaojin/dbms/utils/structure"
)

type MySQLProcessor struct {
	Ctx        context.Context `json:"-"`
	TaskName   string          `json:"taskName"`
	TaskFlow   string          `json:"taskFlow"`
	SchemaName string          `json:"schemaName"`
	TableName  string          `json:"tableName"`

	DBCharset                string                           `json:"dbCharset"`
	BuildinDatatypeRules     []*buildin.BuildinDatatypeRule   `json:"-"`
	BuildinDefaultValueRules []*buildin.BuildinDefaultvalRule `json:"-"`
	ColumnRouteRules         map[string]string                `json:"columnRouteRules"`
	IsBaseline               bool                             `json:"isBaseline"`
	Database                 database.IDatabase               `json:"-"`
}

func (p *MySQLProcessor) GenDatabaseSchemaTable() (string, string) {
	return p.SchemaName, p.TableName
}

func (p *MySQLProcessor) GenDatabaseTableCollation() (string, error) {
	collation, err := p.Database.GetDatabaseTableCollation(p.SchemaName, p.TableName)
	if err != nil {
		return "", err
	}
	return collation, nil
}

func (p *MySQLProcessor) GenDatabaseTableCharset() (string, error) {
	charset, err := p.Database.GetDatabaseTableCharset(p.SchemaName, p.TableName)
	if err != nil {
		return "", err
	}
	return charset, nil
}

func (p *MySQLProcessor) GenDatabaseTableComment() (string, error) {
	comment, err := p.Database.GetDatabaseTableComment(p.SchemaName, p.TableName)
	if err != nil {
		return "", err
	}
	if len(comment) == 0 {
		return "", nil
	}
	return comment[0]["COMMENTS"], nil
}

func (p *MySQLProcessor) GenDatabaseTableColumnDetail() (map[string]structure.NewColumn, map[string]map[string]structure.OldColumn, error) {
	infos, err := p.Database.GetDatabaseTableColumnInfo(p.SchemaName, p.TableName, false)
	if err != nil {
		return nil, nil, err
	}

	var (
		structTaskRules   []*migrate.TaskStructRule
		structSchemaRules []*migrate.SchemaStructRule
		structTableRules  []*migrate.TableStructRule
		structColumnRules []*migrate.ColumnStructRule
	)
	// baseline
	if p.IsBaseline {
		structTaskRules, err = model.GetIStructMigrateTaskRuleRW().QueryTaskStructRule(p.Ctx, &migrate.TaskStructRule{TaskName: p.TaskName})
		if err != nil {
			return nil, nil, err
		}
		structSchemaRules, err = model.GetIStructMigrateSchemaRuleRW().QuerySchemaStructRule(p.Ctx, &migrate.SchemaStructRule{
			TaskName:    p.TaskName,
			SchemaNameS: p.SchemaName})
		if err != nil {
			return nil, nil, err
		}
		structTableRules, err = model.GetIStructMigrateTableRuleRW().QueryTableStructRule(p.Ctx, &migrate.TableStructRule{
			TaskName:    p.TaskName,
			SchemaNameS: p.SchemaName,
			TableNameS:  p.TableName})
		if err != nil {
			return nil, nil, err
		}
		structColumnRules, err = model.GetIStructMigrateColumnRuleRW().QueryColumnStructRule(p.Ctx, &migrate.ColumnStructRule{
			TaskName:    p.TaskName,
			SchemaNameS: p.SchemaName,
			TableNameS:  p.TableName})
		if err != nil {
			return nil, nil, err
		}
	}

	newColumns := make(map[string]structure.NewColumn, len(infos))
	oldColumns := make(map[string]map[string]structure.OldColumn, len(infos))

	for _, c := range infos {
		columnNameUtf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_NAME"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenDatabaseTableColumnDetail] mysql compatible database schema [%s] table [%s] column [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_NAME"], err)
		}
		columnName := stringutil.BytesToString(columnNameUtf8Raw)

		defaultValUtf8Raw, err := stringutil.CharsetConvert([]byte(c["DATA_DEFAULT"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenDatabaseTableColumnDetail] mysql compatible database schema [%s] table [%s] column [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_NAME"], err)
		}
		columnDefaultValue := stringutil.BytesToString(defaultValUtf8Raw)

		commentUtf8Raw, err := stringutil.CharsetConvert([]byte(c["COMMENTS"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenDatabaseTableColumnDetail] mysql compatible database schema [%s] table [%s] column [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_NAME"], err)
		}
		columnComment := stringutil.BytesToString(commentUtf8Raw)

		var columnNameNew string
		if val, ok := p.ColumnRouteRules[columnName]; ok {
			columnNameNew = val
		} else {
			columnNameNew = columnName
		}

		if !p.IsBaseline {
			originColumnType, _, err := mapping.MYSQLDatabaseTableColumnMapORACLECompatibleDatatypeRule(&mapping.Column{
				ColumnName:        columnName,
				Datatype:          c["DATA_TYPE"],
				DataPrecision:     c["DATA_PRECISION"],
				DataLength:        c["DATA_LENGTH"],
				DataScale:         c["DATA_SCALE"],
				DatetimePrecision: c["DATETIME_PRECISION"],
				DataDefault:       columnDefaultValue,
				Nullable:          c["NULLABLE"],
				Comment:           columnComment,
			}, p.BuildinDatatypeRules)
			if err != nil {
				return nil, nil, err
			}
			colDefaultVal, err := mapping.MYSQLHandleColumnRuleWitheDefaultValuePriority(columnName, c["DATA_TYPE"], columnDefaultValue, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(c["CHARACTER_SET_NAME"])], constant.CharsetUTF8MB4,
				nil, nil)
			if err != nil {
				return nil, nil, err
			}
			newColumns[columnNameNew] = structure.NewColumn{
				Datatype:    originColumnType,
				NULLABLE:    c["NULLABLE"],
				DataDefault: colDefaultVal,
				Charset:     c["CHARACTER_SET_NAME"],
				Collation:   c["COLLATION_NAME"],
				Comment:     columnComment,
			}

			oldColumns[columnNameNew] = map[string]structure.OldColumn{
				columnName: {
					DatatypeName:      c["DATA_TYPE"],
					DataLength:        c["DATA_LENGTH"],
					DataPrecision:     c["DATA_PRECISION"],
					DatetimePrecision: c["DATETIME_PRECISION"], // only mysql compatible database
					DataScale:         c["DATA_SCALE"],
					NULLABLE:          c["NULLABLE"],
					DataDefault:       colDefaultVal,
					Charset:           c["CHARACTER_SET_NAME"],
					Collation:         c["COLLATION_NAME"],
					Comment:           columnComment,
				},
			}
		} else {
			// task flow
			switch {
			case strings.EqualFold(p.TaskFlow, constant.TaskFlowMySQLToOracle) || strings.EqualFold(p.TaskFlow, constant.TaskFlowTiDBToOracle):
				_, buildInColumnType, err := mapping.MYSQLDatabaseTableColumnMapORACLECompatibleDatatypeRule(&mapping.Column{
					ColumnName:        columnName,
					Datatype:          c["DATA_TYPE"],
					DataPrecision:     c["DATA_PRECISION"],
					DataLength:        c["DATA_LENGTH"],
					DataScale:         c["DATA_SCALE"],
					DatetimePrecision: c["DATETIME_PRECISION"],
					DataDefault:       columnDefaultValue,
					Nullable:          c["NULLABLE"],
					Comment:           columnComment,
				}, p.BuildinDatatypeRules)
				if err != nil {
					return nil, nil, err
				}
				// priority, return target database table column datatype
				convertColumnDatatype, convertColumnDefaultValue, err := mapping.MYSQLHandleColumnRuleWithPriority(
					p.TableName,
					columnName,
					c["DATA_TYPE"],
					buildInColumnType,
					columnDefaultValue,
					constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(c["CHARACTER_SET_NAME"])],
					constant.CharsetUTF8MB4,
					p.BuildinDefaultValueRules,
					structTaskRules,
					structSchemaRules,
					structTableRules,
					structColumnRules)
				if err != nil {
					return nil, nil, err
				}

				newColumns[columnNameNew] = structure.NewColumn{
					Datatype:    convertColumnDatatype,
					NULLABLE:    c["NULLABLE"],
					DataDefault: convertColumnDefaultValue,
					Charset:     constant.MigrateTableStructureDatabaseCharsetMap[p.TaskFlow][c["CHARACTER_SET_NAME"]],
					Collation:   constant.MigrateTableStructureDatabaseCollationMap[p.TaskFlow][c["COLLATION_NAME"]][constant.MigrateTableStructureDatabaseCharsetMap[p.TaskFlow][c["CHARACTER_SET_NAME"]]],
					Comment:     columnComment,
				}

				oldColumns[columnNameNew] = map[string]structure.OldColumn{
					columnName: {
						DatatypeName:      c["DATA_TYPE"],
						DataLength:        c["DATA_LENGTH"],
						DataPrecision:     c["DATA_PRECISION"],
						DatetimePrecision: c["DATETIME_PRECISION"], // only mysql compatible database
						DataScale:         c["DATA_SCALE"],
						NULLABLE:          c["NULLABLE"],
						DataDefault:       convertColumnDefaultValue,
						Charset:           c["CHARACTER_SET_NAME"],
						Collation:         c["COLLATION_NAME"],
						Comment:           columnComment,
					},
				}
			default:
				return nil, nil, fmt.Errorf("mysql compatible database current task [%s] schema [%s] taskflow [%s] column rule isn'p.support, please contact author", p.TaskName, p.SchemaName, p.TaskFlow)
			}
		}

	}

	return newColumns, oldColumns, nil
}

func (p *MySQLProcessor) GenDatabaseTableIndexDetail() (map[string]structure.Index, error) {
	infos, err := p.Database.GetDatabaseTableNormalIndex(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	indexes := make(map[string]structure.Index, len(infos))

	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] mysql compatible database schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["INDEX_NAME"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] mysql compatible database schema [%s] table [%s] index_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["INDEX_NAME"], err)
		}
		indexName := stringutil.BytesToString(utf8Raw)

		var indexColumns []string
		for _, col := range stringutil.StringSplit(columnList, ",") {
			if val, ok := p.ColumnRouteRules[col]; ok {
				indexColumns = append(indexColumns, val)
			} else {
				indexColumns = append(indexColumns, col)
			}
		}
		indexes[indexName] = structure.Index{
			IndexType:   c["INDEX_TYPE"],
			Uniqueness:  c["UNIQUENESS"],
			IndexColumn: stringutil.StringJoin(indexColumns, ","),
			// only oracle compatible database
			DomainIndexOwner: "",
			DomainIndexName:  "",
			DomainParameters: "",
		}
	}

	infos, err = p.Database.GetDatabaseTableUniqueIndex(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] mysql compatible database schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["INDEX_NAME"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] mysql compatible database schema [%s] table [%s] index_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["INDEX_NAME"], err)
		}
		indexName := stringutil.BytesToString(utf8Raw)

		var indexColumns []string
		for _, col := range stringutil.StringSplit(columnList, ",") {
			if val, ok := p.ColumnRouteRules[col]; ok {
				indexColumns = append(indexColumns, val)
			} else {
				indexColumns = append(indexColumns, col)
			}
		}
		indexes[indexName] = structure.Index{
			IndexType:   c["INDEX_TYPE"],
			Uniqueness:  c["UNIQUENESS"],
			IndexColumn: stringutil.StringJoin(indexColumns, ","),
			// only oracle compatible database
			DomainIndexOwner: "",
			DomainIndexName:  "",
			DomainParameters: "",
		}
	}
	return indexes, nil
}

func (p *MySQLProcessor) GenDatabaseTablePrimaryConstraintDetail() (map[string]structure.ConstraintPrimary, error) {
	infos, err := p.Database.GetDatabaseTablePrimaryKey(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	primaryCons := make(map[string]structure.ConstraintPrimary)
	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["CONSTRAINT_NAME"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTablePrimaryConstraintDetail] mysql compatible database schema [%s] table [%s] constraint_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["CONSTRAINT_NAME"], err)
		}
		constraintName := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTablePrimaryConstraintDetail] mysql compatible database schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)
		var indexColumns []string
		for _, col := range stringutil.StringSplit(columnList, ",") {
			if val, ok := p.ColumnRouteRules[col]; ok {
				indexColumns = append(indexColumns, val)
			} else {
				indexColumns = append(indexColumns, col)
			}
		}
		primaryCons[constraintName] = structure.ConstraintPrimary{ConstraintColumn: stringutil.StringJoin(indexColumns, ",")}
	}

	return primaryCons, nil
}

func (p *MySQLProcessor) GenDatabaseTableUniqueConstraintDetail() (map[string]structure.ConstraintUnique, error) {
	infos, err := p.Database.GetDatabaseTableUniqueKey(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	uniqueCons := make(map[string]structure.ConstraintUnique)
	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["CONSTRAINT_NAME"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableUniqueConstraintDetail] mysql compatible database schema [%s] table [%s] constraint_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["CONSTRAINT_NAME"], err)
		}
		constraintName := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableUniqueConstraintDetail] mysql compatible database schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)

		var indexColumns []string
		for _, col := range stringutil.StringSplit(columnList, ",") {
			if val, ok := p.ColumnRouteRules[col]; ok {
				indexColumns = append(indexColumns, val)
			} else {
				indexColumns = append(indexColumns, col)
			}
		}
		uniqueCons[constraintName] = structure.ConstraintUnique{ConstraintColumn: stringutil.StringJoin(indexColumns, ",")}
	}

	return uniqueCons, nil
}

func (p *MySQLProcessor) GenDatabaseTableForeignConstraintDetail() (map[string]structure.ConstraintForeign, error) {
	infos, err := p.Database.GetDatabaseTableForeignKey(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	foreignCons := make(map[string]structure.ConstraintForeign)
	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["CONSTRAINT_NAME"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableForeignConstraintDetail] mysql compatible database schema [%s] table [%s] constraint_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["CONSTRAINT_NAME"], err)
		}
		constraintName := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableForeignConstraintDetail] mysql compatible database schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["RCOLUMN_LIST"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableForeignConstraintDetail] mysql compatible database schema [%s] table [%s] r_column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["RCOLUMN_LIST"], err)
		}
		rColumnList := stringutil.BytesToString(utf8Raw)

		var indexColumns, rIndexColumns []string
		for _, col := range stringutil.StringSplit(columnList, ",") {
			if val, ok := p.ColumnRouteRules[col]; ok {
				indexColumns = append(indexColumns, val)
			} else {
				indexColumns = append(indexColumns, col)
			}
		}
		for _, col := range stringutil.StringSplit(rColumnList, ",") {
			if val, ok := p.ColumnRouteRules[col]; ok {
				rIndexColumns = append(rIndexColumns, val)
			} else {
				rIndexColumns = append(rIndexColumns, col)
			}
		}
		foreignCons[constraintName] = structure.ConstraintForeign{
			ConstraintColumn:      stringutil.StringJoin(indexColumns, ","),
			ReferencedTableSchema: c["R_OWNER"],
			ReferencedTableName:   c["RTABLE_NAME"],
			ReferencedColumnName:  stringutil.StringJoin(rIndexColumns, ","),
			DeleteRule:            c["DELETE_RULE"],
			UpdateRule:            c["UPDATE_RULE"], // database oracle isn'p.supported update rule
		}
	}

	return foreignCons, nil
}

func (p *MySQLProcessor) GenDatabaseTableCheckConstraintDetail() (map[string]structure.ConstraintCheck, error) {
	infos, err := p.Database.GetDatabaseTableCheckKey(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	checkCons := make(map[string]structure.ConstraintCheck)
	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["CONSTRAINT_NAME"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableCheckConstraintDetail] mysql compatible database schema [%s] table [%s] constraint_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["CONSTRAINT_NAME"], err)
		}
		constraintName := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["SEARCH_CONDITION"]), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableCheckConstraintDetail] mysql compatible database schema [%s] table [%s] search_condition [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["SEARCH_CONDITION"], err)
		}
		searchCondition := stringutil.BytesToString(utf8Raw)

		checkCons[constraintName] = structure.ConstraintCheck{ConstraintExpression: searchCondition}
	}

	return checkCons, nil
}

func (p *MySQLProcessor) GenDatabaseTablePartitionDetail() ([]structure.Partition, error) {
	express, err := p.Database.GetDatabaseTablePartitionExpress(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}
	if len(express) == 0 {
		return nil, nil
	}
	var partitions []structure.Partition
	for _, c := range express {
		partitions = append(partitions, structure.Partition{
			PartitionKey:     c["PARTITION_EXPRESSION"],
			PartitionType:    c["PARTITION_METHOD"],
			SubPartitionKey:  c["SUBPARTITION_EXPRESSION"],
			SubPartitionType: c["SUBPARTITION_METHOD"],
		})
	}

	return partitions, nil
}
