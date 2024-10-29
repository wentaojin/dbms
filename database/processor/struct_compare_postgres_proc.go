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
package processor

import (
	"context"
	"fmt"
	"strings"

	"github.com/wentaojin/dbms/model/rule"

	"github.com/wentaojin/dbms/database/mapping"

	"github.com/wentaojin/dbms/model"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/model/buildin"
	"github.com/wentaojin/dbms/model/migrate"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/utils/stringutil"
	"github.com/wentaojin/dbms/utils/structure"
)

type PostgresProcessor struct {
	Ctx                      context.Context                  `json:"-"`
	TaskName                 string                           `json:"taskName"`
	TaskFlow                 string                           `json:"taskFlow"`
	SchemaName               string                           `json:"schemaName"`
	TableName                string                           `json:"tableName"`
	DBCharset                string                           `json:"dbCharset"`
	Database                 database.IDatabase               `json:"-"`
	BuildinDatatypeRules     []*buildin.BuildinDatatypeRule   `json:"-"`
	BuildinDefaultValueRules []*buildin.BuildinDefaultvalRule `json:"-"`
	ColumnRouteRules         map[string]string                `json:"columnRouteRules"`
	IsBaseline               bool                             `json:"isBaseline"`
	TableCharset             string                           `json:"tableCharset"`
	TableCollation           string                           `json:"tableCollation"`
}

func (p *PostgresProcessor) GenDatabaseSchemaTable() (string, string) {
	return p.SchemaName, p.TableName
}

func (p *PostgresProcessor) GenDatabaseTableCollation() (string, error) {
	tableCollation, err := p.Database.GetDatabaseTableCollation(p.SchemaName, p.TableName)
	if err != nil {
		return "", err
	}
	p.TableCollation = stringutil.StringUpper(tableCollation)
	return stringutil.StringUpper(tableCollation), nil
}

func (p *PostgresProcessor) GenDatabaseTableCharset() (string, error) {
	tableCharset, err := p.Database.GetDatabaseTableCharset(p.SchemaName, p.TableName)
	if err != nil {
		return "", err
	}
	if strings.EqualFold(tableCharset, "") {
		p.TableCharset = p.DBCharset
		return p.DBCharset, nil
	}
	p.TableCharset = stringutil.StringUpper(tableCharset)
	return stringutil.StringUpper(tableCharset), nil
}

func (p *PostgresProcessor) GenDatabaseTableComment() (string, error) {
	comment, err := p.Database.GetDatabaseTableComment(p.SchemaName, p.TableName)
	if err != nil {
		return "", err
	}
	if len(comment) == 0 {
		return "", nil
	}
	return comment[0]["COMMENT"], nil
}

func (p *PostgresProcessor) GenDatabaseTableColumnDetail() (map[string]structure.NewColumn, map[string]map[string]structure.OldColumn, error) {
	infos, err := p.Database.GetDatabaseTableColumnInfo(p.SchemaName, p.TableName)
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

		columnRoutes, err := model.GetIMigrateColumnRouteRW().FindColumnRouteRule(p.Ctx, &rule.ColumnRouteRule{
			TaskName:    p.TaskName,
			SchemaNameS: p.SchemaName,
			TableNameS:  p.TableName,
		})
		if err != nil {
			return nil, nil, err
		}
		for _, c := range columnRoutes {
			p.ColumnRouteRules[c.ColumnNameS] = c.ColumnNameT
		}
	}

	newColumns := make(map[string]structure.NewColumn, len(infos))
	oldColumns := make(map[string]map[string]structure.OldColumn, len(infos))

	for _, c := range infos {
		columnNameUtf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_NAME"]), constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenDatabaseTableColumnDetail] postgres schema [%s] table [%s] column [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_NAME"], err)
		}
		columnName := stringutil.BytesToString(columnNameUtf8Raw)

		defaultValUtf8Raw, err := stringutil.CharsetConvert([]byte(c["DATA_DEFAULT"]), constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenDatabaseTableColumnDetail] postgres schema [%s] table [%s] column [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_NAME"], err)
		}
		columnDefaultValue := stringutil.BytesToString(defaultValUtf8Raw)

		commentUtf8Raw, err := stringutil.CharsetConvert([]byte(c["COMMENT"]), constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenDatabaseTableColumnDetail] postgres schema [%s] table [%s] column [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_NAME"], err)
		}
		columnComment := stringutil.BytesToString(commentUtf8Raw)

		var columnNameNew string
		if val, ok := p.ColumnRouteRules[columnName]; ok {
			columnNameNew = val
		} else {
			columnNameNew = columnName
		}

		var (
			columnCharset   string
			columnCollation string
		)
		if strings.EqualFold(c["CHARSET"], "UNKNOWN") {
			if stringutil.IsContainedStringIgnoreCase(constant.PostgresDatabaseTableColumnSupportCharsetCollationDatatype, c["DATA_TYPE"]) {
				columnCharset = p.TableCharset
			} else {
				// exclude integer datatype or not support column charset datatype
				columnCharset = ""
			}
		} else {
			columnCharset = c["CHARSET"]
		}
		if strings.EqualFold(c["COLLATION"], "UNKNOWN") {
			if stringutil.IsContainedStringIgnoreCase(constant.PostgresDatabaseTableColumnSupportCharsetCollationDatatype, c["DATA_TYPE"]) {
				columnCollation = p.TableCollation
			} else {
				// exclude integer datatype or not support column charset datatype
				columnCollation = ""
			}
		} else {
			columnCollation = c["COLLATION"]
		}

		if !p.IsBaseline {
			originColumnType, _, err := mapping.PostgresDatabaseTableColumnMapMYSQLCompatibleDatatypeRule(
				p.TaskFlow,
				&mapping.Column{
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

			colDefaultVal, err := mapping.PostgresHandleColumnRuleWitheDefaultValuePriority(columnName, columnDefaultValue, constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(columnCharset)], constant.CharsetUTF8MB4, nil, nil)
			if err != nil {
				return nil, nil, err
			}

			newColumns[columnNameNew] = structure.NewColumn{
				Datatype:    stringutil.StringUpper(originColumnType),
				NULLABLE:    c["NULLABLE"],
				DataDefault: colDefaultVal,
				Charset:     stringutil.StringUpper(columnCharset),
				Collation:   stringutil.StringUpper(columnCollation),
				Comment:     columnComment,
			}

			oldColumns[columnNameNew] = map[string]structure.OldColumn{
				columnName: {
					Datatype:          stringutil.StringUpper(originColumnType),
					DatatypeName:      c["DATA_TYPE"],
					DataLength:        c["DATA_LENGTH"],
					DataPrecision:     c["DATA_PRECISION"],
					DatetimePrecision: c["DATETIME_PRECISION"], // only postgres compatible database
					DataScale:         c["DATA_SCALE"],
					NULLABLE:          c["NULLABLE"],
					DataDefault:       colDefaultVal,
					Charset:           columnCharset,
					Collation:         columnCollation,
					Comment:           columnComment,
				},
			}
		} else {
			// task flow
			switch {
			case strings.EqualFold(p.TaskFlow, constant.TaskFlowPostgresToMySQL) || strings.EqualFold(p.TaskFlow, constant.TaskFlowPostgresToTiDB):
				originColumnType, buildInColumnType, err := mapping.PostgresDatabaseTableColumnMapMYSQLCompatibleDatatypeRule(
					p.TaskFlow,
					&mapping.Column{
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
				convertColumnDatatype, convertColumnDefaultValue, err := mapping.PostgresHandleColumnRuleWithPriority(
					p.TableName,
					columnName,
					originColumnType,
					buildInColumnType,
					columnDefaultValue,
					constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(columnCharset)],
					constant.CharsetUTF8MB4,
					p.BuildinDefaultValueRules,
					structTaskRules,
					structSchemaRules,
					structTableRules,
					structColumnRules)
				if err != nil {
					return nil, nil, err
				}

				convColumnCharset, convColumnCollation, err := p.genPostgresMappingMYSQLCompatibleDatabaseTableColumnCharsetCollationByNewColumn(columnName, stringutil.StringUpper(columnCharset), stringutil.StringUpper(columnCollation))
				if err != nil {
					return nil, nil, err
				}

				// MYSQL compatible database integer column datatype exclude decimal "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT"
				if val, ok := constant.MYSQLCompatibleDatabaseTableIntegerColumnDatatypeWidthExcludeDecimal[convertColumnDatatype]; ok {
					convertColumnDatatype = stringutil.StringBuilder(convertColumnDatatype, `(`, val, `)`)
				}

				newColumns[columnNameNew] = structure.NewColumn{
					Datatype:    stringutil.StringUpper(convertColumnDatatype),
					NULLABLE:    c["NULLABLE"],
					DataDefault: convertColumnDefaultValue,
					Charset:     stringutil.StringUpper(convColumnCharset),
					Collation:   stringutil.StringUpper(convColumnCollation),
					Comment:     columnComment,
				}

				oldColumns[columnNameNew] = map[string]structure.OldColumn{
					columnName: {
						Datatype:          stringutil.StringUpper(originColumnType),
						DatatypeName:      c["DATA_TYPE"],
						DataLength:        c["DATA_LENGTH"],
						DataPrecision:     c["DATA_PRECISION"],
						DatetimePrecision: c["DATETIME_PRECISION"], // only postgres compatible database
						DataScale:         c["DATA_SCALE"],
						NULLABLE:          c["NULLABLE"],
						DataDefault:       convertColumnDefaultValue,
						Charset:           c["CHARACTER_SET_NAME"],
						Collation:         c["COLLATION_NAME"],
						Comment:           columnComment,
					},
				}
			default:
				return nil, nil, fmt.Errorf("postgres compatible database current task [%s] schema [%s] taskflow [%s] column rule isn'p.support, please contact author", p.TaskName, p.SchemaName, p.TaskFlow)
			}
		}
	}

	return newColumns, oldColumns, nil
}

func (p *PostgresProcessor) GenDatabaseTableIndexDetail() (map[string]structure.Index, error) {
	infos, err := p.Database.GetDatabaseTableNormalIndex(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}
	indexes := make(map[string]structure.Index, len(infos))

	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] postgres schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["INDEX_OWNER"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] postgres schema [%s] table [%s] index_owner [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["INDEX_OWNER"], err)
		}
		indexOwner := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["INDEX_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] postgres schema [%s] table [%s] index_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["INDEX_NAME"], err)
		}
		indexName := stringutil.BytesToString(utf8Raw)

		// ignore the same index name under different schema users
		// marvin00.idx_name00、marvin01.idx_name00
		// Note:
		// additional processing of the same index name in different schemas
		// automatically change index names for index names that are not under the current schema，for example:
		// the migrate schema is marvin00, index_names list: marvin00.idx_marvin, marvin01.idx_marvin, then marvin01.idx_marvin -> marvin00.idx_marvin_marvin01
		if !strings.EqualFold(indexOwner, p.SchemaName) {
			indexName = stringutil.StringBuilder(indexName, "_ow_", indexOwner)
		}

		// column route
		indexColumns := strings.Split(columnList, constant.StringSeparatorComplexSymbol)
		// Function Index: SUBSTR("NAME8",1,8)|+|NAME9
		for i, s := range indexColumns {
			// SUBSTR("NAME8",1,8) OR NAME9
			brackets := stringutil.StringExtractorWithinBrackets(s)
			if len(brackets) == 0 {
				// NAME9 PART
				if val, ok := p.ColumnRouteRules[s]; ok {
					indexColumns[i] = val
				} else {
					indexColumns[i] = s
				}
			} else {
				for k, v := range p.ColumnRouteRules {
					if stringutil.StringMatcher(s, "\""+k+"\"") {
						indexColumns[i] = stringutil.StringReplacer(s, "\""+k+"\"", v)
					}
				}
			}
		}

		indexes[indexName] = structure.Index{
			IndexType:   c["INDEX_TYPE"],
			Uniqueness:  c["UNIQUENESS"],
			IndexColumn: stringutil.StringJoin(indexColumns, constant.StringSeparatorComplexSymbol),
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
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] postgres schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["INDEX_OWNER"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] postgres schema [%s] table [%s] index_owner [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["INDEX_OWNER"], err)
		}
		indexOwner := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["INDEX_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] postgres schema [%s] table [%s] index_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["INDEX_NAME"], err)
		}
		indexName := stringutil.BytesToString(utf8Raw)

		// ignore the same index name under different schema users
		// marvin00.idx_name00、marvin01.idx_name00
		// Note:
		// additional processing of the same index name in different schemas
		// automatically change index names for index names that are not under the current schema，for example:
		// the migrate schema is marvin00, index_names list: marvin00.idx_marvin, marvin01.idx_marvin, then marvin01.idx_marvin -> marvin00.idx_marvin_marvin01
		if !strings.EqualFold(indexOwner, p.SchemaName) {
			indexName = stringutil.StringBuilder(indexName, "_ow_", indexOwner)
		}

		// column route
		indexColumns := strings.Split(columnList, constant.StringSeparatorComplexSymbol)
		// Function Index: SUBSTR("NAME8",1,8)|+|NAME9
		for i, s := range indexColumns {
			// SUBSTR("NAME8",1,8) OR NAME9
			brackets := stringutil.StringExtractorWithinBrackets(s)
			if len(brackets) == 0 {
				// NAME9 PART
				if val, ok := p.ColumnRouteRules[s]; ok {
					indexColumns[i] = val
				} else {
					indexColumns[i] = s
				}
			} else {
				for k, v := range p.ColumnRouteRules {
					if stringutil.StringMatcher(s, "\""+k+"\"") {
						indexColumns[i] = stringutil.StringReplacer(s, "\""+k+"\"", v)
					}
				}
			}
		}

		indexes[indexName] = structure.Index{
			IndexType:   c["INDEX_TYPE"],
			Uniqueness:  c["UNIQUENESS"],
			IndexColumn: stringutil.StringJoin(indexColumns, constant.StringSeparatorComplexSymbol),
			// only oracle compatible database
			DomainIndexOwner: "",
			DomainIndexName:  "",
			DomainParameters: "",
		}
	}
	return indexes, nil
}

func (p *PostgresProcessor) GenDatabaseTablePrimaryConstraintDetail() (map[string]structure.ConstraintPrimary, error) {
	infos, err := p.Database.GetDatabaseTablePrimaryKey(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	primaryCons := make(map[string]structure.ConstraintPrimary, len(infos))
	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTablePrimaryConstraintDetail] postgres schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["CONSTRAINT_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] postgres schema [%s] table [%s] constraint_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["CONSTRAINT_NAME"], err)
		}
		constraintName := stringutil.BytesToString(utf8Raw)

		// column route
		var indexColumns []string
		for _, col := range stringutil.StringSplit(columnList, constant.StringSeparatorComplexSymbol) {
			if val, ok := p.ColumnRouteRules[col]; ok {
				indexColumns = append(indexColumns, val)
			} else {
				indexColumns = append(indexColumns, col)
			}
		}

		primaryCons[constraintName] = structure.ConstraintPrimary{ConstraintColumn: stringutil.StringJoin(indexColumns, constant.StringSeparatorComplexSymbol)}
	}

	return primaryCons, nil
}

func (p *PostgresProcessor) GenDatabaseTableUniqueConstraintDetail() (map[string]structure.ConstraintUnique, error) {
	infos, err := p.Database.GetDatabaseTableUniqueKey(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	uniqueCons := make(map[string]structure.ConstraintUnique)
	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableUniqueConstraintDetail] postgres schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["CONSTRAINT_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] postgres schema [%s] table [%s] constraint_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["CONSTRAINT_NAME"], err)
		}
		constraintName := stringutil.BytesToString(utf8Raw)

		// column route
		var indexColumns []string
		for _, col := range stringutil.StringSplit(columnList, constant.StringSeparatorComplexSymbol) {
			if val, ok := p.ColumnRouteRules[col]; ok {
				indexColumns = append(indexColumns, val)
			} else {
				indexColumns = append(indexColumns, col)
			}
		}

		uniqueCons[constraintName] = structure.ConstraintUnique{ConstraintColumn: stringutil.StringJoin(indexColumns, constant.StringSeparatorComplexSymbol)}
	}

	return uniqueCons, nil
}

func (p *PostgresProcessor) GenDatabaseTableForeignConstraintDetail() (map[string]structure.ConstraintForeign, error) {
	infos, err := p.Database.GetDatabaseTableForeignKey(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	foreignCons := make(map[string]structure.ConstraintForeign)
	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableForeignConstraintDetail] postgres schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["CONSTRAINT_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] postgres schema [%s] table [%s] constraint_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["CONSTRAINT_NAME"], err)
		}
		constraintName := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["RCOLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableForeignConstraintDetail] postgres schema [%s] table [%s] r_column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["RCOLUMN_LIST"], err)
		}
		rColumnList := stringutil.BytesToString(utf8Raw)

		// column route
		var indexColumns, rIndexColumns []string
		for _, col := range stringutil.StringSplit(columnList, constant.StringSeparatorComplexSymbol) {
			if val, ok := p.ColumnRouteRules[col]; ok {
				indexColumns = append(indexColumns, val)
			} else {
				indexColumns = append(indexColumns, col)
			}
		}
		for _, col := range stringutil.StringSplit(rColumnList, constant.StringSeparatorComplexSymbol) {
			if val, ok := p.ColumnRouteRules[col]; ok {
				rIndexColumns = append(rIndexColumns, val)
			} else {
				rIndexColumns = append(rIndexColumns, col)
			}
		}

		foreignCons[constraintName] = structure.ConstraintForeign{
			ConstraintColumn:      stringutil.StringJoin(indexColumns, constant.StringSeparatorComplexSymbol),
			ReferencedTableSchema: c["R_OWNER"],
			ReferencedTableName:   c["RTABLE_NAME"],
			ReferencedColumnName:  stringutil.StringJoin(rIndexColumns, constant.StringSeparatorComplexSymbol),
			DeleteRule:            c["DELETE_RULE"],
			UpdateRule:            c["UPDATE_RULE"], // database oracle isn't supported update rule
		}
	}

	return foreignCons, nil
}

func (p *PostgresProcessor) GenDatabaseTableCheckConstraintDetail() (map[string]structure.ConstraintCheck, error) {
	infos, err := p.Database.GetDatabaseTableCheckKey(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	checkCons := make(map[string]structure.ConstraintCheck)
	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["CONSTRAINT_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] postgres schema [%s] table [%s] constraint_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["CONSTRAINT_NAME"], err)
		}
		constraintName := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["SEARCH_CONDITION"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableCheckConstraintDetail] postgres schema [%s] table [%s] search_condition [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["SEARCH_CONDITION"], err)
		}
		searchCondition := stringutil.BytesToString(utf8Raw)

		checkCons[constraintName] = structure.ConstraintCheck{ConstraintExpression: searchCondition}
	}

	return checkCons, nil
}

func (p *PostgresProcessor) GenDatabaseTablePartitionDetail() ([]structure.Partition, error) {
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
			PartitionKey:     c["PARTITION_EXPRESS"],
			PartitionType:    c["PARTITIONING_TYPE"],
			SubPartitionKey:  c["SUBPARTITION_EXPRESS"],
			SubPartitionType: c["SUBPARTITIONING_TYPE"],
		})
	}

	return partitions, nil
}

func (p *PostgresProcessor) genPostgresMappingMYSQLCompatibleDatabaseTableColumnCharsetCollationByNewColumn(columnName string, columnCharset, columnCollation string) (string, string, error) {
	var (
		charset   string
		collation string
	)

	if !strings.EqualFold(columnCharset, "UNKNOWN") || !strings.EqualFold(columnCharset, "") {
		if val, ok := constant.MigrateTableStructureDatabaseCharsetMap[p.TaskFlow][stringutil.StringUpper(columnCharset)]; ok {
			charset = val
		} else {
			return charset, collation, fmt.Errorf("the postgres compatible database schema [%s] table [%s] column [%s] charset [%v] table collation [%s] mapping failed", p.SchemaName, p.TableName, columnName, p.DBCharset, columnCollation)
		}
	} else {
		charset = "UNKNOWN"
	}

	if !strings.EqualFold(columnCollation, "UNKNOWN") || !strings.EqualFold(columnCollation, "") {
		var tempCharset string
		if strings.EqualFold(columnCharset, "UNKNOWN") {
			tempCharset = p.DBCharset
		} else {
			tempCharset = columnCharset
		}
		if val, ok := constant.MigrateTableStructureDatabaseCollationMap[p.TaskFlow][strings.ToUpper(columnCollation)][constant.MigrateTableStructureDatabaseCharsetMap[p.TaskFlow][stringutil.StringUpper(tempCharset)]]; ok {
			collation = val
		} else {
			return charset, collation, fmt.Errorf("the postgres compatible database schema [%s] table [%s] column [%s] charset [%v] table collation [%s] mapping failed", p.SchemaName, p.TableName, columnName, p.DBCharset, columnCollation)
		}
	} else {
		columnCollation = "UNKNOWN"
	}

	return charset, collation, nil
}
