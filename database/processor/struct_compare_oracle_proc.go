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

	"github.com/wentaojin/dbms/database/mapping"

	"github.com/wentaojin/dbms/model"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/model/buildin"
	"github.com/wentaojin/dbms/model/migrate"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/utils/stringutil"
	"github.com/wentaojin/dbms/utils/structure"
)

type OracleProcessor struct {
	Ctx                      context.Context                  `json:"-"`
	TaskName                 string                           `json:"taskName"`
	TaskFlow                 string                           `json:"taskFlow"`
	SchemaName               string                           `json:"schemaName"`
	TableName                string                           `json:"tableName"`
	DBCharset                string                           `json:"dbCharset"`
	DBCollation              bool                             `json:"dbCollation"`
	Database                 database.IDatabase               `json:"-"`
	BuildinDatatypeRules     []*buildin.BuildinDatatypeRule   `json:"-"`
	BuildinDefaultValueRules []*buildin.BuildinDefaultvalRule `json:"-"`
	ColumnRouteRules         map[string]string                `json:"columnRouteRules"`
	IsBaseline               bool                             `json:"isBaseline"`
	TableCollation           string                           `json:"tableCollation"`
	SchemaCollation          string                           `json:"schemaCollation"`
	NLSComp                  string                           `json:"NLSComp"`
}

func (p *OracleProcessor) GenDatabaseSchemaTable() (string, string) {
	return p.SchemaName, p.TableName
}

func (p *OracleProcessor) GenDatabaseTableCollation() (string, error) {
	if p.DBCollation {
		tableCollation, err := p.Database.GetDatabaseTableCollation(p.SchemaName, p.TableName)
		if err != nil {
			return "", err
		}
		if !strings.EqualFold(tableCollation, "") {
			p.TableCollation = stringutil.StringUpper(tableCollation)

			return stringutil.StringUpper(tableCollation), nil
		}
		schemaCollation, err := p.Database.GetDatabaseSchemaCollation(p.SchemaName)
		if err != nil {
			return "", err
		}
		if strings.EqualFold(tableCollation, "") && !strings.EqualFold(schemaCollation, "") {
			p.SchemaCollation = stringutil.StringUpper(schemaCollation)

			return stringutil.StringUpper(schemaCollation), nil
		}
		return "", fmt.Errorf("oracle database collation [%v] schema collation [%v] table collation [%v] isn't support by getColumnCollation", p.DBCollation, schemaCollation, tableCollation)
	} else {
		nlsComp, err := p.Database.GetDatabaseCollation()
		if err != nil {
			return "", err
		}
		p.NLSComp = stringutil.StringUpper(nlsComp)
		return stringutil.StringUpper(nlsComp), nil
	}
}

func (p *OracleProcessor) GenDatabaseTableCharset() (string, error) {
	return p.DBCharset, nil
}

func (p *OracleProcessor) GenDatabaseTableComment() (string, error) {
	comment, err := p.Database.GetDatabaseTableComment(p.SchemaName, p.TableName)
	if err != nil {
		return "", err
	}
	if len(comment) == 0 {
		return "", nil
	}
	return comment[0]["COMMENTS"], nil
}

func (p *OracleProcessor) GenDatabaseTableColumnDetail() (map[string]structure.NewColumn, map[string]map[string]structure.OldColumn, error) {
	infos, err := p.Database.GetDatabaseTableColumnInfo(p.SchemaName, p.TableName, p.DBCollation)
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
		columnNameUtf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenDatabaseTableColumnDetail] oracle schema [%s] table [%s] column [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_NAME"], err)
		}
		columnName := stringutil.BytesToString(columnNameUtf8Raw)

		defaultValUtf8Raw, err := stringutil.CharsetConvert([]byte(c["DATA_DEFAULT"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenDatabaseTableColumnDetail] oracle schema [%s] table [%s] column [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_NAME"], err)
		}
		columnDefaultValue := stringutil.BytesToString(defaultValUtf8Raw)

		commentUtf8Raw, err := stringutil.CharsetConvert([]byte(c["COMMENTS"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenDatabaseTableColumnDetail] oracle schema [%s] table [%s] column [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_NAME"], err)
		}
		columnComment := stringutil.BytesToString(commentUtf8Raw)

		var columnNameNew string
		if val, ok := p.ColumnRouteRules[columnName]; ok {
			columnNameNew = val
		} else {
			columnNameNew = columnName
		}

		if !p.IsBaseline {
			originColumnType, _, err := mapping.OracleDatabaseTableColumnMapMYSQLCompatibleDatatypeRule(&mapping.Column{
				ColumnName:    columnName,
				Datatype:      c["DATA_TYPE"],
				CharUsed:      c["CHAR_USED"],
				CharLength:    c["CHAR_LENGTH"],
				DataPrecision: c["DATA_PRECISION"],
				DataLength:    c["DATA_LENGTH"],
				DataScale:     c["DATA_SCALE"],
				DataDefault:   columnDefaultValue,
				Nullable:      c["NULLABLE"],
				Comment:       columnComment,
			}, p.BuildinDatatypeRules)
			if err != nil {
				return nil, nil, err
			}

			colDefaultVal, err := mapping.OracleHandleColumnRuleWitheDefaultValuePriority(columnName, columnDefaultValue, constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)],
				constant.CharsetUTF8MB4, nil, nil)
			if err != nil {
				return nil, nil, err
			}
			var columnCollation string
			if p.DBCollation {
				columnCollation = c["COLLATION"]
			} else {
				// the oracle database collation isn't support, it's set ""
				columnCollation = ""
			}

			originColumnCharset, originColumnCollation := p.genDatabaseTableColumnCharsetCollationByOldColumn(c["DATA_TYPE"], columnCollation)

			newColumns[columnNameNew] = structure.NewColumn{
				Datatype:    originColumnType,
				NULLABLE:    c["NULLABLE"],
				DataDefault: colDefaultVal,
				Charset:     originColumnCharset,
				Collation:   originColumnCollation,
				Comment:     columnComment,
			}

			oldColumns[columnNameNew] = map[string]structure.OldColumn{
				columnName: {
					DatatypeName:      c["DATA_TYPE"],
					DataLength:        c["DATA_LENGTH"],
					DataPrecision:     c["DATA_PRECISION"],
					DatetimePrecision: "", // only mysql compatible database
					DataScale:         c["DATA_SCALE"],
					NULLABLE:          c["NULLABLE"],
					DataDefault:       colDefaultVal,
					Charset:           originColumnCharset,
					Collation:         originColumnCollation,
					Comment:           columnComment,
				},
			}
		} else {
			// task flow
			switch {
			case strings.EqualFold(p.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(p.TaskFlow, constant.TaskFlowOracleToTiDB):
				originColumnType, buildInColumnType, err := mapping.OracleDatabaseTableColumnMapMYSQLCompatibleDatatypeRule(&mapping.Column{
					ColumnName:    columnName,
					Datatype:      c["DATA_TYPE"],
					CharUsed:      c["CHAR_USED"],
					CharLength:    c["CHAR_LENGTH"],
					DataPrecision: c["DATA_PRECISION"],
					DataLength:    c["DATA_LENGTH"],
					DataScale:     c["DATA_SCALE"],
					DataDefault:   columnDefaultValue,
					Nullable:      c["NULLABLE"],
					Comment:       columnComment,
				}, p.BuildinDatatypeRules)
				if err != nil {
					return nil, nil, err
				}
				// priority, return target database table column datatype
				convertColumnDatatype, convertColumnDefaultValue, err := mapping.OracleHandleColumnRuleWithPriority(
					p.TableName,
					columnName,
					originColumnType,
					buildInColumnType,
					columnDefaultValue,
					constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)],
					constant.CharsetUTF8MB4,
					p.BuildinDefaultValueRules,
					structTaskRules,
					structSchemaRules,
					structTableRules,
					structColumnRules)
				if err != nil {
					return nil, nil, err
				}

				var columnCollation string
				if p.DBCollation {
					columnCollation = c["COLLATION"]
				} else {
					// the oracle database collation isn't support, it's set ""
					columnCollation = ""
				}

				convColumnCharset, convColumnCollation, err := p.genDatabaseTableColumnCharsetCollationByNewColumn(columnName, c["DATA_TYPE"], columnCollation)
				if err != nil {
					return nil, nil, err
				}

				originColumnCharset, originColumnCollation := p.genDatabaseTableColumnCharsetCollationByOldColumn(c["DATA_TYPE"], columnCollation)

				// MYSQL compatible database integer column datatype exclude decimal "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT"
				if val, ok := constant.MYSQLCompatibleDatabaseTableIntegerColumnDatatypeExcludeDecimal[convertColumnDatatype]; ok {
					convertColumnDatatype = stringutil.StringBuilder(convertColumnDatatype, `(`, val, `)`)
				}

				newColumns[columnNameNew] = structure.NewColumn{
					Datatype:    convertColumnDatatype,
					NULLABLE:    c["NULLABLE"],
					DataDefault: convertColumnDefaultValue,
					Charset:     convColumnCharset,
					Collation:   convColumnCollation,
					Comment:     columnComment,
				}

				oldColumns[columnNameNew] = map[string]structure.OldColumn{
					columnName: {
						DatatypeName:      c["DATA_TYPE"],
						DataLength:        c["DATA_LENGTH"],
						DataPrecision:     c["DATA_PRECISION"],
						DatetimePrecision: "", // only mysql compatible database
						DataScale:         c["DATA_SCALE"],
						NULLABLE:          c["NULLABLE"],
						DataDefault:       convertColumnDefaultValue,
						Charset:           originColumnCharset,
						Collation:         originColumnCollation,
						Comment:           columnComment,
					},
				}
			default:
				return nil, nil, fmt.Errorf("oracle current task [%s] schema [%s] taskflow [%s] isn't support, please contact author", p.TaskName, p.SchemaName, p.TaskFlow)
			}
		}
	}

	return newColumns, oldColumns, nil
}

func (p *OracleProcessor) GenDatabaseTableIndexDetail() (map[string]structure.Index, error) {
	infos, err := p.Database.GetDatabaseTableNormalIndex(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}
	indexes := make(map[string]structure.Index, len(infos))

	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] oracle schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["INDEX_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] oracle schema [%s] table [%s] index_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["INDEX_NAME"], err)
		}
		indexName := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["ITYP_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] oracle schema [%s] table [%s] ityp_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["ITYP_NAME"], err)
		}
		itypName := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["PARAMETERS"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] oracle schema [%s] table [%s] parameters [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["PARAMETERS"], err)
		}
		parameters := stringutil.BytesToString(utf8Raw)

		// column route
		var indexColumns []string
		for _, col := range stringutil.StringSplit(columnList, ",") {
			if val, ok := p.ColumnRouteRules[col]; ok {
				indexColumns = append(indexColumns, val)
			} else {
				indexColumns = append(indexColumns, col)
			}
		}

		indexes[indexName] = structure.Index{
			IndexType:        c["INDEX_TYPE"],
			Uniqueness:       c["UNIQUENESS"],
			IndexColumn:      stringutil.StringJoin(indexColumns, ","),
			DomainIndexOwner: c["ITYP_OWNER"],
			DomainIndexName:  itypName,
			DomainParameters: parameters,
		}
	}

	infos, err = p.Database.GetDatabaseTableUniqueIndex(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] oracle schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["INDEX_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] oracle schema [%s] table [%s] index_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["INDEX_NAME"], err)
		}
		indexName := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["ITYP_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] oracle schema [%s] table [%s] ityp_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["ITYP_NAME"], err)
		}
		itypName := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["PARAMETERS"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] oracle schema [%s] table [%s] parameters [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["PARAMETERS"], err)
		}
		parameters := stringutil.BytesToString(utf8Raw)

		// column route
		var indexColumns []string
		for _, col := range stringutil.StringSplit(columnList, ",") {
			if val, ok := p.ColumnRouteRules[col]; ok {
				indexColumns = append(indexColumns, val)
			} else {
				indexColumns = append(indexColumns, col)
			}
		}

		indexes[indexName] = structure.Index{
			IndexType:        c["INDEX_TYPE"],
			Uniqueness:       c["UNIQUENESS"],
			IndexColumn:      stringutil.StringJoin(indexColumns, ","),
			DomainIndexOwner: c["ITYP_OWNER"],
			DomainIndexName:  itypName,
			DomainParameters: parameters,
		}
	}
	return indexes, nil
}

func (p *OracleProcessor) GenDatabaseTablePrimaryConstraintDetail() (map[string]structure.ConstraintPrimary, error) {
	infos, err := p.Database.GetDatabaseTablePrimaryKey(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	primaryCons := make(map[string]structure.ConstraintPrimary, len(infos))
	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTablePrimaryConstraintDetail] oracle schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["CONSTRAINT_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] oracle schema [%s] table [%s] constraint_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["CONSTRAINT_NAME"], err)
		}
		constraintName := stringutil.BytesToString(utf8Raw)

		// column route
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

func (p *OracleProcessor) GenDatabaseTableUniqueConstraintDetail() (map[string]structure.ConstraintUnique, error) {
	infos, err := p.Database.GetDatabaseTableUniqueKey(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	uniqueCons := make(map[string]structure.ConstraintUnique)
	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableUniqueConstraintDetail] oracle schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["CONSTRAINT_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] oracle schema [%s] table [%s] constraint_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["CONSTRAINT_NAME"], err)
		}
		constraintName := stringutil.BytesToString(utf8Raw)

		// column route
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

func (p *OracleProcessor) GenDatabaseTableForeignConstraintDetail() (map[string]structure.ConstraintForeign, error) {
	infos, err := p.Database.GetDatabaseTableForeignKey(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	foreignCons := make(map[string]structure.ConstraintForeign)
	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableForeignConstraintDetail] oracle schema [%s] table [%s] column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["COLUMN_LIST"], err)
		}
		columnList := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["CONSTRAINT_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] oracle schema [%s] table [%s] constraint_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["CONSTRAINT_NAME"], err)
		}
		constraintName := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["RCOLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableForeignConstraintDetail] oracle schema [%s] table [%s] r_column_list [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["RCOLUMN_LIST"], err)
		}
		rColumnList := stringutil.BytesToString(utf8Raw)

		// column route
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
			UpdateRule:            "", // database oracle isn't supported update rule
		}
	}

	return foreignCons, nil
}

func (p *OracleProcessor) GenDatabaseTableCheckConstraintDetail() (map[string]structure.ConstraintCheck, error) {
	infos, err := p.Database.GetDatabaseTableCheckKey(p.SchemaName, p.TableName)
	if err != nil {
		return nil, err
	}

	checkCons := make(map[string]structure.ConstraintCheck)
	for _, c := range infos {
		utf8Raw, err := stringutil.CharsetConvert([]byte(c["CONSTRAINT_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableIndexDetail] oracle schema [%s] table [%s] constraint_name [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["CONSTRAINT_NAME"], err)
		}
		constraintName := stringutil.BytesToString(utf8Raw)

		utf8Raw, err = stringutil.CharsetConvert([]byte(c["SEARCH_CONDITION"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(p.DBCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenDatabaseTableCheckConstraintDetail] oracle schema [%s] table [%s] search_condition [%s] charset [%v] convert [UTFMB4] failed, error: %v", p.SchemaName, p.TableName, p.DBCharset, c["SEARCH_CONDITION"], err)
		}
		searchCondition := stringutil.BytesToString(utf8Raw)

		checkCons[constraintName] = structure.ConstraintCheck{ConstraintExpression: searchCondition}
	}

	return checkCons, nil
}

func (p *OracleProcessor) GenDatabaseTablePartitionDetail() ([]structure.Partition, error) {
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

func (p *OracleProcessor) genDatabaseTableColumnCharsetCollationByNewColumn(columnName, datatype, columnCollation string) (string, string, error) {
	var charset, collation string
	// the oracle 12.2 and the above version support column collation
	if p.DBCollation {
		// check column sort collation
		if val, ok := constant.MigrateTableStructureDatabaseCollationMap[p.TaskFlow][strings.ToUpper(columnCollation)][constant.MigrateTableStructureDatabaseCharsetMap[p.TaskFlow][stringutil.StringUpper(p.DBCharset)]]; ok {
			collation = val
			charset = constant.MigrateTableStructureDatabaseCharsetMap[p.TaskFlow][p.DBCharset]
		} else {
			// exclude the column with the integer datatype
			if !strings.EqualFold(columnCollation, "") {
				return charset, collation, fmt.Errorf("the oracle schema [%s] table [%s] column [%s] charset [%v] collation [%s] check failed", p.SchemaName, p.TableName, columnName, p.DBCharset, columnCollation)
			}
			charset = "UNKNOWN"
			collation = "UNKNOWN"
		}
	} else {
		// the oracle 12.2 and the below version isn't support column collation, and set "UNKNOWN"
		switch datatype {
		case constant.BuildInOracleDatatypeCharacter, constant.BuildInOracleDatatypeLong, constant.BuildInOracleDatatypeNcharVarying,
			constant.BuildInOracleDatatypeVarchar, constant.BuildInOracleDatatypeChar, constant.BuildInOracleDatatypeNchar, constant.BuildInOracleDatatypeVarchar2, constant.BuildInOracleDatatypeNvarchar2:
			charset = constant.MigrateTableStructureDatabaseCharsetMap[p.TaskFlow][p.DBCharset]
			if strings.EqualFold(columnCollation, "") && !strings.EqualFold(p.TableCollation, "") {
				if val, ok := constant.MigrateTableStructureDatabaseCollationMap[p.TaskFlow][strings.ToUpper(p.TableCollation)][charset]; ok {
					collation = val
				} else {
					return charset, collation, fmt.Errorf("the oracle schema [%s] table [%s] column [%s] charset [%v] table collation [%s] mapping failed", p.SchemaName, p.TableName, columnName, p.DBCharset, columnCollation)
				}
			} else if strings.EqualFold(columnCollation, "") && strings.EqualFold(p.TableCollation, "") && !strings.EqualFold(p.SchemaCollation, "") {
				if val, ok := constant.MigrateTableStructureDatabaseCollationMap[p.TaskFlow][strings.ToUpper(p.SchemaCollation)][charset]; ok {
					collation = val
				} else {
					return charset, collation, fmt.Errorf("the oracle schema [%s] table [%s] column [%s] charset [%v] schema collation [%s] mapping failed", p.SchemaName, p.TableName, columnName, p.DBCharset, columnCollation)
				}
			} else {
				if val, ok := constant.MigrateTableStructureDatabaseCollationMap[p.TaskFlow][strings.ToUpper(p.NLSComp)][charset]; ok {
					collation = val
				} else {
					return charset, collation, fmt.Errorf("the oracle schema [%s] table [%s] column [%s] charset [%v] nlscomp collation [%s] mapping failed", p.SchemaName, p.TableName, columnName, p.DBCharset, columnCollation)
				}
			}
		default:
			charset = "UNKNOWN"
			collation = "UNKNOWN"
		}
	}
	return charset, collation, nil
}

func (p *OracleProcessor) genDatabaseTableColumnCharsetCollationByOldColumn(datatype string, columnCollation string) (string, string) {
	if p.DBCollation {
		if !strings.EqualFold(columnCollation, "") {
			return p.DBCharset, columnCollation
		} else {
			// exclude the column with the integer datatype
			return "UNKNOWN", "UNKNOWN"
		}
	} else {
		// the oracle 12.2 and the below version isn't support column collation, and set "UNKNOWN"
		switch datatype {
		case constant.BuildInOracleDatatypeCharacter, constant.BuildInOracleDatatypeLong, constant.BuildInOracleDatatypeNcharVarying,
			constant.BuildInOracleDatatypeVarchar, constant.BuildInOracleDatatypeChar, constant.BuildInOracleDatatypeNchar, constant.BuildInOracleDatatypeVarchar2, constant.BuildInOracleDatatypeNvarchar2:
			if strings.EqualFold(columnCollation, "") && !strings.EqualFold(p.TableCollation, "") {
				return p.DBCharset, p.TableCollation
			}
			if strings.EqualFold(columnCollation, "") && strings.EqualFold(p.TableCollation, "") && !strings.EqualFold(p.SchemaCollation, "") {
				return p.DBCharset, p.SchemaCollation
			}
			return p.DBCharset, p.NLSComp
		default:
			return "UNKNOWN", "UNKNOWN"
		}
	}
}
