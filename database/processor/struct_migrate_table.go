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
	"fmt"
	"regexp"
	"strings"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

type StructMigrateTable struct {
	TaskName            string                                `json:"taskName"`
	TaskFlow            string                                `json:"taskFlow"`
	DatasourceS         *Datasource                           `json:"datasourceS"`
	TableAttributes     *database.StructMigrateAttributes     `json:"tableAttributes"`
	TableAttributesRule *database.StructMigrateAttributesRule `json:"tableAttributesRule"`
	DBCharsetT          string                                `json:"DBCharsetT"`
}

func (t *StructMigrateTable) GenSchemaNameS() string {
	return t.DatasourceS.SchemaNameS
}

func (t *StructMigrateTable) GenTableNameS() string {
	return t.DatasourceS.TableNameS
}

func (t *StructMigrateTable) GenTableTypeS() string {
	return t.DatasourceS.TableTypeS
}

func (t *StructMigrateTable) GenTableOriginDDlS() string {
	return t.TableAttributes.OriginStruct
}

func (t *StructMigrateTable) GenTableCreatePrefixT() string {
	return t.TableAttributesRule.CreatePrefixRule
}

func (t *StructMigrateTable) GenSchemaNameT() (string, error) {
	var schemaName string
	if val, ok := t.TableAttributesRule.SchemaNameRule[t.DatasourceS.SchemaNameS]; ok {
		schemaName = val
		return schemaName, nil
	} else {
		return "", fmt.Errorf("[GenSchemaNameT] the upstream database schema [%v] isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS)
	}
}

func (t *StructMigrateTable) GenTableNameT() (string, error) {
	var tableName string
	if val, ok := t.TableAttributesRule.TableNameRule[t.DatasourceS.TableNameS]; ok {
		tableName = val
		return tableName, nil
	} else {
		return "", fmt.Errorf("[GenTableNameT] the upstream database schema [%v] table [%v] isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS)
	}
}

func (t *StructMigrateTable) GenTableSuffix() (string, error) {
	var tableCharset, tableCollation, tableSuffix string

	if val, ok := constant.MigrateTableStructureDatabaseCharsetMap[t.TaskFlow][t.TableAttributes.TableCharset]; ok {
		tableCharset = val
	} else {
		return tableSuffix, fmt.Errorf("[GenTableSuffix] the upstream database schema table charset [%v] isn't support mapping target database charset", t.TableAttributes.TableCharset)
	}

	if val, ok := constant.MigrateTableStructureDatabaseCollationMap[t.TaskFlow][t.TableAttributes.TableCollation][tableCharset]; ok {
		tableCollation = val
	} else {
		return tableSuffix, fmt.Errorf("[GenTableSuffix] the upstream database schema table charset [%v] collation [%v] isn't support mapping target database charset [%v]", t.TableAttributes.TableCharset, t.TableAttributes.TableCollation, tableCharset)
	}

	switch t.TaskFlow {
	case constant.TaskFlowOracleToMySQL:
		tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s", tableCharset, tableCollation)
		return tableSuffix, nil
	case constant.TaskFlowOracleToTiDB:
		if strings.EqualFold(t.TableAttributesRule.TableAttrRule, "") {
			tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s", tableCharset, tableCollation)
		} else {
			tableSuffix = fmt.Sprintf("ENGINE=InnoDB DEFAULT CHARSET=%s COLLATE=%s %s", tableCharset, tableCollation, stringutil.StringUpper(t.TableAttributesRule.TableAttrRule))
		}
		return tableSuffix, nil
	default:
		return tableSuffix, fmt.Errorf("[GenTableSuffix] oracle current taskflow [%s] isn't support, please contact author or reselect", t.TaskFlow)
	}
}

func (t *StructMigrateTable) GenTablePrimaryKey() (string, error) {
	var primaryKey string
	if len(t.TableAttributes.PrimaryKey) > 1 {
		return primaryKey, fmt.Errorf("[GenTablePrimaryKey] the upstream database schema schema [%s] table [%s] primary key exist multiple values: [%v]", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.TableAttributes.PrimaryKey)
	}
	if len(t.TableAttributes.PrimaryKey) == 1 {
		var (
			columnList     string
			primaryColumns []string
		)

		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(t.TableAttributes.PrimaryKey[0]["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return "", fmt.Errorf("[GenTablePrimaryKey] the upstream database schema [%s] table [%s] primary key [%s] charset convert failed, %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.TableAttributes.PrimaryKey[0]["COLUMN_LIST"], err)
		}
		columnList = stringutil.BytesToString(convertUtf8Raw)

		primaryColumns = strings.Split(columnList, ",")

		switch t.TaskFlow {
		case constant.TaskFlowOracleToTiDB:
			var (
				pkColumns   []string
				pkColumnStr string
			)
			for _, s := range primaryColumns {
				if val, ok := t.TableAttributesRule.ColumnNameRule[s]; ok {
					convertTargetRaw, err := stringutil.CharsetConvert([]byte(val), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
					if err != nil {
						return "", fmt.Errorf("[GenTablePrimaryKey] the upstream database schema [%s] table [%s] primary charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
					}
					pkColumns = append(pkColumns, fmt.Sprintf("`%s`", stringutil.BytesToString(convertTargetRaw)))
				} else {
					return "", fmt.Errorf("[GenTablePrimaryKey] the upstream database schema [%v] table [%v] column [%v] isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, s)
				}
			}
			pkColumnStr = strings.Join(pkColumns, ",")

			// clustered index table
			// single primary column and the column is integer
			if len(primaryColumns) == 1 {
				singleIntegerPK := false
				// get single primary column datatype
				for columnName, columnDatatype := range t.TableAttributesRule.ColumnDatatypeRule {
					if primaryColumns[0] == columnName {
						// offer the column datatype after converting by the column datatype rule,
						for _, integerType := range constant.TiDBDatabaseIntegerColumnDatatypePrimaryKey {
							if find := strings.Contains(stringutil.StringUpper(columnDatatype), stringutil.StringUpper(integerType)); find {
								singleIntegerPK = true
							}
						}
					}
				}
				// if the table primary key is integer datatype, and the table attr rule can't null (shard_row_id_bits)
				// it represents the primary key need set NONCLUSTERED attributes
				if singleIntegerPK && !strings.EqualFold(t.TableAttributesRule.TableAttrRule, "") {
					primaryKey = fmt.Sprintf("PRIMARY KEY (%s) NONCLUSTERED", pkColumnStr)
				} else if singleIntegerPK && strings.EqualFold(t.TableAttributesRule.TableAttrRule, "") {
					primaryKey = fmt.Sprintf("PRIMARY KEY (%s) CLUSTERED", pkColumnStr)
				} else {
					// otherwise，depend on the tidb database params tidb_enable_clustered_index
					primaryKey = fmt.Sprintf("PRIMARY KEY (%s)", pkColumnStr)
				}
			} else {
				// otherwise，depend on the tidb database params tidb_enable_clustered_index
				primaryKey = fmt.Sprintf("PRIMARY KEY (%s)", pkColumnStr)
			}

		case constant.TaskFlowOracleToMySQL:
			var (
				pkColumns   []string
				pkColumnStr string
			)
			for _, s := range primaryColumns {
				if val, ok := t.TableAttributesRule.ColumnNameRule[s]; ok {
					convertTargetRaw, err := stringutil.CharsetConvert([]byte(val), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
					if err != nil {
						return "", fmt.Errorf("[GenTablePrimaryKey] the upstream database schema [%s] table [%s] primary charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
					}
					pkColumns = append(pkColumns, fmt.Sprintf("`%s`", stringutil.BytesToString(convertTargetRaw)))
				} else {
					return "", fmt.Errorf("[GenTablePrimaryKey] the upstream database schema [%v] table [%v] column [%v] isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, s)
				}
			}
			pkColumnStr = strings.Join(pkColumns, ",")

			primaryKey = fmt.Sprintf("PRIMARY KEY (%s)", pkColumnStr)
		default:
			return primaryKey, fmt.Errorf("[GenTablePrimaryKey] oracle current taskflow [%s] isn't support, please contact author or reselect", t.TaskFlow)
		}
	}
	return primaryKey, nil
}

func (t *StructMigrateTable) GenTableUniqueKey() ([]string, error) {
	var uniqueKeys []string
	for _, rowUKCol := range t.TableAttributes.UniqueKey {
		var (
			columnList string
			consName   string
		)

		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(rowUKCol["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenTableUniqueKey] the upstream database schema [%s] table [%s] unique charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
		}
		columnList = stringutil.BytesToString(convertUtf8Raw)

		convertUtf8Raw, err = stringutil.CharsetConvert([]byte(rowUKCol["CONSTRAINT_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenTableUniqueKey] the upstream database schema [%s] table [%s] constraint [%s] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, rowUKCol["CONSTRAINT_NAME"], t.DBCharsetT, err)
		}
		consName = stringutil.BytesToString(convertUtf8Raw)

		switch t.TaskFlow {
		case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
			convertTargetRaw, err := stringutil.CharsetConvert([]byte(consName), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
			if err != nil {
				return nil, fmt.Errorf("[GenTableUniqueKey] the upstream database schema [%s] table [%s] unique charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
			}
			consName = stringutil.BytesToString(convertTargetRaw)

			var ukColumns []string

			for _, s := range strings.Split(columnList, ",") {
				if val, ok := t.TableAttributesRule.ColumnNameRule[s]; ok {
					convertTargetRaw, err = stringutil.CharsetConvert([]byte(val), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
					if err != nil {
						return nil, fmt.Errorf("[GenTableUniqueKey] the upstream database schema [%s] table [%s] unique charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
					}
					ukColumns = append(ukColumns, fmt.Sprintf("`%s`", stringutil.BytesToString(convertTargetRaw)))
				} else {
					return nil, fmt.Errorf("[GenTableUniqueKey] the upstream database schema [%v] table [%v] column [%v] isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, s)
				}
			}
			uniqueKeys = append(uniqueKeys, fmt.Sprintf("UNIQUE KEY `%s` (%s)", consName, strings.Join(ukColumns, ",")))
		default:
			return uniqueKeys, fmt.Errorf("[GenTableUniqueKey] oracle current taskflow [%s] isn't support, please contact author or reselect", t.TaskFlow)
		}
	}
	return uniqueKeys, nil
}

func (t *StructMigrateTable) GenTableForeignKey() ([]string, error) {
	var (
		foreignKeys []string
		fk          string
		columnList  string
		rOwner      string
		rTable      string
		rColumnList string
		consName    string
	)
	for _, rowFKCol := range t.TableAttributes.ForeignKey {
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(rowFKCol["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenTableForeignKey] the upstream database schema [%s] table [%s] foreign column_list charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
		}
		columnList = stringutil.BytesToString(convertUtf8Raw)

		convertUtf8Raw, err = stringutil.CharsetConvert([]byte(rowFKCol["R_OWNER"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenTableForeignKey] the upstream database schema [%s] table [%s] foreign r_owner charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
		}
		rOwner = stringutil.BytesToString(convertUtf8Raw)

		convertUtf8Raw, err = stringutil.CharsetConvert([]byte(rowFKCol["RTABLE_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenTableForeignKey] the upstream database schema [%s] table [%s] foreign rtable_name charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
		}
		rTable = stringutil.BytesToString(convertUtf8Raw)

		convertUtf8Raw, err = stringutil.CharsetConvert([]byte(rowFKCol["RCOLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenTableForeignKey] the upstream database schema [%s] table [%s] foreign rcolumn_list charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
		}
		rColumnList = stringutil.BytesToString(convertUtf8Raw)

		convertUtf8Raw, err = stringutil.CharsetConvert([]byte(rowFKCol["CONSTRAINT_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenTableForeignKey] the upstream database schema [%s] table [%s] foreign constraint_name charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
		}
		consName = stringutil.BytesToString(convertUtf8Raw)

		if strings.EqualFold(t.TableAttributesRule.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleLower) {
			rOwner = strings.ToLower(rOwner)
			rTable = strings.ToLower(rTable)
			consName = strings.ToLower(consName)
		}
		if strings.EqualFold(t.TableAttributesRule.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
			rOwner = strings.ToUpper(rOwner)
			rTable = strings.ToUpper(rTable)
			consName = strings.ToUpper(consName)
		}

		switch t.TaskFlow {
		case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
			var (
				fkColumns  []string
				rfkColumns []string
			)
			for _, s := range strings.Split(columnList, ",") {
				if val, ok := t.TableAttributesRule.ColumnNameRule[s]; ok {
					convertTargetRaw, err := stringutil.CharsetConvert([]byte(val), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
					if err != nil {
						return nil, fmt.Errorf("[GenTableForeignKey] the upstream database schema [%s] table [%s] foreign column_list charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
					}

					fkColumns = append(fkColumns, fmt.Sprintf("`%s`", stringutil.BytesToString(convertTargetRaw)))
				} else {
					return nil, fmt.Errorf("[GenTableForeignKey] the upstream database schema [%v] table [%v] column [%v] isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, s)
				}
			}
			columnList = strings.Join(fkColumns, ",")

			for _, s := range strings.Split(rColumnList, ",") {
				if val, ok := t.TableAttributesRule.ColumnNameRule[s]; ok {
					convertTargetRaw, err := stringutil.CharsetConvert([]byte(val), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
					if err != nil {
						return nil, fmt.Errorf("[GenTableForeignKey] the upstream database schema [%s] table [%s] foreign rcolumn_list charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
					}
					rfkColumns = append(rfkColumns, fmt.Sprintf("`%s`", stringutil.BytesToString(convertTargetRaw)))
				} else {
					return nil, fmt.Errorf("[GenTableForeignKey] the upstream database schema [%v] table [%v] column [%v] isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, s)
				}
			}
			rColumnList = strings.Join(rfkColumns, ",")

			convertTargetRaw, err := stringutil.CharsetConvert([]byte(rOwner), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
			if err != nil {
				return nil, fmt.Errorf("[GenTableForeignKey] the upstream database schema [%s] table [%s] foreign r_owner charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
			}
			rOwner = stringutil.BytesToString(convertTargetRaw)
			convertTargetRaw, err = stringutil.CharsetConvert([]byte(rTable), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
			if err != nil {
				return nil, fmt.Errorf("[GenTableForeignKey] the upstream database schema [%s] table [%s] foreign rtable_name charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
			}
			rTable = stringutil.BytesToString(convertTargetRaw)
			convertTargetRaw, err = stringutil.CharsetConvert([]byte(consName), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
			if err != nil {
				return nil, fmt.Errorf("[GenTableForeignKey] the upstream database schema [%s] table [%s] foreign constraint_name charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
			}
			consName = fmt.Sprintf("%s", stringutil.BytesToString(convertTargetRaw))

			if rowFKCol["DELETE_RULE"] == "" || rowFKCol["DELETE_RULE"] == "NO ACTION" {
				fk = fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`.`%s` (%s)",
					consName,
					columnList,
					rOwner,
					rTable,
					rColumnList)
			}
			if rowFKCol["DELETE_RULE"] == "CASCADE" {
				fk = fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`.`%s`(%s) ON DELETE CASCADE",
					consName,
					columnList,
					rOwner,
					rTable,
					rColumnList)
			}
			if rowFKCol["DELETE_RULE"] == "SET NULL" {
				fk = fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY(%s) REFERENCES `%s`.`%s`(%s) ON DELETE SET NULL",
					consName,
					columnList,
					rOwner,
					rTable,
					rColumnList)
			}
			foreignKeys = append(foreignKeys, fk)
		default:
			return foreignKeys, fmt.Errorf("[GenTableForeignKey] oracle current taskflow [%s] isn't support, please contact author or reselect", t.TaskFlow)
		}
	}

	return foreignKeys, nil
}

func (t *StructMigrateTable) GenTableCheckKey() ([]string, error) {
	var checkKeys []string
	// check constraint match
	// example："LOC" IS noT nUll and loc in ('a','b','c')
	reg, err := regexp.Compile(`\s+(?i:AND)\s+|\s+(?i:OR)\s+`)
	if err != nil {
		return checkKeys, fmt.Errorf("the upstream database schema [%v] table [%v] check constraint regexp [AND/OR] failed: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, err)
	}

	matchRex, err := regexp.Compile(`(^.*)(?i:IS NOT NULL)`)
	if err != nil {
		return checkKeys, fmt.Errorf("the upstream database schema [%v] table [%v] check constraint regexp match [IS NOT NULL] failed: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, err)
	}

	checkRex, err := regexp.Compile(`(.*)(?i:IS NOT NULL)`)
	if err != nil {
		fmt.Printf("\"the upstream database schema [%v] table [%v] check constraint regexp check [IS NOT NULL] failed: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, err)
	}

	for _, rowCKCol := range t.TableAttributes.CheckKey {
		var (
			searchCond     string
			constraintName string
		)
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(rowCKCol["SEARCH_CONDITION"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenTableCheckKey] the upstream database schema [%s] table [%s] check search_condition charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
		}
		searchCond = stringutil.BytesToString(convertUtf8Raw)

		convertUtf8Raw, err = stringutil.CharsetConvert([]byte(rowCKCol["CONSTRAINT_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenTableCheckKey] the upstream database schema [%s] table [%s] check constraint_name charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
		}
		constraintName = stringutil.BytesToString(convertUtf8Raw)

		// match replace
		for _, rowCol := range t.TableAttributes.TableColumns {
			var columnName string
			convertUtf8Raw, err = stringutil.CharsetConvert([]byte(rowCol["COLUMN_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
			if err != nil {
				return nil, fmt.Errorf("[GenTableCheckKey] the upstream database schema [%s] table [%s] check column_name charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
			}
			columnName = stringutil.BytesToString(convertUtf8Raw)

			replaceRex, err := regexp.Compile(fmt.Sprintf("(?i)%v", columnName))
			if err != nil {
				return nil, err
			}
			if val, ok := t.TableAttributesRule.ColumnNameRule[columnName]; ok {
				searchCond = replaceRex.ReplaceAllString(searchCond, val)
				if strings.EqualFold(t.TableAttributesRule.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleLower) {
					constraintName = strings.ToLower(constraintName)
				}
				if strings.EqualFold(t.TableAttributesRule.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
					constraintName = strings.ToUpper(constraintName)
				}
			} else {
				return nil, fmt.Errorf("[GenTableCheckKey] the upstream database schema [%v] table [%v] column [%v] isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, columnName)
			}
		}

		// exclude not null constraint
		s := strings.TrimSpace(searchCond)

		if !reg.MatchString(s) {
			if !matchRex.MatchString(s) {
				switch t.TaskFlow {
				case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
					convertTargetRaw, err := stringutil.CharsetConvert([]byte(constraintName), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
					if err != nil {
						return nil, fmt.Errorf("[GenTableCheckKey] the upstream database schema [%s] table [%s] check constraint_name one charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
					}
					constraintName = stringutil.BytesToString(convertTargetRaw)
					convertTargetRaw, err = stringutil.CharsetConvert([]byte(searchCond), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
					if err != nil {
						return nil, fmt.Errorf("[GenTableCheckKey] the upstream database schema [%s] table [%s] check constraint_name one charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
					}
					searchCond = stringutil.BytesToString(convertTargetRaw)
					checkKeys = append(checkKeys, fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)",
						constraintName,
						searchCond))
				default:
					return checkKeys, fmt.Errorf("[GenTableCheckKey] oracle current taskflow [%s] generate table suffix isn't support, please contact author or reselect", t.TaskFlow)
				}
			}
		} else {
			strArray := strings.Fields(s)
			var (
				idxArray        []int
				checkArray      []string
				constraintArray []string
			)
			for idx, val := range strArray {
				if strings.EqualFold(val, "AND") || strings.EqualFold(val, "OR") {
					idxArray = append(idxArray, idx)
				}
			}

			idxArray = append(idxArray, len(strArray))

			for idx, val := range idxArray {
				if idx == 0 {
					checkArray = append(checkArray, strings.Join(strArray[0:val], " "))
				} else {
					checkArray = append(checkArray, strings.Join(strArray[idxArray[idx-1]:val], " "))
				}
			}

			for _, val := range checkArray {
				v := strings.TrimSpace(val)
				if !checkRex.MatchString(v) {
					constraintArray = append(constraintArray, v)
				}
			}

			sd := strings.Join(constraintArray, " ")
			d := strings.Fields(sd)

			if strings.EqualFold(d[0], "AND") || strings.EqualFold(d[0], "OR") {
				d = d[1:]
			}
			if strings.EqualFold(d[len(d)-1], "AND") || strings.EqualFold(d[len(d)-1], "OR") {
				d = d[:len(d)-1]
			}

			switch t.TaskFlow {
			case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
				convertTargetRaw, err := stringutil.CharsetConvert([]byte(constraintName), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
				if err != nil {
					return nil, fmt.Errorf("[GenTableCheckKey] the upstream database schema [%s] table [%s] check constraint_name two charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
				}
				constraintName = stringutil.BytesToString(convertTargetRaw)

				checkCond := strings.Join(d, " ")
				convertTargetRaw, err = stringutil.CharsetConvert([]byte(checkCond), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
				if err != nil {
					return nil, fmt.Errorf("[GenTableCheckKey] the upstream database schema [%s] table [%s] check constraint_name one charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.DBCharsetT, err)
				}
				checkCond = stringutil.BytesToString(convertTargetRaw)
				checkKeys = append(checkKeys, fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)",
					constraintName,
					checkCond))
			default:
				return checkKeys, fmt.Errorf("[GenTableCheckKey] oracle current taskflow [%s] isn't support, please contact author or reselect", t.TaskFlow)
			}
		}
	}

	return checkKeys, nil
}

func (t *StructMigrateTable) GenTableUniqueIndex() ([]string, []string, error) {
	var (
		uniqueIndexes         []string
		compatibilityIndexSql []string
	)
	schemaName, err := t.GenSchemaNameT()
	if err != nil {
		return nil, nil, err
	}
	tableName, err := t.GenTableNameT()
	if err != nil {
		return nil, nil, err
	}

	for _, idxMeta := range t.TableAttributes.UniqueIndex {
		var (
			columnList string
			indexName  string
			indexOwner string
		)
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(idxMeta["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenTableUniqueIndex] the upstream database schema [%s] table [%s] column [%s] unique_index charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["COLUMN_LIST"], t.DBCharsetT, err)
		}
		columnList = stringutil.BytesToString(convertUtf8Raw)

		convertUtf8Raw, err = stringutil.CharsetConvert([]byte(idxMeta["INDEX_OWNER"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenTableUniqueIndex] the upstream database schema [%s] table [%s] column [%s] index_name [%s] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["COLUMN_LIST"], idxMeta["INDEX_NAME"], t.DBCharsetT, err)
		}
		indexOwner = stringutil.BytesToString(convertUtf8Raw)

		convertUtf8Raw, err = stringutil.CharsetConvert([]byte(idxMeta["INDEX_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenTableUniqueIndex] the upstream database schema [%s] table [%s] column [%s] index_name [%s] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["COLUMN_LIST"], idxMeta["INDEX_NAME"], t.DBCharsetT, err)
		}
		indexName = stringutil.BytesToString(convertUtf8Raw)

		// Note:
		// additional processing of the same index name in different schemas
		// automatically change index names for index names that are not under the current schema，for example:
		// the migrate schema is marvin00, index_names list: marvin00.uniq_marvin, marvin01.uniq_marvin, then marvin01.uniq_marvin -> marvin00.uniq_marvin_marvin01
		if !strings.EqualFold(indexOwner, t.DatasourceS.SchemaNameS) {
			indexName = stringutil.StringBuilder(indexName, "_ow_", indexOwner)
		}

		if strings.EqualFold(t.TableAttributesRule.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleLower) {
			indexName = strings.ToLower(indexName)
		}
		if strings.EqualFold(t.TableAttributesRule.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
			indexName = strings.ToUpper(indexName)
		}

		switch t.TaskFlow {
		case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
			convertTargetRaw, err := stringutil.CharsetConvert([]byte(indexName), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
			if err != nil {
				return nil, nil, fmt.Errorf("[GenTableUniqueIndex] the upstream database schema [%s] table [%s] column [%s] index_name [%s] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["COLUMN_LIST"], idxMeta["INDEX_NAME"], t.DBCharsetT, err)
			}
			indexName = stringutil.BytesToString(convertTargetRaw)

			// Function Index: SUBSTR("NAME8",1,8)|+|NAME9
			ukColumns := strings.Split(columnList, "|+|")
			for i, s := range ukColumns {
				// SUBSTR("NAME8",1,8) OR NAME9
				brackets := stringutil.StringExtractorWithinBrackets(s)
				if len(brackets) == 0 {
					// NAME9 PART
					if val, ok := t.TableAttributesRule.ColumnNameRule[s]; ok {
						convertTargetRaw, err = stringutil.CharsetConvert([]byte(val), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
						if err != nil {
							return nil, nil, fmt.Errorf("[GenTableUniqueIndex] the upstream database schema [%s] table [%s] column [%s] normal_index charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["COLUMN_LIST"], t.DBCharsetT, err)
						}
						ukColumns[i] = fmt.Sprintf("`%s`", stringutil.BytesToString(convertTargetRaw))
					} else {
						return nil, nil, fmt.Errorf("[GenTableUniqueIndex] the upstream database schema [%v] table [%v] column [%v] isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, s)
					}
				} else {
					for k, v := range t.TableAttributesRule.ColumnNameRule {
						if stringutil.StringMatcher(s, "\""+k+"\"") {
							convertTargetRaw, err = stringutil.CharsetConvert([]byte(stringutil.StringReplacer(s, "\""+k+"\"", fmt.Sprintf("`%s`", v))), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
							if err != nil {
								return nil, nil, fmt.Errorf("[GenTableUniqueIndex] the upstream database schema [%s] table [%s] column [%s] index_name [%s] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["COLUMN_LIST"], s, t.DBCharsetT, err)
							}
							ukColumns[i] = stringutil.BytesToString(convertTargetRaw)
						}
					}
				}
			}
			columnList = strings.Join(ukColumns, ",")

			if idxMeta["TABLE_NAME"] != "" && strings.EqualFold(idxMeta["UNIQUENESS"], "UNIQUE") {
				switch strings.ToUpper(idxMeta["INDEX_TYPE"]) {
				case "NORMAL":
					uniqueIndexes = append(uniqueIndexes, fmt.Sprintf("UNIQUE INDEX `%s` (%s)", indexName, columnList))
					continue

				case "FUNCTION-BASED NORMAL":
					sqlStr := fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s` (%s);", indexName, schemaName, tableName, columnList)
					compatibilityIndexSql = append(compatibilityIndexSql, sqlStr)

					zap.L().Warn("reverse unique key",
						zap.String("task_name", t.TaskName),
						zap.String("task_flow", t.TaskFlow),
						zap.String("schema_name_s", t.DatasourceS.SchemaNameS),
						zap.String("table_name_s", idxMeta["TABLE_NAME"]),
						zap.String("index_name_s", idxMeta["INDEX_NAME"]),
						zap.String("index_type_s", idxMeta["INDEX_TYPE"]),
						zap.String("index_column_list_s", idxMeta["COLUMN_LIST"]),
						zap.String("create_sql", sqlStr),
						zap.String("warn", "database not support"))
					continue
				case "NORMAL/REV":
					sqlStr := fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON %s.%s (%s) REVERSE;", indexName, schemaName, tableName, columnList)
					compatibilityIndexSql = append(compatibilityIndexSql, sqlStr)

					zap.L().Warn("reverse unique key",
						zap.String("task_name", t.TaskName),
						zap.String("task_flow", t.TaskFlow),
						zap.String("schema_name_s", t.DatasourceS.SchemaNameS),
						zap.String("table_name_s", idxMeta["TABLE_NAME"]),
						zap.String("index_name_s", idxMeta["INDEX_NAME"]),
						zap.String("index_type_s", idxMeta["INDEX_TYPE"]),
						zap.String("index_column_list_s", idxMeta["COLUMN_LIST"]),
						zap.String("create_sql", sqlStr),
						zap.String("warn", "database not support"))
					continue

				default:
					zap.L().Error("reverse unique index",
						zap.String("task_name", t.TaskName),
						zap.String("task_flow", t.TaskFlow),
						zap.String("schema_name_s", t.DatasourceS.SchemaNameS),
						zap.String("table_name_s", idxMeta["TABLE_NAME"]),
						zap.String("index_name_s", idxMeta["INDEX_NAME"]),
						zap.String("index_type_s", idxMeta["INDEX_TYPE"]),
						zap.String("index_column_list_s", idxMeta["COLUMN_LIST"]),
						zap.String("error", "database not support"))

					return uniqueIndexes, compatibilityIndexSql, fmt.Errorf("[GenTableUniqueIndex] the upstream database schema [%s] table [%s] reverse unique index panic, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta)
				}
			} else {
				zap.L().Error("reverse unique key",
					zap.String("task_name", t.TaskName),
					zap.String("task_flow", t.TaskFlow),
					zap.String("schema_name_s", t.DatasourceS.SchemaNameS),
					zap.String("table_name_s", idxMeta["TABLE_NAME"]),
					zap.String("index_name_s", idxMeta["INDEX_NAME"]),
					zap.String("index_type_s", idxMeta["INDEX_TYPE"]),
					zap.String("index_column_list_s", idxMeta["COLUMN_LIST"]))
				return uniqueIndexes, compatibilityIndexSql, fmt.Errorf("[GenTableUniqueIndex] the upstream database schema [%s] table [%s] reverse nonunique index panic, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta)
			}
		default:
			return uniqueIndexes, compatibilityIndexSql, fmt.Errorf("[GenTableUniqueIndex] oracle current taskflow [%s] isn't support, please contact author or reselect", t.TaskFlow)
		}
	}
	return uniqueIndexes, compatibilityIndexSql, nil
}

func (t *StructMigrateTable) GenTableNormalIndex() ([]string, []string, error) {
	// normal index【normal index、function index、bit-map index、domain index】
	var (
		normalIndexes         []string
		compatibilityIndexSql []string
	)
	for _, idxMeta := range t.TableAttributes.NormalIndex {
		var (
			columnList string
			indexName  string
			indexOwner string
			itypOwner  string
			itypName   string
		)
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(idxMeta["COLUMN_LIST"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenTableNormalIndex] the upstream database schema [%s] table [%s] column [%s] normal_index charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["COLUMN_LIST"], t.DBCharsetT, err)
		}
		columnList = stringutil.BytesToString(convertUtf8Raw)

		convertUtf8Raw, err = stringutil.CharsetConvert([]byte(idxMeta["INDEX_OWNER"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenTableUniqueIndex] the upstream database schema [%s] table [%s] column [%s] index_name [%s] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["COLUMN_LIST"], idxMeta["INDEX_NAME"], t.DBCharsetT, err)
		}
		indexOwner = stringutil.BytesToString(convertUtf8Raw)

		convertUtf8Raw, err = stringutil.CharsetConvert([]byte(idxMeta["INDEX_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenTableNormalIndex] the upstream database schema [%s] table [%s] index [%v] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["INDEX_NAME"], t.DBCharsetT, err)
		}
		indexName = stringutil.BytesToString(convertUtf8Raw)

		// Note:
		// additional processing of the same index name in different schemas
		// automatically change index names for index names that are not under the current schema，for example:
		// the migrate schema is marvin00, index_names list: marvin00.idx_marvin, marvin01.idx_marvin, then marvin01.idx_marvin -> marvin00.idx_marvin_marvin01
		if !strings.EqualFold(indexOwner, t.DatasourceS.SchemaNameS) {
			indexName = stringutil.StringBuilder(indexName, "_ow_", indexOwner)
		}

		convertUtf8Raw, err = stringutil.CharsetConvert([]byte(idxMeta["ITYP_OWNER"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenTableNormalIndex] the upstream database schema [%s] table [%s] itype_owner [%v] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["ITYP_OWNER"], t.DBCharsetT, err)
		}
		itypOwner = stringutil.BytesToString(convertUtf8Raw)

		convertUtf8Raw, err = stringutil.CharsetConvert([]byte(idxMeta["ITYP_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("[GenTableNormalIndex] the upstream database schema [%s] table [%s] itype_name [%v] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["ITYP_NAME"], t.DBCharsetT, err)
		}
		itypName = stringutil.BytesToString(convertUtf8Raw)

		if strings.EqualFold(t.TableAttributesRule.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleLower) {
			itypOwner = strings.ToLower(itypOwner)
			itypName = strings.ToLower(itypName)
			indexName = strings.ToLower(indexName)
		}
		if strings.EqualFold(t.TableAttributesRule.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
			itypOwner = strings.ToUpper(itypOwner)
			itypName = strings.ToUpper(itypName)
			indexName = strings.ToUpper(indexName)
		}

		schemaName, err := t.GenSchemaNameT()
		if err != nil {
			return nil, nil, err
		}
		tableName, err := t.GenTableNameT()
		if err != nil {
			return nil, nil, err
		}

		switch t.TaskFlow {
		case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
			convertTargetRaw, err := stringutil.CharsetConvert([]byte(indexName), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
			if err != nil {
				return nil, nil, fmt.Errorf("[GenTableNormalIndex] the upstream database schema [%s] table [%s] index [%v] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["INDEX_NAME"], t.DBCharsetT, err)
			}
			indexName = stringutil.BytesToString(convertTargetRaw)

			convertTargetRaw, err = stringutil.CharsetConvert([]byte(itypOwner), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
			if err != nil {
				return nil, nil, fmt.Errorf("[GenTableNormalIndex] the upstream database schema [%s] table [%s] itype_owner [%v] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["ITYP_OWNER"], t.DBCharsetT, err)
			}
			itypOwner = stringutil.BytesToString(convertTargetRaw)

			convertTargetRaw, err = stringutil.CharsetConvert([]byte(itypName), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
			if err != nil {
				return nil, nil, fmt.Errorf("[GenTableNormalIndex] the upstream database schema [%s] table [%s] itype_name [%v] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["ITYP_NAME"], t.DBCharsetT, err)
			}
			itypName = stringutil.BytesToString(convertTargetRaw)

			// Function Index: SUBSTR("NAME8",1,8)|+|NAME9
			normalIndex := strings.Split(columnList, "|+|")
			for i, col := range normalIndex {
				// SUBSTR("NAME8",1,8) OR NAME9
				brackets := stringutil.StringExtractorWithinBrackets(col)
				if len(brackets) == 0 {
					// NAME9 PART
					if val, ok := t.TableAttributesRule.ColumnNameRule[col]; ok {
						convertTargetRaw, err = stringutil.CharsetConvert([]byte(val), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
						if err != nil {
							return nil, nil, fmt.Errorf("[GenTableNormalIndex] the upstream database schema [%s] table [%s] column [%s] normal_index charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["COLUMN_LIST"], t.DBCharsetT, err)
						}
						normalIndex[i] = fmt.Sprintf("`%s`", stringutil.BytesToString(convertTargetRaw))
					} else {
						return nil, nil, fmt.Errorf("[GenTableNormalIndex] the upstream database schema [%v] table [%v] column [%v] isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, col)
					}
				} else {
					for k, v := range t.TableAttributesRule.ColumnNameRule {
						if stringutil.StringMatcher(col, "\""+k+"\"") {
							convertTargetRaw, err = stringutil.CharsetConvert([]byte(stringutil.StringReplacer(col, "\""+k+"\"", fmt.Sprintf("`%s`", v))), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
							if err != nil {
								return nil, nil, fmt.Errorf("[GenTableNormalIndex] the upstream database schema [%s] table [%s] column_list [%s] column [%s] normal_index charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta["COLUMN_LIST"], col, t.DBCharsetT, err)
							}
							normalIndex[i] = stringutil.BytesToString(convertTargetRaw)
						}
					}
				}
			}
			columnList = strings.Join(normalIndex, ",")

			indexName = fmt.Sprintf("`%s`", indexName)
			itypOwner = fmt.Sprintf("`%s`", itypOwner)
			itypName = fmt.Sprintf("`%s`", itypName)
			if idxMeta["TABLE_NAME"] != "" && strings.EqualFold(idxMeta["UNIQUENESS"], "NONUNIQUE") {
				switch idxMeta["INDEX_TYPE"] {
				case "NORMAL":
					keyIndex := fmt.Sprintf("KEY %s (%s)", indexName, columnList)
					normalIndexes = append(normalIndexes, keyIndex)
					continue

				case "FUNCTION-BASED NORMAL":
					sqlStr := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);",
						indexName, schemaName, tableName,
						columnList)

					compatibilityIndexSql = append(compatibilityIndexSql, sqlStr)

					zap.L().Warn("reverse normal index",
						zap.String("task_name", t.TaskName),
						zap.String("task_flow", t.TaskFlow),
						zap.String("schema_name_s", t.DatasourceS.SchemaNameS),
						zap.String("table_name_s", idxMeta["TABLE_NAME"]),
						zap.String("index_name_s", idxMeta["INDEX_NAME"]),
						zap.String("index_type_s", idxMeta["INDEX_TYPE"]),
						zap.String("index_column_list_s", idxMeta["COLUMN_LIST"]),
						zap.String("create_sql", sqlStr),
						zap.String("warn", "database not support"))
					continue

				case "BITMAP":
					sqlStr := fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);",
						indexName, schemaName, tableName,
						columnList)

					compatibilityIndexSql = append(compatibilityIndexSql, sqlStr)

					zap.L().Warn("reverse normal index",
						zap.String("task_name", t.TaskName),
						zap.String("task_flow", t.TaskFlow),
						zap.String("schema_name_s", t.DatasourceS.SchemaNameS),
						zap.String("table_name_s", idxMeta["TABLE_NAME"]),
						zap.String("index_name_s", idxMeta["INDEX_NAME"]),
						zap.String("index_type_s", idxMeta["INDEX_TYPE"]),
						zap.String("index_column_list_s", idxMeta["COLUMN_LIST"]),
						zap.String("create_sql", sqlStr),
						zap.String("warn", "database not support"))
					continue

				case "FUNCTION-BASED BITMAP":
					sqlStr := fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);",
						indexName, schemaName, tableName,
						columnList)

					compatibilityIndexSql = append(compatibilityIndexSql, sqlStr)

					zap.L().Warn("reverse normal index",
						zap.String("task_name", t.TaskName),
						zap.String("task_flow", t.TaskFlow),
						zap.String("schema_name_s", t.DatasourceS.SchemaNameS),
						zap.String("table_name_s", idxMeta["TABLE_NAME"]),
						zap.String("index_name_s", idxMeta["INDEX_NAME"]),
						zap.String("index_type_s", idxMeta["INDEX_TYPE"]),
						zap.String("index_column_list_s", idxMeta["COLUMN_LIST"]),
						zap.String("create_sql", sqlStr),
						zap.String("warn", "database not support"))
					continue

				case "DOMAIN":
					sqlStr := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s) INDEXTYPE IS %s.%s PARAMETERS ('%s');",
						indexName, schemaName, tableName,
						columnList,
						itypOwner,
						itypName,
						idxMeta["PARAMETERS"])

					compatibilityIndexSql = append(compatibilityIndexSql, sqlStr)

					zap.L().Warn("reverse normal index",
						zap.String("task_name", t.TaskName),
						zap.String("task_flow", t.TaskFlow),
						zap.String("schema_name_s", t.DatasourceS.SchemaNameS),
						zap.String("table_name_s", idxMeta["TABLE_NAME"]),
						zap.String("index_name_s", idxMeta["INDEX_NAME"]),
						zap.String("index_type_s", idxMeta["INDEX_TYPE"]),
						zap.String("index_column_list_s", idxMeta["COLUMN_LIST"]),
						zap.String("domain_owner_s", idxMeta["ITYP_OWNER"]),
						zap.String("domain_index_name_s", idxMeta["ITYP_NAME"]),
						zap.String("domain_parameters_s", idxMeta["PARAMETERS"]),
						zap.String("create sql", sqlStr),
						zap.String("warn", "database not support"))
					continue

				case "NORMAL/REV":
					sqlStr := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s) REVERSE;",
						indexName, schemaName, tableName,
						columnList)
					compatibilityIndexSql = append(compatibilityIndexSql, sqlStr)

					zap.L().Warn("reverse normal index",
						zap.String("task_name", t.TaskName),
						zap.String("task_flow", t.TaskFlow),
						zap.String("schema_name_s", t.DatasourceS.SchemaNameS),
						zap.String("table_name_s", idxMeta["TABLE_NAME"]),
						zap.String("index_name_s", idxMeta["INDEX_NAME"]),
						zap.String("index_type_s", idxMeta["INDEX_TYPE"]),
						zap.String("index_column_list_s", idxMeta["COLUMN_LIST"]),
						zap.String("domain_owner_s", idxMeta["ITYP_OWNER"]),
						zap.String("domain_index_name_s", idxMeta["ITYP_NAME"]),
						zap.String("domain_parameters_s", idxMeta["PARAMETERS"]),
						zap.String("create sql", sqlStr),
						zap.String("warn", "database not support"))
					continue

				default:
					zap.L().Error("reverse normal index",
						zap.String("task_name", t.TaskName),
						zap.String("task_flow", t.TaskFlow),
						zap.String("schema_name_s", t.DatasourceS.SchemaNameS),
						zap.String("table_name_s", idxMeta["TABLE_NAME"]),
						zap.String("index_name_s", idxMeta["INDEX_NAME"]),
						zap.String("index_type_s", idxMeta["INDEX_TYPE"]),
						zap.String("index_column_list_s", idxMeta["COLUMN_LIST"]),
						zap.String("domain_owner_s", idxMeta["ITYP_OWNER"]),
						zap.String("domain_index_name_s", idxMeta["ITYP_NAME"]),
						zap.String("domain_parameters_s", idxMeta["PARAMETERS"]),
						zap.String("error", "database not support"))

					return normalIndexes, compatibilityIndexSql, fmt.Errorf("[GenTableNormalIndex] the upstream database schema [%s] table [%s] reverse normal index panic, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, idxMeta)
				}
			} else {
				zap.L().Error("reverse normal index",
					zap.String("task_name", t.TaskName),
					zap.String("task_flow", t.TaskFlow),
					zap.String("schema_name_s", t.DatasourceS.SchemaNameS),
					zap.String("table_name_s", idxMeta["TABLE_NAME"]),
					zap.String("index_name_s", idxMeta["INDEX_NAME"]),
					zap.String("index_type_s", idxMeta["INDEX_TYPE"]),
					zap.String("index_column_list_s", idxMeta["COLUMN_LIST"]),
					zap.String("domain_owner_s", idxMeta["ITYP_OWNER"]),
					zap.String("domain_index_name_s", idxMeta["ITYP_NAME"]),
					zap.String("domain_parameters_s", idxMeta["PARAMETERS"]))
				return normalIndexes, compatibilityIndexSql, fmt.Errorf("[GenTableNormalIndex] the upstream database schema [%s] table [%s] reverse normal unique index panic, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableTypeS, idxMeta)
			}
		default:
			return normalIndexes, compatibilityIndexSql, fmt.Errorf("[GenTableNormalIndex] oracle current taskflow [%s] isn't support, please contact author or reselect", t.TaskFlow)
		}
	}
	return normalIndexes, compatibilityIndexSql, nil
}

func (t *StructMigrateTable) GenTableComment() (string, error) {
	var tableComment string
	if !strings.EqualFold(t.TableAttributesRule.TableCommentRule, "") {
		// comment、data default、column name unescaped
		convertUtf8Raw := []byte(t.TableAttributesRule.TableCommentRule)
		switch t.TaskFlow {
		case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
			convertTargetRaw, err := stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
			if err != nil {
				return "", fmt.Errorf("[GenTableComment] the upstream database schema [%s] table [%s] comment [%v] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, t.TableAttributesRule.TableCommentRule, t.DBCharsetT, err)
			}
			tableComment = fmt.Sprintf("COMMENT='%s'", stringutil.BytesToString(convertTargetRaw))
			return tableComment, nil
		default:
			return tableComment, fmt.Errorf("[GenTableComment] oracle current taskflow [%s] isn't support, please contact author or reselect", t.TaskFlow)
		}
	}
	return tableComment, nil
}

func (t *StructMigrateTable) GenTableColumns() ([]string, error) {
	var tableColumns []string
	for _, rowCol := range t.TableAttributes.TableColumns {
		var (
			columnCollation string
			nullable        string
			comment         string
			dataDefault     string
			columnType      string
		)

		// column collation
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(rowCol["COLUMN_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(t.TableAttributes.TableCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("[GenTableColumns] the upstream database schema [%s] table [%s] column [%v] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, rowCol["COLUMN_NAME"], t.DBCharsetT, err)
		}
		columnName := stringutil.BytesToString(convertUtf8Raw)

		if val, exist := t.TableAttributesRule.ColumnCollationRule[columnName]; exist {
			columnCollation = val
		} else {
			return tableColumns, fmt.Errorf("[GenTableColumns] oracle table [%s.%s] column [%s] collation isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, columnName)
		}

		// column datatype
		if val, ok := t.TableAttributesRule.ColumnDatatypeRule[columnName]; ok {
			columnType = val
		} else {
			return tableColumns, fmt.Errorf("[GenTableColumns] oracle table [%s.%s] column [%s] datatype isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, columnName)
		}

		// NULLABLE
		if strings.EqualFold(rowCol["NULLABLE"], "Y") {
			nullable = "NULL"
		} else {
			nullable = "NOT NULL"
		}

		// column attr
		if val, ok := t.TableAttributesRule.ColumnDefaultValueRule[columnName]; ok {
			dataDefault = val
		} else {
			return tableColumns, fmt.Errorf("[GenTableColumns] oracle table [%s.%s] column [%s] default value isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, columnName)
		}

		if val, ok := t.TableAttributesRule.ColumnCommentRule[columnName]; ok {
			comment = val
		} else {
			return tableColumns, fmt.Errorf("[GenTableColumns] oracle table [%s.%s] column [%s] comment isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, columnName)
		}

		if val, ok := t.TableAttributesRule.ColumnNameRule[columnName]; ok {
			columnName = val
		} else {
			return tableColumns, fmt.Errorf("[GenTableColumns] oracle table [%s.%s] column [%s] rule isn't exist, please contact author or recheck", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, columnName)
		}

		switch t.TaskFlow {
		case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
			// column name

			if strings.EqualFold(t.TableAttributesRule.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleLower) {
				columnName = strings.ToLower(columnName)
			}
			if strings.EqualFold(t.TableAttributesRule.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
				columnName = strings.ToUpper(columnName)
			}

			// comment、data default、column name unescaped
			convertTargetRaw, err := stringutil.CharsetConvert([]byte(columnName), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
			if err != nil {
				return nil, fmt.Errorf("[GenTableColumns] the upstream database schema [%s] table [%s] column [%v] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, columnName, t.DBCharsetT, err)
			}
			columnName = stringutil.BytesToString(convertTargetRaw)

			convertTargetRaw, err = stringutil.CharsetConvert([]byte(comment), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
			if err != nil {
				return nil, fmt.Errorf("[GenTableColumns] the upstream database schema [%s] table [%s] column [%v] comment [%v] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, columnName, comment, t.DBCharsetT, err)
			}
			comment = stringutil.BytesToString(convertTargetRaw)

			convertTargetRaw, err = stringutil.CharsetConvert([]byte(dataDefault), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(t.DBCharsetT)])
			if err != nil {
				return nil, fmt.Errorf("[GenTableColumns] the upstream database schema [%s] table [%s] column [%v] default value [%v] charset convert [%s] failed, error: %v", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, columnName, dataDefault, t.DBCharsetT, err)
			}
			dataDefault = stringutil.BytesToString(convertTargetRaw)

			if strings.EqualFold(nullable, "NULL") {
				switch {
				case columnCollation != "" && comment != "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULL) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s COMMENT '%s'", columnName, columnType, columnCollation, comment))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s DEFAULT %s COMMENT '%s'", columnName, columnType, columnCollation, dataDefault, comment))
					}
				case columnCollation != "" && comment == "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULL) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s", columnName, columnType, columnCollation))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s DEFAULT %s", columnName, columnType, columnCollation, dataDefault))
					}
				case columnCollation == "" && comment != "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULL) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COMMENT '%s'", columnName, columnType, comment))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s DEFAULT %s COMMENT '%s'", columnName, columnType, dataDefault, comment))
					}
				case columnCollation == "" && comment == "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULL) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s", columnName, columnType))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s DEFAULT %s", columnName, columnType, dataDefault))
					}
				default:
					return tableColumns, fmt.Errorf("[GenTableColumns] oracle table [%s.%s] column [%s] nulllable [NULL] panic, collation [%s] comment [%s]", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, columnName, columnCollation, comment)
				}
			}

			if strings.EqualFold(nullable, "NOT NULL") {
				switch {
				case columnCollation != "" && comment != "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULL) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s COMMENT '%s'", columnName, columnType, columnCollation, nullable, comment))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s DEFAULT %s COMMENT '%s'", columnName, columnType, columnCollation, nullable, dataDefault, comment))
					}
				case columnCollation != "" && comment == "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULL) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s", columnName, columnType, columnCollation, nullable))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s DEFAULT %s", columnName, columnType, columnCollation, nullable, dataDefault))
					}
				case columnCollation == "" && comment != "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULL) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s COMMENT '%s'", columnName, columnType, nullable, comment))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s DEFAULT %s COMMENT '%s'", columnName, columnType, nullable, dataDefault, comment))
					}
				case columnCollation == "" && comment == "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULL) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s", columnName, columnType, nullable))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s DEFAULT %s", columnName, columnType, nullable, dataDefault))
					}
				default:
					return tableColumns, fmt.Errorf("[GenTableColumns] oracle table [%s.%s] column [%s] nulllable [NOT NULL] panic, collation [%s] comment [%s]", t.DatasourceS.SchemaNameS, t.DatasourceS.TableNameS, columnName, columnCollation, comment)
				}
			}

		default:
			return tableColumns, fmt.Errorf("[GenTableColumns] oracle current taskflow [%s] isn't support, please contact author or reselect", t.TaskFlow)
		}
	}

	return tableColumns, nil
}

func (t *StructMigrateTable) GenTableColumnComment() ([]string, error) {
	switch t.TaskFlow {
	case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
		// the GenTableColumns function had done
		return nil, nil
	default:
		return nil, fmt.Errorf("[GenTableColumnComment] oracle current taskflow [%s] isn't support, please contact author or reselect", t.TaskFlow)
	}
}

func (t *StructMigrateTable) String() string {
	jsonStr, _ := stringutil.MarshalJSON(t)
	return jsonStr
}
