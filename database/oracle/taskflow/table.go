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
package taskflow

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

type Table struct {
	TaskName            string                        `json:"taskName"`
	TaskFlow            string                        `json:"taskFlow"`
	Datasource          *Datasource                   `json:"datasource"`
	TableAttributes     *database.TableAttributes     `json:"tableAttributes"`
	TableAttributesRule *database.TableAttributesRule `json:"tableAttributesRule"`
}

func (t *Table) GenSchemaNameS() string {
	return t.Datasource.SchemaNameS
}

func (t *Table) GenTableNameS() string {
	return t.Datasource.TableNameS
}

func (t *Table) GenTableTypeS() string {
	return t.Datasource.TableTypeS
}

func (t *Table) GenTableOriginDDlS() string {
	return t.TableAttributes.OriginStruct
}

func (t *Table) GenSchemaNameT() string {
	var schemaName string
	if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameLower) {
		schemaName = strings.ToLower(t.TableAttributesRule.SchemaNameRule[t.Datasource.SchemaNameS])
	}
	if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameUpper) {
		schemaName = strings.ToUpper(t.TableAttributesRule.SchemaNameRule[t.Datasource.SchemaNameS])
	}
	if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameOrigin) {
		schemaName = t.TableAttributesRule.SchemaNameRule[t.Datasource.SchemaNameS]
	}
	switch {
	case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
		schemaName = fmt.Sprintf("`%s`", schemaName)
		return schemaName
	default:
		return schemaName
	}
}

func (t *Table) GenTableNameT() string {
	var tableName string
	if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameLower) {
		tableName = strings.ToLower(t.TableAttributesRule.TableNameRule[t.Datasource.TableNameS])
	}
	if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameUpper) {
		tableName = strings.ToUpper(t.TableAttributesRule.TableNameRule[t.Datasource.TableNameS])
	}
	if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameOrigin) {
		tableName = t.TableAttributesRule.TableNameRule[t.Datasource.TableNameS]
	}
	switch {
	case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
		tableName = fmt.Sprintf("`%s`", tableName)
		return tableName
	default:
		return tableName
	}
}

func (t *Table) GenTableSuffix() (string, error) {
	var (
		tableCharset   string
		tableCollation string
		tableSuffix    string
	)

	tableCharset = constant.MigrateTableStructureDatabaseCharsetMap[t.TaskFlow][t.Datasource.DBCharsetS]

	// schema、db、table collation
	if t.Datasource.CollationS {
		// table collation
		if t.Datasource.TableCollationS != "" {
			if val, ok := constant.MigrateTableStructureDatabaseCollationMap[t.TaskFlow][t.Datasource.TableCollationS][tableCharset]; ok {
				tableCollation = val
			} else {
				return tableSuffix, fmt.Errorf("oracle database table collation [%v] isn't support", t.Datasource.TableCollationS)
			}
		}
		// schema collation
		if t.Datasource.TableCollationS == "" && t.Datasource.SchemaCollationS != "" {
			if val, ok := constant.MigrateTableStructureDatabaseCollationMap[t.TaskFlow][t.Datasource.SchemaCollationS][tableCharset]; ok {
				tableCollation = val
			} else {
				return tableSuffix, fmt.Errorf("oracle database schema collation [%v] table collation [%v] isn't support", t.Datasource.SchemaCollationS, t.Datasource.TableCollationS)
			}
		}
		if t.Datasource.TableCollationS == "" && t.Datasource.SchemaCollationS == "" {
			return tableSuffix, fmt.Errorf("oracle database schema collation [%v] table collation [%v] isn't support", t.Datasource.SchemaNameS, t.Datasource.TableCollationS)
		}
	} else {
		// db collation
		if val, ok := constant.MigrateTableStructureDatabaseCollationMap[t.TaskFlow][t.Datasource.DBNlsCompS][tableCharset]; ok {
			tableCollation = val
		} else {
			return tableSuffix, fmt.Errorf("oracle database nls_comp [%v] collation isn't support", t.Datasource.DBNlsCompS)
		}
	}

	switch stringutil.StringUpper(t.TaskFlow) {
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
		return tableSuffix, fmt.Errorf("oracle current taskflow [%s] generate table suffix isn't support, please contact author or reselect", t.TaskFlow)
	}
}

func (t *Table) GenTablePrimaryKey() (string, error) {
	var primaryKey string
	if len(t.TableAttributes.PrimaryKey) > 1 {
		return primaryKey, fmt.Errorf("oracle database schema [%s] table [%s] primary key exist multiple values: [%v]", t.Datasource.SchemaNameS, t.Datasource.TableNameS, t.TableAttributes.PrimaryKey)
	}
	if len(t.TableAttributes.PrimaryKey) == 1 {
		var (
			columnList     string
			primaryColumns []string
		)

		if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameLower) {
			columnList = strings.ToLower(t.TableAttributes.PrimaryKey[0]["COLUMN_LIST"])
		}
		if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameUpper) {
			columnList = strings.ToUpper(t.TableAttributes.PrimaryKey[0]["COLUMN_LIST"])
		}
		if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameOrigin) {
			columnList = t.TableAttributes.PrimaryKey[0]["COLUMN_LIST"]
		}

		for _, col := range strings.Split(columnList, ",") {
			switch {
			case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
				primaryColumns = append(primaryColumns, fmt.Sprintf("`%s`", col))
			default:
				primaryColumns = append(primaryColumns, col)
			}
		}

		switch stringutil.StringUpper(t.TaskFlow) {
		case constant.TaskFlowOracleToTiDB:
			// clustered index table
			// single primary column and the column is integer
			if len(primaryColumns) == 1 {
				singleIntegerPK := false
				// get single primary column datatype
				for columnName, columnDatatype := range t.TableAttributesRule.ColumnDatatypeRule {
					if strings.EqualFold(primaryColumns[0], columnName) {
						// offer the column datatype after converting by the column datatype rule,
						for _, integerType := range constant.TiDBDatabaseIntegerPrimaryKeyMenu {
							if find := strings.Contains(stringutil.StringUpper(columnDatatype), stringutil.StringUpper(integerType)); find {
								singleIntegerPK = true
							}
						}
					}
				}
				// if the table primary key is integer datatype, and the table attr rule can't null (shard_row_id_bits)
				// it represents the primary key need set NONCLUSTERED attributes
				if singleIntegerPK && !strings.EqualFold(t.TableAttributesRule.TableAttrRule, "") {
					primaryKey = fmt.Sprintf("PRIMARY KEY (%s) NONCLUSTERED", strings.Join(primaryColumns, ","))
				} else if singleIntegerPK && strings.EqualFold(t.TableAttributesRule.TableAttrRule, "") {
					primaryKey = fmt.Sprintf("PRIMARY KEY (%s) CLUSTERED", strings.Join(primaryColumns, ","))
				} else {
					primaryKey = fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryColumns, ","))
				}
			}
			// otherwise
			primaryKey = fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryColumns, ","))
		default:
			primaryKey = fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryColumns, ","))
		}
	}
	return primaryKey, nil
}

func (t *Table) GenTableUniqueKey() ([]string, error) {
	var uniqueKeys []string
	if len(t.TableAttributes.UniqueKey) > 0 {
		for _, rowUKCol := range t.TableAttributes.UniqueKey {
			var (
				ukArr      []string
				columnList string
				uk         string
			)
			if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameLower) {
				columnList = strings.ToLower(rowUKCol["COLUMN_LIST"])
			}
			if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameUpper) {
				columnList = strings.ToUpper(rowUKCol["COLUMN_LIST"])
			}
			if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameOrigin) {
				columnList = rowUKCol["COLUMN_LIST"]
			}
			for _, col := range strings.Split(columnList, ",") {
				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
					ukArr = append(ukArr, fmt.Sprintf("`%s`", col))
				default:
					ukArr = append(ukArr, col)
				}
			}

			switch {
			case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
				uk = fmt.Sprintf("UNIQUE KEY `%s` (%s)",
					rowUKCol["CONSTRAINT_NAME"], strings.Join(ukArr, ","))
			default:
				uk = fmt.Sprintf("CONSTRAINT %s UNIQUE (%s)",
					rowUKCol["CONSTRAINT_NAME"], strings.Join(ukArr, ","))
			}

			uniqueKeys = append(uniqueKeys, uk)
		}
	}
	return uniqueKeys, nil
}

func (t *Table) GenTableForeignKey() ([]string, error) {
	var foreignKeys []string
	if len(t.TableAttributes.ForeignKey) > 0 {
		var (
			fk          string
			columnList  string
			rOwner      string
			rTable      string
			rColumnList string
		)
		for _, rowFKCol := range t.TableAttributes.ForeignKey {
			if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameLower) {
				columnList = strings.ToLower(rowFKCol["COLUMN_LIST"])
				rOwner = strings.ToLower(rowFKCol["R_OWNER"])
				rTable = strings.ToLower(rowFKCol["RTABLE_NAME"])
				rColumnList = strings.ToLower(rowFKCol["RCOLUMN_LIST"])
			}
			if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameUpper) {
				columnList = strings.ToUpper(rowFKCol["COLUMN_LIST"])
				rOwner = strings.ToUpper(rowFKCol["R_OWNER"])
				rTable = strings.ToUpper(rowFKCol["RTABLE_NAME"])
				rColumnList = strings.ToUpper(rowFKCol["RCOLUMN_LIST"])
			}
			if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameOrigin) {
				columnList = rowFKCol["COLUMN_LIST"]
				rOwner = rowFKCol["R_OWNER"]
				rTable = rowFKCol["RTABLE_NAME"]
				rColumnList = rowFKCol["RCOLUMN_LIST"]
			}

			switch {
			case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
				var (
					originCols  []string
					rOriginCols []string
				)
				for _, c := range strings.Split(columnList, ",") {
					originCols = append(originCols, fmt.Sprintf("`%s`", c))
				}
				for _, c := range strings.Split(rColumnList, ",") {
					rOriginCols = append(rOriginCols, fmt.Sprintf("`%s`", c))
				}

				if rowFKCol["DELETE_RULE"] == "" || rowFKCol["DELETE_RULE"] == "NO ACTION" {
					fk = fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`.`%s` (%s)",
						rowFKCol["CONSTRAINT_NAME"],
						strings.Join(originCols, ","),
						rOwner,
						rTable,
						strings.Join(rOriginCols, ","))
				}
				if rowFKCol["DELETE_RULE"] == "CASCADE" {
					fk = fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`.`%s`(%s) ON DELETE CASCADE",
						rowFKCol["CONSTRAINT_NAME"],
						strings.Join(originCols, ","),
						rOwner,
						rTable,
						strings.Join(rOriginCols, ","))
				}
				if rowFKCol["DELETE_RULE"] == "SET NULL" {
					fk = fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY(%s) REFERENCES `%s`.`%s`(%s) ON DELETE SET NULL",
						rowFKCol["CONSTRAINT_NAME"],
						strings.Join(originCols, ","),
						rOwner,
						rTable,
						strings.Join(rOriginCols, ","))
				}
			default:
				if rowFKCol["DELETE_RULE"] == "" || rowFKCol["DELETE_RULE"] == "NO ACTION" {
					fk = fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s)",
						rowFKCol["CONSTRAINT_NAME"],
						columnList,
						rOwner,
						rTable,
						rColumnList)
				}
				if rowFKCol["DELETE_RULE"] == "CASCADE" {
					fk = fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s(%s) ON DELETE CASCADE",
						rowFKCol["CONSTRAINT_NAME"],
						columnList,
						rOwner,
						rTable,
						rColumnList)
				}
				if rowFKCol["DELETE_RULE"] == "SET NULL" {
					fk = fmt.Sprintf("CONSTRAINT %s FOREIGN KEY(%s) REFERENCES %s.%s(%s) ON DELETE SET NULL",
						rowFKCol["CONSTRAINT_NAME"],
						columnList,
						rOwner,
						rTable,
						rColumnList)
				}
			}
			foreignKeys = append(foreignKeys, fk)
		}
	}

	return foreignKeys, nil
}

func (t *Table) GenTableCheckKey() ([]string, error) {
	var checkKeys []string
	if len(t.TableAttributes.CheckKey) > 0 {
		// check constraint match
		// example："LOC" IS noT nUll and loc in ('a','b','c')
		reg, err := regexp.Compile(`\s+(?i:AND)\s+|\s+(?i:OR)\s+`)
		if err != nil {
			return checkKeys, fmt.Errorf("check constraint regexp [AND/OR] failed: %v", err)
		}

		matchRex, err := regexp.Compile(`(^.*)(?i:IS NOT NULL)`)
		if err != nil {
			return checkKeys, fmt.Errorf("check constraint regexp match [IS NOT NULL] failed: %v", err)
		}

		checkRex, err := regexp.Compile(`(.*)(?i:IS NOT NULL)`)
		if err != nil {
			fmt.Printf("check constraint regexp check [IS NOT NULL] failed: %v", err)
		}

		for _, rowCKCol := range t.TableAttributes.CheckKey {

			searchCond := rowCKCol["SEARCH_CONDITION"]
			constraintName := rowCKCol["CONSTRAINT_NAME"]

			// match replace
			for _, rowCol := range t.TableAttributes.TableColumns {
				replaceRex, err := regexp.Compile(fmt.Sprintf("(?i)%v", rowCol["COLUMN_NAME"]))
				if err != nil {
					return nil, err
				}
				if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameLower) {
					searchCond = replaceRex.ReplaceAllString(searchCond, strings.ToLower(rowCol["COLUMN_NAME"]))
				}
				if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameUpper) {
					searchCond = replaceRex.ReplaceAllString(searchCond, strings.ToUpper(rowCol["COLUMN_NAME"]))
				}
				if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameOrigin) {
					searchCond = replaceRex.ReplaceAllString(searchCond, rowCol["COLUMN_NAME"])
				}
			}

			// exclude not null constraint
			s := strings.TrimSpace(searchCond)

			if !reg.MatchString(s) {
				if !matchRex.MatchString(s) {
					switch {
					case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
						checkKeys = append(checkKeys, fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)",
							constraintName,
							searchCond))
					default:
						checkKeys = append(checkKeys, fmt.Sprintf("CONSTRAINT %s CHECK (%s)",
							constraintName,
							searchCond))
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

				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
					checkKeys = append(checkKeys, fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)",
						constraintName,
						strings.Join(d, " ")))
				default:
					checkKeys = append(checkKeys, fmt.Sprintf("CONSTRAINT %s CHECK (%s)",
						constraintName,
						strings.Join(d, " ")))
				}
			}
		}
	}

	return checkKeys, nil
}

func (t *Table) GenTableUniqueIndex() ([]string, []string, error) {
	var (
		uniqueIndexes         []string
		compatibilityIndexSql []string
	)
	if len(t.TableAttributes.UniqueIndex) > 0 {
		for _, idxMeta := range t.TableAttributes.UniqueIndex {
			var columnList string
			if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameLower) {
				columnList = strings.ToLower(idxMeta["COLUMN_LIST"])
			}
			if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameUpper) {
				columnList = strings.ToUpper(idxMeta["COLUMN_LIST"])
			}
			if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameOrigin) {
				columnList = idxMeta["COLUMN_LIST"]
			}
			if idxMeta["TABLE_NAME"] != "" && strings.EqualFold(idxMeta["UNIQUENESS"], "UNIQUE") {
				switch idxMeta["INDEX_TYPE"] {
				case "NORMAL":
					switch {
					case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
						var uniqueIndex []string
						for _, col := range strings.Split(columnList, ",") {
							uniqueIndex = append(uniqueIndex, fmt.Sprintf("`%s`", col))
						}
						uniqueIDX := fmt.Sprintf("UNIQUE INDEX `%s` (%s)", idxMeta["INDEX_NAME"], strings.Join(uniqueIndex, ","))
						uniqueIndexes = append(uniqueIndexes, uniqueIDX)
					default:
						uniqueIDX := fmt.Sprintf("UNIQUE INDEX %s (%s)", idxMeta["INDEX_NAME"], columnList)
						uniqueIndexes = append(uniqueIndexes, uniqueIDX)
					}
					continue

				case "FUNCTION-BASED NORMAL":
					var sqlStr string
					switch {
					case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
						var uniqueIndex []string
						for _, col := range strings.Split(columnList, ",") {
							uniqueIndex = append(uniqueIndex, fmt.Sprintf("`%s`", col))
						}
						sqlStr = fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s` (%s);",
							idxMeta["INDEX_NAME"], t.GenSchemaNameT(), t.GenTableNameT(),
							strings.Join(uniqueIndex, ","))
					default:
						sqlStr = fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s (%s);",
							idxMeta["INDEX_NAME"], t.GenSchemaNameT(), t.GenTableNameT(),
							columnList)
					}
					compatibilityIndexSql = append(compatibilityIndexSql, sqlStr)

					zap.L().Warn("reverse unique key",
						zap.String("schema", t.Datasource.SchemaNameS),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create sql", sqlStr),
						zap.String("warn", "database not support"))

					continue

				case "NORMAL/REV":
					var sqlStr string
					switch {
					case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
						var uniqueIndex []string
						for _, col := range strings.Split(columnList, ",") {
							uniqueIndex = append(uniqueIndex, fmt.Sprintf("`%s`", col))
						}
						sqlStr = fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s` (%s) REVERSE;",
							idxMeta["INDEX_NAME"], t.GenSchemaNameT(), t.GenTableNameT(),
							strings.Join(uniqueIndex, ","))
					default:
						sqlStr = fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s (%s);",
							idxMeta["INDEX_NAME"], t.GenSchemaNameT(), t.GenTableNameT(),
							columnList)
					}
					compatibilityIndexSql = append(compatibilityIndexSql, sqlStr)

					zap.L().Warn("reverse unique key",
						zap.String("schema", t.Datasource.SchemaNameS),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create sql", sqlStr),
						zap.String("warn", "database not support"))

					continue

				default:
					zap.L().Error("reverse unique index",
						zap.String("schema", t.Datasource.SchemaNameS),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("error", "database not support"))

					return uniqueIndexes, compatibilityIndexSql, fmt.Errorf("[UNIQUE] oracle schema [%s] table [%s] reverse normal index panic, error: %v", t.Datasource.SchemaNameS, t.Datasource.TableNameS, idxMeta)
				}
			}
			zap.L().Error("reverse unique key",
				zap.String("schema", t.Datasource.SchemaNameS),
				zap.String("table", idxMeta["TABLE_NAME"]),
				zap.String("index name", idxMeta["INDEX_NAME"]),
				zap.String("index type", idxMeta["INDEX_TYPE"]),
				zap.String("index column list", idxMeta["COLUMN_LIST"]))
			return uniqueIndexes, compatibilityIndexSql,
				fmt.Errorf("[NON-UNIQUE] oracle schema [%s] table [%s] panic, error: %v", t.Datasource.SchemaNameS, t.Datasource.TableNameS, idxMeta)
		}
	}

	return uniqueIndexes, compatibilityIndexSql, nil
}

func (t *Table) GenTableNormalIndex() ([]string, []string, error) {
	// normal index【normal index、function index、bit-map index、domain index】
	var (
		normalIndexes         []string
		compatibilityIndexSql []string
	)
	if len(t.TableAttributes.NormalIndex) > 0 {
		for _, idxMeta := range t.TableAttributes.NormalIndex {
			var (
				columnList string
				indexName  string
				itypOwner  string
				itypName   string
			)
			if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameLower) {
				columnList = strings.ToLower(idxMeta["COLUMN_LIST"])
				itypOwner = strings.ToLower(idxMeta["ITYP_OWNER"])
				itypName = strings.ToLower(idxMeta["ITYP_NAME"])
			}
			if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameUpper) {
				columnList = strings.ToUpper(idxMeta["COLUMN_LIST"])
				itypOwner = strings.ToUpper(idxMeta["ITYP_OWNER"])
				itypName = strings.ToUpper(idxMeta["ITYP_NAME"])
			}
			if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameOrigin) {
				columnList = idxMeta["COLUMN_LIST"]
				itypOwner = idxMeta["ITYP_OWNER"]
				itypName = idxMeta["ITYP_NAME"]
			}

			switch {
			case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
				var normalIndex []string
				for _, col := range strings.Split(columnList, ",") {
					normalIndex = append(normalIndex, fmt.Sprintf("`%s`", col))
				}
				indexName = fmt.Sprintf("`%s`", idxMeta["INDEX_NAME"])
				columnList = strings.Join(normalIndex, ",")
				itypOwner = fmt.Sprintf("`%s`", itypOwner)
				itypName = fmt.Sprintf("`%s`", itypName)
			default:
				indexName = idxMeta["INDEX_NAME"]
			}

			if idxMeta["TABLE_NAME"] != "" && strings.EqualFold(idxMeta["UNIQUENESS"], "NONUNIQUE") {
				switch idxMeta["INDEX_TYPE"] {
				case "NORMAL":
					keyIndex := fmt.Sprintf("KEY %s (%s)", indexName, columnList)
					normalIndexes = append(normalIndexes, keyIndex)
					continue

				case "FUNCTION-BASED NORMAL":
					sqlStr := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);",
						indexName, t.GenSchemaNameT(), t.GenTableNameT(),
						columnList)

					compatibilityIndexSql = append(compatibilityIndexSql, sqlStr)

					zap.L().Warn("reverse normal index",
						zap.String("schema", t.Datasource.SchemaNameS),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create sql", sqlStr),
						zap.String("warn", "database not support"))
					continue

				case "BITMAP":
					sqlStr := fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);",
						indexName, t.GenSchemaNameT(), t.GenTableNameT(),
						columnList)

					compatibilityIndexSql = append(compatibilityIndexSql, sqlStr)

					zap.L().Warn("reverse normal index",
						zap.String("schema", t.Datasource.SchemaNameS),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create sql", sqlStr),
						zap.String("warn", "database not support"))
					continue

				case "FUNCTION-BASED BITMAP":
					sqlStr := fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);",
						indexName, t.GenSchemaNameT(), t.GenTableNameT(),
						columnList)

					compatibilityIndexSql = append(compatibilityIndexSql, sqlStr)

					zap.L().Warn("reverse normal index",
						zap.String("schema", t.Datasource.SchemaNameS),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("create sql", sqlStr),
						zap.String("warn", "database not support"))
					continue

				case "DOMAIN":
					sqlStr := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s) INDEXTYPE IS %s.%s PARAMETERS ('%s');",
						indexName, t.GenSchemaNameT(), t.GenTableNameT(),
						columnList,
						itypOwner,
						itypName,
						idxMeta["PARAMETERS"])

					compatibilityIndexSql = append(compatibilityIndexSql, sqlStr)

					zap.L().Warn("reverse normal index",
						zap.String("schema", t.Datasource.SchemaNameS),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("domain owner", idxMeta["ITYP_OWNER"]),
						zap.String("domain index name", idxMeta["ITYP_NAME"]),
						zap.String("domain parameters", idxMeta["PARAMETERS"]),
						zap.String("create sql", sqlStr),
						zap.String("warn", "database not support"))
					continue

				case "NORMAL/REV":
					sqlStr := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s) REVERSE;",
						indexName, t.GenSchemaNameT(), t.GenTableNameT(),
						columnList)
					compatibilityIndexSql = append(compatibilityIndexSql, sqlStr)

					zap.L().Warn("reverse normal index",
						zap.String("schema", t.Datasource.SchemaNameS),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("domain owner", idxMeta["ITYP_OWNER"]),
						zap.String("domain index name", idxMeta["ITYP_NAME"]),
						zap.String("domain parameters", idxMeta["PARAMETERS"]),
						zap.String("create sql", sqlStr),
						zap.String("warn", "database not support"))
					continue

				default:
					zap.L().Error("reverse normal index",
						zap.String("schema", t.Datasource.SchemaNameS),
						zap.String("table", idxMeta["TABLE_NAME"]),
						zap.String("index name", idxMeta["INDEX_NAME"]),
						zap.String("index type", idxMeta["INDEX_TYPE"]),
						zap.String("index column list", idxMeta["COLUMN_LIST"]),
						zap.String("domain owner", idxMeta["ITYP_OWNER"]),
						zap.String("domain index name", idxMeta["ITYP_NAME"]),
						zap.String("domain parameters", idxMeta["PARAMETERS"]),
						zap.String("error", "database not support"))

					return normalIndexes, compatibilityIndexSql, fmt.Errorf("[NORMAL] oracle schema [%s] table [%s] reverse normal index panic, error: %v", t.Datasource.SchemaNameS, t.Datasource.TableNameS, idxMeta)
				}
			}

			zap.L().Error("reverse normal index",
				zap.String("schema", t.Datasource.SchemaNameS),
				zap.String("table", idxMeta["TABLE_NAME"]),
				zap.String("index name", idxMeta["INDEX_NAME"]),
				zap.String("index type", idxMeta["INDEX_TYPE"]),
				zap.String("index column list", idxMeta["COLUMN_LIST"]),
				zap.String("domain owner", idxMeta["ITYP_OWNER"]),
				zap.String("domain index name", idxMeta["ITYP_NAME"]),
				zap.String("domain parameters", idxMeta["PARAMETERS"]))
			return normalIndexes, compatibilityIndexSql, fmt.Errorf("[NON-NORMAL] oracle schema [%s] table [%s] reverse normal index panic, error: %v", t.Datasource.SchemaNameS, t.Datasource.TableTypeS, idxMeta)
		}
	}

	return normalIndexes, compatibilityIndexSql, nil
}

func (t *Table) GenTableComment() (string, error) {
	var tableComment string
	if len(t.TableAttributesRule.TableCommentRule) > 0 && !strings.EqualFold(t.TableAttributesRule.TableCommentRule, "") {
		switch {
		case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
			tableComment = fmt.Sprintf("COMMENT='%s'", t.TableAttributesRule.TableCommentRule)
		default:
			tableComment = fmt.Sprintf("COMMENT ON TABLE %s.%s IS '%s'", t.GenSchemaNameT(), t.GenTableNameT(), t.TableAttributesRule.TableCommentRule)
		}
	}
	return tableComment, nil
}

func (t *Table) GenTableColumns() ([]string, error) {
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
		columnName := rowCol["COLUMN_NAME"]
		if val, exist := t.TableAttributesRule.ColumnCollationRule[columnName]; exist {
			columnCollation = val
		} else {
			return tableColumns, fmt.Errorf("oracle table [%s.%s] column [%s] collation isn't exist, please contact author or recheck", t.Datasource.SchemaNameS, t.Datasource.TableNameS, columnName)
		}

		// column datatype
		if val, ok := t.TableAttributesRule.ColumnDatatypeRule[columnName]; ok {
			columnType = val
		} else {
			return tableColumns, fmt.Errorf("oracle table [%s.%s] column [%s] datatype isn't exist, please contact author or recheck", t.Datasource.SchemaNameS, t.Datasource.TableNameS, columnName)
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
			return tableColumns, fmt.Errorf("oracle table [%s.%s] column [%s] default value isn't exist, please contact author or recheck", t.Datasource.SchemaNameS, t.Datasource.TableNameS, columnName)
		}

		// column name
		if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameLower) {
			columnName = strings.ToLower(columnName)
		}
		if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameUpper) {
			columnName = strings.ToUpper(columnName)
		}
		if strings.EqualFold(t.TableAttributesRule.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameOrigin) {
			columnName = rowCol["COLUMN_NAME"]
		}

		switch {
		case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
			// column comment
			if val, ok := t.TableAttributesRule.ColumnCommentRule[columnName]; ok {
				comment = val
			} else {
				return tableColumns, fmt.Errorf("oracle table [%s.%s] column [%s] comment isn't exist, please contact author or recheck", t.Datasource.SchemaNameS, t.Datasource.TableNameS, columnName)
			}

			if strings.EqualFold(nullable, "NULL") {
				switch {
				case columnCollation != "" && comment != "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULLSTRING) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s COMMENT %s", columnName, columnType, columnCollation, comment))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s DEFAULT %s COMMENT %s", columnName, columnType, columnCollation, dataDefault, comment))
					}
				case columnCollation != "" && comment == "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULLSTRING) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s", columnName, columnType, columnCollation))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s DEFAULT %s", columnName, columnType, columnCollation, dataDefault))
					}
				case columnCollation == "" && comment != "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULLSTRING) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COMMENT %s", columnName, columnType, comment))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s DEFAULT %s COMMENT %s", columnName, columnType, dataDefault, comment))
					}
				case columnCollation == "" && comment == "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULLSTRING) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s", columnName, columnType))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s DEFAULT %s", columnName, columnType, dataDefault))
					}
				default:
					return tableColumns, fmt.Errorf("oracle table [%s.%s] column [%s] nulllable [NULL] panic, collation [%s] comment [%s]", t.Datasource.SchemaNameS, t.Datasource.TableNameS, columnName, columnCollation, comment)
				}
			}

			if strings.EqualFold(nullable, "NOT NULL") {
				switch {
				case columnCollation != "" && comment != "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULLSTRING) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s COMMENT %s", columnName, columnType, columnCollation, nullable, comment))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s DEFAULT %s COMMENT %s", columnName, columnType, columnCollation, nullable, dataDefault, comment))
					}
				case columnCollation != "" && comment == "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULLSTRING) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s", columnName, columnType, columnCollation, nullable))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s COLLATE %s %s DEFAULT %s", columnName, columnType, columnCollation, nullable, dataDefault))
					}
				case columnCollation == "" && comment != "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULLSTRING) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s COMMENT %s", columnName, columnType, nullable, comment))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s DEFAULT %s COMMENT %s", columnName, columnType, nullable, dataDefault, comment))
					}
				case columnCollation == "" && comment == "":
					if strings.EqualFold(dataDefault, constant.OracleDatabaseTableColumnDefaultValueWithNULLSTRING) {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s", columnName, columnType, nullable))
					} else {
						tableColumns = append(tableColumns, fmt.Sprintf("`%s` %s %s DEFAULT %s", columnName, columnType, nullable, dataDefault))
					}
				default:
					return tableColumns, fmt.Errorf("oracle table [%s.%s] column [%s] nulllable [NOT NULL] panic, collation [%s] comment [%s]", t.Datasource.SchemaNameS, t.Datasource.TableNameS, columnName, columnCollation, comment)
				}
			}

		default:
			return tableColumns, fmt.Errorf("oracle current taskflow [%s] generating table columns isn't support, please contact author or reselect", t.TaskFlow)
		}
	}

	return tableColumns, nil
}

func (t *Table) GenTableColumnComment() ([]string, error) {
	switch {
	case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
		// the GenTableColumns function had done
		return nil, nil
	default:
		return nil, fmt.Errorf("oracle current taskflow [%s] isn't support, please contact author or reselect", t.TaskFlow)
	}
}

func (t *Table) String() string {
	jsonStr, _ := stringutil.MarshalJSON(t)
	return jsonStr
}
