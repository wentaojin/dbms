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
package mapping

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/wentaojin/dbms/model/buildin"
	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/model/migrate"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func MYSQLDatabaseTableColumnMapORACLECompatibleDatatypeRule(c *Column, buildinDatatypes []*buildin.BuildinDatatypeRule) (string, string, error) {
	var (
		// origin column datatype
		originColumnType string
		// build-in column datatype
		buildInColumnType string
	)

	dataLength, err := strconv.Atoi(c.DataLength)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("column [%s] data_length string to int failed: %v", c.ColumnName, err)
	}

	// https://stackoverflow.com/questions/5634104/what-is-the-size-of-column-of-int11-in-mysql-in-bytes
	// MySQL Numeric Data Type Only in term of display, t
	// INT(x) will make difference only in term of display, that is to show the number in x digits, and not restricted to 11.
	// You pair it using ZEROFILL, which will prepend the zeros until it matches your length.
	// So, for any number of x in INT(x) if the stored value has less digits than x, ZEROFILL will prepend zeros.
	// ignore length of numeric types
	dataPrecision, err := strconv.Atoi(c.DataPrecision)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("column [%s] data_precision string to int failed: %v", c.ColumnName, err)
	}
	dataScale, err := strconv.Atoi(c.DataScale)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("column [%s] data_scale string to int failed: %v", c.ColumnName, err)
	}
	datetimePrecision, err := strconv.Atoi(c.DatetimePrecision)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datetime_precision string to int failed: %v", c.ColumnName, err)
	}

	// build-in column datatype rule
	buildinDatatypeMap := make(map[string]string)
	for _, b := range buildinDatatypes {
		buildinDatatypeMap[stringutil.StringUpper(b.DatatypeNameS)] = b.DatatypeNameT
	}

	// mysql -> oracle compatible database
	// https://docs.oracle.com/cd/E12151_01/doc.150/e12155/oracle_mysql_compared.htm#BABHHAJC
	switch stringutil.StringUpper(c.Datatype) {
	case constant.BuildInMySQLDatatypeTinyint:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInMySQLDatatypeTinyint, dataPrecision)
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeTinyint]; ok {
			buildInColumnType = fmt.Sprintf("%s(3,0)", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeTinyint)
		}

	case constant.BuildInMySQLDatatypeSmallint:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInMySQLDatatypeSmallint, dataPrecision)
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeSmallint]; ok {
			buildInColumnType = fmt.Sprintf("%s(5,0)", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeSmallint)
		}
	case constant.BuildInMySQLDatatypeMediumint:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInMySQLDatatypeMediumint, dataPrecision)
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeMediumint]; ok {
			buildInColumnType = fmt.Sprintf("%s(7,0)", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeMediumint)
		}
	case constant.BuildInMySQLDatatypeInt:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInMySQLDatatypeInt, dataPrecision)
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeInt]; ok {
			buildInColumnType = fmt.Sprintf("%s(10,0)", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeInt)
		}
	case constant.BuildInMySQLDatatypeBigint:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInMySQLDatatypeBigint, dataPrecision)
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeBigint]; ok {
			buildInColumnType = fmt.Sprintf("%s(19,0)", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeBigint)
		}
	case constant.BuildInMySQLDatatypeFloat:
		originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInMySQLDatatypeFloat, dataPrecision, dataScale)
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeFloat]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeFloat)
		}
	case constant.BuildInMySQLDatatypeDouble:
		originColumnType = fmt.Sprintf("%s", constant.BuildInMySQLDatatypeDouble)
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeDouble]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeDouble)
		}
	case constant.BuildInMySQLDatatypeDecimal:
		originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInMySQLDatatypeDecimal, dataPrecision, dataScale)
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeDecimal]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeDecimal)
		}

	case constant.BuildInMySQLDatatypeReal:
		originColumnType = constant.BuildInMySQLDatatypeReal
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeReal]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeReal)
		}
	case constant.BuildInMySQLDatatypeYear:
		originColumnType = constant.BuildInMySQLDatatypeYear
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeYear]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeYear)
		}

	case constant.BuildInMySQLDatatypeTime:
		if datetimePrecision == 0 {
			originColumnType = fmt.Sprintf("%s", constant.BuildInMySQLDatatypeTime)
		} else {
			originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInMySQLDatatypeTime, datetimePrecision)
		}
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeTime]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeTime)
		}
	case constant.BuildInMySQLDatatypeDate:
		originColumnType = constant.BuildInMySQLDatatypeDate
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeDate]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeDate)
		}
	case constant.BuildInMySQLDatatypeDatetime:
		if datetimePrecision == 0 {
			originColumnType = fmt.Sprintf("%s", constant.BuildInMySQLDatatypeDatetime)
		} else {
			originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInMySQLDatatypeDatetime, datetimePrecision)
		}
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeDatetime]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeDatetime)
		}
	case constant.BuildInMySQLDatatypeTimestamp:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInMySQLDatatypeTimestamp, datetimePrecision)
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeTimestamp]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), datetimePrecision)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeTimestamp)
		}
	case constant.BuildInMySQLDatatypeChar:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInMySQLDatatypeChar, dataLength)
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeChar]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d CHAR)", stringutil.StringUpper(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeChar)
		}
	case constant.BuildInMySQLDatatypeVarchar:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInMySQLDatatypeVarchar, dataLength)
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeVarchar]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d CHAR)", stringutil.StringUpper(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeVarchar)
		}
	case constant.BuildInMySQLDatatypeTinyText:
		originColumnType = constant.BuildInMySQLDatatypeTinyText
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeTinyText]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d CHAR)", stringutil.StringUpper(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeTinyText)
		}
	case constant.BuildInMySQLDatatypeText:
		originColumnType = constant.BuildInMySQLDatatypeText
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeText]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeText)
		}
		// ORA-43912: invalid collation specified for a CLOB or NCLOB value
		// columnCollation = ""

	case constant.BuildInMySQLDatatypeMediumText:
		originColumnType = constant.BuildInMySQLDatatypeMediumText
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeMediumText]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeMediumText)
		}
		// ORA-43912: invalid collation specified for a CLOB or NCLOB value
		// columnCollation = ""

	case constant.BuildInMySQLDatatypeLongText:
		originColumnType = constant.BuildInMySQLDatatypeLongText
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeLongText]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeLongText)
		}
		// ORA-43912: invalid collation specified for a CLOB or NCLOB value
		// columnCollation = ""

	case constant.BuildInMySQLDatatypeBit:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInMySQLDatatypeBit, dataPrecision)
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeBit]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), dataPrecision)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeBit)
		}
	case constant.BuildInMySQLDatatypeBinary:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInMySQLDatatypeBinary, dataPrecision)
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeBinary]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), dataPrecision)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeBinary)
		}
	case constant.BuildInMySQLDatatypeVarbinary:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInMySQLDatatypeVarbinary, dataLength)
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeVarbinary]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeVarbinary)
		}
	case constant.BuildInMySQLDatatypeTinyBlob:
		originColumnType = constant.BuildInMySQLDatatypeTinyBlob
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeTinyBlob]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeTinyBlob)
		}
	case constant.BuildInMySQLDatatypeBlob:
		originColumnType = constant.BuildInMySQLDatatypeBlob
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeBlob]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeBlob)
		}
	case constant.BuildInMySQLDatatypeMediumBlob:
		originColumnType = constant.BuildInMySQLDatatypeMediumBlob
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeMediumBlob]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeMediumBlob)
		}
	case constant.BuildInMySQLDatatypeLongBlob:
		originColumnType = constant.BuildInMySQLDatatypeLongBlob
		if val, ok := buildinDatatypeMap[constant.BuildInMySQLDatatypeLongBlob]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%s] map oracle column type rule isn't exist, please checkin", constant.BuildInMySQLDatatypeLongBlob)
		}
	case constant.BuildInMySQLDatatypeEnum:
		// originColumnType = "ENUM"
		// buildInColumnType = fmt.Sprintf("VARCHAR2(%d CHAR)", dataLength)
		// return originColumnType, buildInColumnType, nil
		return originColumnType, buildInColumnType, fmt.Errorf("oracle compatible database isn't support data type ENUM, please manual check")

	case constant.BuildInMySQLDatatypeSet:
		// originColumnType = "SET"
		// buildInColumnType = fmt.Sprintf("VARCHAR2(%d CHAR)", dataLength)
		// return originColumnType, buildInColumnType, nil
		return originColumnType, buildInColumnType, fmt.Errorf("oracle compatible database isn't support data type SET, please manual check")

	default:
		return originColumnType, buildInColumnType, fmt.Errorf("mysql compatible database table column type [%v] isn't support, please contact author or reselect", stringutil.StringUpper(c.Datatype))
	}
}

// MYSQLHandleColumnRuleWithPriority priority, return column datatype and default value
// column > table > schema > task
func MYSQLHandleColumnRuleWithPriority(originSourceTable, originColumnName, originDatatype string, buildInDatatype, originDefaultValue, sourceCharset, targetCharset string, buildinDefaultValueRules []*buildin.BuildinDefaultvalRule, taskRules []*migrate.TaskStructRule, schemaRules []*migrate.SchemaStructRule, tableRules []*migrate.TableStructRule, columnRules []*migrate.ColumnStructRule) (string, string, error) {
	var (
		datatype, defaultValue string
		err                    error
	)
	columnDatatypes, columnDefaultValues := mysqlHandleColumnRuleWithColumnHighPriority(originColumnName, columnRules)

	globalDatatypes, globalDefaultValues := mysqlHandelColumnRuleWithTableSchemaTaskPriority(originSourceTable, buildinDefaultValueRules, taskRules, schemaRules, tableRules)

	// column default value
	defaultValue, err = MYSQLHandleColumnRuleWitheDefaultValuePriority(originColumnName, originDatatype, originDefaultValue, sourceCharset, targetCharset, columnDefaultValues, globalDefaultValues)
	if err != nil {
		return datatype, defaultValue, err
	}

	// column high priority
	if len(columnDatatypes) == 0 && len(columnDefaultValues) == 0 {
		// global priority
		for _, ruleColumnTypeT := range globalDatatypes {
			return ruleColumnTypeT, defaultValue, nil
		}

		datatype = buildInDatatype
		return datatype, defaultValue, nil
	}

	for customColName, columnRuleMap := range columnDatatypes {
		// global priority
		if len(columnRuleMap) == 0 {
			for _, ruleColumnTypeT := range globalDatatypes {
				return ruleColumnTypeT, defaultValue, nil
			}

			datatype = buildInDatatype
			return datatype, defaultValue, nil
		}
		// column priority
		// case field rule
		if originColumnName == customColName {
			for _, ruleColumnTypeT := range columnRuleMap {
				return ruleColumnTypeT, defaultValue, nil
			}
		}
	}
	datatype = buildInDatatype
	return datatype, defaultValue, nil
}

func MYSQLHandleColumnRuleWitheDefaultValuePriority(columnName, datatype, originDefaultValue, sourceCharset, targetCharset string, columnCustomDefaultValue map[string]map[string]string, columnGlobalDefaultValue map[string]string) (string, error) {
	if len(columnCustomDefaultValue) == 0 && len(columnGlobalDefaultValue) == 0 {
		// string data charset processing
		dataDefault, err := mysqlHandleColumnDefaultValueCharset(columnName, datatype, originDefaultValue, sourceCharset, targetCharset)
		if err != nil {
			return originDefaultValue, err
		}
		return dataDefault, nil
	}

	// priority
	// column > global
	var columnLevelVal, globalLevelVal string
	if len(columnCustomDefaultValue) != 0 {
		if vals, exist := columnCustomDefaultValue[columnName]; exist {
			for k, v := range vals {
				if strings.EqualFold(strings.TrimSpace(k), strings.TrimSpace(originDefaultValue)) {
					columnLevelVal = v
					// break skip
					break
				}
			}
		}
	}

	if len(columnGlobalDefaultValue) != 0 {
		for k, v := range columnGlobalDefaultValue {
			if strings.EqualFold(strings.TrimSpace(k), strings.TrimSpace(originDefaultValue)) {
				globalLevelVal = v
				// break skip
				break
			}
		}
	}

	if !strings.EqualFold(columnLevelVal, "") {
		originDefaultValue = columnLevelVal
	}

	if strings.EqualFold(columnLevelVal, "") && !strings.EqualFold(globalLevelVal, "") {
		originDefaultValue = globalLevelVal
	}

	dataDefault, err := mysqlHandleColumnDefaultValueCharset(columnName, datatype, strings.TrimSpace(originDefaultValue), constant.CharsetUTF8MB4, targetCharset)
	if err != nil {
		return originDefaultValue, err
	}
	return dataDefault, nil
}

// mysqlHandelColumnRuleWithTableSchemaTaskPriority priority
// table > schema > task -> server
func mysqlHandelColumnRuleWithTableSchemaTaskPriority(sourceTable string, buildinDefaultValueRules []*buildin.BuildinDefaultvalRule, taskRules []*migrate.TaskStructRule, schemaRules []*migrate.SchemaStructRule, tableRules []*migrate.TableStructRule) (map[string]string, map[string]string) {
	taskDatatype, taskDefaultVal := mysqlHandleColumnRuleWithTaskLevel(taskRules)

	schemaDatatype, schemaDefaultVal := mysqlHandleColumnRuleWithSchemaLevel(schemaRules)

	tableDatatype, tableDefaultVal := mysqlHandleColumnRuleWithTableLevel(sourceTable, tableRules)

	buildinDefaultValues := mysqlHandleColumnDefaultValueRuleWithServerLevel(buildinDefaultValueRules)

	globalColumnDatatype := stringutil.ExchangeStringDict(tableDatatype, stringutil.ExchangeStringDict(schemaDatatype, taskDatatype))

	globalColumnDefaultVal := stringutil.ExchangeStringDict(tableDefaultVal, stringutil.ExchangeStringDict(schemaDefaultVal, stringutil.ExchangeStringDict(taskDefaultVal, buildinDefaultValues)))

	return globalColumnDatatype, globalColumnDefaultVal
}

// mysqlHandleColumnRuleWithColumnHighPriority priority
// column high priority
func mysqlHandleColumnRuleWithColumnHighPriority(columNameS string, columnRules []*migrate.ColumnStructRule) (map[string]map[string]string, map[string]map[string]string) {
	columnDatatypeMap := make(map[string]map[string]string)
	columnDefaultValMap := make(map[string]map[string]string)

	if len(columnRules) == 0 {
		return columnDatatypeMap, columnDefaultValMap
	}

	datatypeMap := make(map[string]string)
	defaultValMap := make(map[string]string)
	for _, cr := range columnRules {
		if strings.EqualFold(columNameS, cr.ColumnNameS) {
			// exclude columnType "", represent it's not configure column datatype rule
			if !strings.EqualFold(cr.ColumnTypeS, "") && !strings.EqualFold(cr.ColumnTypeT, "") {
				datatypeMap[cr.ColumnTypeS] = cr.ColumnTypeT
			}
			if !strings.EqualFold(cr.DefaultValueS, "") && !strings.EqualFold(cr.DefaultValueT, "") {
				defaultValMap[cr.DefaultValueS] = cr.DefaultValueT
			}
		}
	}
	if datatypeMap != nil {
		columnDatatypeMap[columNameS] = datatypeMap
	}
	if defaultValMap != nil {
		columnDefaultValMap[columNameS] = defaultValMap
	}

	return columnDatatypeMap, columnDefaultValMap
}

func mysqlHandleColumnDefaultValueRuleWithServerLevel(buildinRules []*buildin.BuildinDefaultvalRule) map[string]string {
	columnDefaultValMap := make(map[string]string)

	if len(buildinRules) == 0 {
		return columnDefaultValMap
	}

	for _, t := range buildinRules {
		if !strings.EqualFold(t.DefaultValueS, "") && !strings.EqualFold(t.DefaultValueT, "") {
			columnDefaultValMap[t.DefaultValueS] = t.DefaultValueT
		}
	}
	return columnDefaultValMap
}

func mysqlHandleColumnRuleWithTaskLevel(taskRules []*migrate.TaskStructRule) (map[string]string, map[string]string) {
	columnDatatypeMap := make(map[string]string)
	columnDefaultValMap := make(map[string]string)

	if len(taskRules) == 0 {
		return columnDatatypeMap, columnDefaultValMap
	}

	for _, t := range taskRules {
		// exclude columnType "", represent it's not configure column datatype rule
		if !strings.EqualFold(t.ColumnTypeS, "") && !strings.EqualFold(t.ColumnTypeT, "") {
			columnDatatypeMap[t.ColumnTypeS] = t.ColumnTypeT
		}
		if !strings.EqualFold(t.DefaultValueS, "") && !strings.EqualFold(t.DefaultValueT, "") {
			columnDefaultValMap[t.DefaultValueS] = t.DefaultValueT
		}
	}
	return columnDatatypeMap, columnDefaultValMap
}

func mysqlHandleColumnRuleWithSchemaLevel(schemaRules []*migrate.SchemaStructRule) (map[string]string, map[string]string) {
	columnDatatypeMap := make(map[string]string)
	columnDefaultValMap := make(map[string]string)

	if len(schemaRules) == 0 {
		return columnDatatypeMap, columnDefaultValMap
	}

	for _, t := range schemaRules {
		// exclude columnType "", represent it's not configure column datatype rule
		if !strings.EqualFold(t.ColumnTypeS, "") && !strings.EqualFold(t.ColumnTypeT, "") {
			columnDatatypeMap[t.ColumnTypeS] = t.ColumnTypeT
		}
		if !strings.EqualFold(t.DefaultValueS, "") && !strings.EqualFold(t.DefaultValueT, "") {
			columnDefaultValMap[t.DefaultValueS] = t.DefaultValueT
		}
	}
	return columnDatatypeMap, columnDefaultValMap
}

func mysqlHandleColumnRuleWithTableLevel(sourceTable string, tableRules []*migrate.TableStructRule) (map[string]string, map[string]string) {
	columnDatatypeMap := make(map[string]string)
	columnDefaultValMap := make(map[string]string)

	if len(tableRules) == 0 {
		return columnDatatypeMap, columnDefaultValMap
	}

	for _, t := range tableRules {
		if sourceTable == t.TableNameS {
			// exclude columnType "", represent it's not configure column datatype rule
			if !strings.EqualFold(t.ColumnTypeS, "") && !strings.EqualFold(t.ColumnTypeT, "") {
				columnDatatypeMap[t.ColumnTypeS] = t.ColumnTypeT
			}
			if !strings.EqualFold(t.DefaultValueS, "") && !strings.EqualFold(t.DefaultValueT, "") {
				columnDefaultValMap[t.DefaultValueS] = t.DefaultValueT
			}
		}
	}
	return columnDatatypeMap, columnDefaultValMap
}

func mysqlHandleColumnDefaultValueCharset(columnName, datatype, defaultVal, sourceCharset, targetCharset string) (string, error) {
	var dataDefault string

	// column default value is '', direct return
	if strings.EqualFold(defaultVal, "''") {
		return defaultVal, nil
	}

	if !strings.EqualFold(defaultVal, "") {
		// MySQL compatible database default value character insensitive
		reg, err := regexp.Compile(`^.+\(\)$`)
		if err != nil {
			return "", err
		}
		if stringutil.IsContainedString(constant.MYSQLCompatibleDatabaseTableColumnDatatypeStringDefaultValueApostrophe, stringutil.StringUpper(datatype)) {
			if !reg.MatchString(defaultVal) || !strings.EqualFold(defaultVal, "CURRENT_TIMESTAMP") || !strings.EqualFold(defaultVal, "") {
				dataDefault = stringutil.StringBuilder(`'`, dataDefault, `'`)
			}
		}
	}

	isTrunc := false
	if strings.HasPrefix(defaultVal, "'") && strings.HasSuffix(defaultVal, "'") {
		isTrunc = true
		defaultVal = defaultVal[1 : len(defaultVal)-1]
	}
	convertUtf8Raw, err := stringutil.CharsetConvert([]byte(defaultVal), sourceCharset, constant.CharsetUTF8MB4)
	if err != nil {
		return defaultVal, fmt.Errorf("column [%s] data default charset convert failed, %v", columnName, err)
	}

	convertTargetRaw, err := stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, targetCharset)
	if err != nil {
		return defaultVal, fmt.Errorf("column [%s] data default charset convert failed, %v", columnName, err)
	}
	if isTrunc {
		// 'K'
		dataDefault = "'" + stringutil.BytesToString(convertTargetRaw) + "'"
	} else {
		if strings.EqualFold(stringutil.BytesToString(convertTargetRaw), constant.MYSQLDatabaseTableColumnDefaultValueWithEmptyString) {
			dataDefault = "'" + stringutil.BytesToString(convertTargetRaw) + "'"
		} else if strings.EqualFold(stringutil.BytesToString(convertTargetRaw), constant.MYSQLDatabaseTableColumnDefaultValueWithNULLSTRING) {
			dataDefault = constant.MYSQLDatabaseTableColumnDefaultValueWithNULL
		} else {
			dataDefault = stringutil.BytesToString(convertTargetRaw)
		}
	}
	return dataDefault, nil
}
