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
	"context"
	"fmt"
	"github.com/wentaojin/dbms/database"
	"strings"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type DataScanRule struct {
	Ctx               context.Context    `json:"-"`
	TaskName          string             `json:"taskName"`
	TaskMode          string             `json:"taskMode"`
	TaskFlow          string             `json:"taskFlow"`
	SchemaNameS       string             `json:"schemaNameS"`
	TableNameS        string             `json:"tableNameS"`
	GlobalSamplerateS string             `json:"globalSamplerateS"`
	GlobalSqlHintS    string             `json:"globalSqlHintS"`
	TableTypeS        map[string]string  `json:"tableTypeS"`
	DBCollationS      bool               `json:"DBCollationS"`
	DatabaseS         database.IDatabase `json:"-"`
	DBCharsetS        string             `json:"DBCharsetS"`
}

func (r *DataScanRule) GenSchemaNameRule() (string, error) {
	//routeRule, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(r.Ctx, &rule.SchemaRouteRule{
	//	TaskName: r.TaskName, SchemaNameS: r.SchemaNameS})
	//if err != nil {
	//	return "", err
	//}
	convertUtf8Raw, err := stringutil.CharsetConvert([]byte(r.SchemaNameS), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
	if err != nil {
		return "", fmt.Errorf("[GetSchemaNameRule] oracle schema [%s] charset convert failed, %v", r.SchemaNameS, err)
	}
	schemaNameS := stringutil.BytesToString(convertUtf8Raw)

	return schemaNameS, nil
}

func (r *DataScanRule) GenTableNameRule() (string, error) {
	convertUtf8Raw, err := stringutil.CharsetConvert([]byte(r.TableNameS), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
	if err != nil {
		return "", fmt.Errorf("[GetTableNameRule] oracle schema [%s] table [%v] charset convert failed, %v", r.SchemaNameS, r.TableNameS, err)
	}
	tableNameS := stringutil.BytesToString(convertUtf8Raw)
	return tableNameS, nil
}

func (r *DataScanRule) GenTableColumnNameRule() (string, string, error) {
	var (
		columnDetailSli, groupColumnSli []string
	)

	sourceColumnInfos, err := r.DatabaseS.GetDatabaseTableColumnInfo(r.SchemaNameS, r.TableNameS, r.DBCollationS)
	if err != nil {
		return "", "", err
	}

	switch {
	case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB):
		for _, rowCol := range sourceColumnInfos {
			columnName := rowCol["COLUMN_NAME"]
			datatypeName := rowCol["DATA_TYPE"]
			dataPrecision := rowCol["DATA_PRECISION"]
			dataScale := rowCol["DATA_SCALE"]
			// NUMBER(38,127) = NUMBER/NUMBER(*)
			if strings.EqualFold(datatypeName, constant.BuildInOracleDatatypeNumber) && dataPrecision == "38" && dataScale == "127" {
				datatype := r.genMYSQLCompatibleDatabaseOracleNumberColumnDatatype(columnName)
				columnDetailSli = append(columnDetailSli, datatype)
				groupColumnSli = append(groupColumnSli, fmt.Sprintf("%s_CATEGORY", columnName))
			}
		}
		return stringutil.StringJoin(columnDetailSli, constant.StringSeparatorComma), stringutil.StringJoin(groupColumnSli, constant.StringSeparatorComma), nil
	default:
		return "", "", fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] is not supoort, please contact author or reselect", r.TaskName, r.TaskMode, r.TaskFlow)
	}
}

func (r *DataScanRule) GenSchemaTableTypeRule() string {
	return r.TableTypeS[r.TableNameS]
}

func (r *DataScanRule) GenSchemaTableCustomRule() (string, string, error) {
	isRecord, err := model.GetIDataScanRuleRW().IsContainedDataScanRuleRecord(r.Ctx, &rule.DataScanRule{
		TaskName:    r.TaskName,
		SchemaNameS: r.SchemaNameS,
		TableNameS:  r.TableNameS,
	})
	if err != nil {
		return "", "", err
	}
	if !isRecord {
		return r.GlobalSamplerateS, r.GlobalSqlHintS, nil
	}

	mr, err := model.GetIDataScanRuleRW().GetDataScanRule(r.Ctx, &rule.DataScanRule{
		TaskName:    r.TaskName,
		SchemaNameS: r.SchemaNameS,
		TableNameS:  r.TableNameS,
	})
	if err != nil {
		return "", "", err
	}

	if strings.EqualFold(mr.TableSamplerateS, "") && strings.EqualFold(mr.SqlHintS, "") {
		return r.GlobalSamplerateS, r.GlobalSqlHintS, nil
	}
	if !strings.EqualFold(mr.TableSamplerateS, "") && strings.EqualFold(mr.SqlHintS, "") {
		return mr.TableSamplerateS, r.GlobalSqlHintS, nil
	}
	if strings.EqualFold(mr.TableSamplerateS, "") && !strings.EqualFold(mr.SqlHintS, "") {
		return r.GlobalSamplerateS, mr.SqlHintS, nil
	}
	return mr.TableSamplerateS, mr.SqlHintS, nil
}

func (r *DataScanRule) genMYSQLCompatibleDatabaseOracleNumberColumnDatatype(columnName string) string {
	return fmt.Sprintf(`CASE
		WHEN %[1]s BETWEEN %v AND %v
		AND %[1]s = TRUNC(%[1]s) THEN 'BIGINT'
		WHEN %[1]s BETWEEN %v AND %v
		AND %[1]s = TRUNC(%[1]s) THEN 'BIGINT_UNSIGNED'
		WHEN %[1]s >= %v
		AND LENGTH(%[1]s) <= 65
		AND %[1]s = TRUNC(%[1]s) THEN 'DECIMAL_INT'
		WHEN %[1]s - TRUNC(%[1]s) != 0 THEN 'DECIMAL_POINT'
		ELSE 'UNKNOWN'
	END AS %[1]s_CATEGORY`,
		columnName,
		constant.DefaultMYSQLCompatibleBigintLowBound,
		constant.DefaultMYSQLCompatibleBigintUpperBound,
		constant.DefaultMYSQLCompatibleBigintUnsignedLowBound,
		constant.DefaultMYSQLCompatibleBigintUnsignedUpperBound,
		constant.DefaultMYSQLCompatibleDecimalLowerBound)
}
