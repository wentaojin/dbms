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
	"fmt"
	"strings"

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

// Bound represents a bound for a column
type Bound struct {
	ColumnNameS string `json:"columnNameS"`
	ColumnNameT string `json:"columnNameT"`
	CollationS  string `json:"collationS"`
	CollationT  string `json:"collationT"`
	LowerS      string `json:"lowerS"`
	UpperS      string `json:"upperS"`
	LowerT      string `json:"lowerT"`
	UpperT      string `json:"upperT"`

	HasLower bool `json:"hasLower"`
	HasUpper bool `json:"hasUpper"`
}

// Range represents chunk range
type Range struct {
	DBTypeS     string         `json:"dbTypeS"`
	DBTypeT     string         `json:"dbTypeT"`
	Bounds      []*Bound       `json:"bounds"`
	BoundOffset map[string]int `json:"boundOffset"`
}

// NewRange return a Range.
func NewRange() *Range {
	return &Range{
		Bounds:      make([]*Bound, 0, 2),
		BoundOffset: make(map[string]int),
	}
}

// String returns the string of Range, used for log.
func (rg *Range) String() string {
	chunkBytes, _ := stringutil.MarshalJSON(rg)
	return chunkBytes
}

func (bd *Bound) String() string {
	chunkBytes, _ := stringutil.MarshalJSON(bd)
	return chunkBytes
}

func (rg *Range) addBound(bound *Bound) {
	rg.Bounds = append(rg.Bounds, bound)
	rg.BoundOffset[bound.ColumnNameS] = len(rg.Bounds) - 1
}

func (rg *Range) ToStringS() string {
	/* for example:
	there is a bucket, the lowerbound and upperbound are (v1, v3), (v2, v4), and the columns are `a` and `b`,
	this bucket's data range is (a > v1 or (a == v1 and b > v3)) and (a < v2 or (a == v2 and b <= v4))
	*/

	lowerConditionS := make([]string, 0, 1)
	upperConditionS := make([]string, 0, 1)

	preConditionForLowerS := make([]string, 0, 1)
	preConditionForUpperS := make([]string, 0, 1)
	preConditionArgsForLowerS := make([]string, 0, 1)
	preConditionArgsForUpperS := make([]string, 0, 1)

	for i, bound := range rg.Bounds {
		lowerSymbol := constant.DataCompareSymbolGt
		upperSymbol := constant.DataCompareSymbolLt
		if i == len(rg.Bounds)-1 {
			upperSymbol = constant.DataCompareSymbolLte
		}

		if bound.HasLower {
			if strings.EqualFold(rg.DBTypeS, constant.DatabaseTypeOracle) {
				if len(preConditionForLowerS) > 0 {
					lowerConditionS = append(lowerConditionS, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForLowerS, " AND "), stringutil.StringBuilder("\"", bound.ColumnNameS, "\""), lowerSymbol, bound.LowerS))
				} else {
					lowerConditionS = append(lowerConditionS, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("\"", bound.ColumnNameS, "\""), lowerSymbol, bound.LowerS))
				}
				preConditionForLowerS = append(preConditionForLowerS, fmt.Sprintf("%s = %s", stringutil.StringBuilder("\"", bound.ColumnNameS, "\""), bound.LowerS))
				preConditionArgsForLowerS = append(preConditionArgsForLowerS, bound.LowerS)

			}

			if strings.EqualFold(rg.DBTypeS, constant.DatabaseTypeMySQL) || strings.EqualFold(rg.DBTypeS, constant.DatabaseTypeTiDB) {
				if len(preConditionForLowerS) > 0 {
					lowerConditionS = append(lowerConditionS, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForLowerS, " AND "), stringutil.StringBuilder("`", bound.ColumnNameS, "`"), lowerSymbol, bound.LowerS))
				} else {
					lowerConditionS = append(lowerConditionS, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("`", bound.ColumnNameS, "`"), lowerSymbol, bound.LowerS))
				}
				preConditionForLowerS = append(preConditionForLowerS, fmt.Sprintf("%s = %s", stringutil.StringBuilder("`", bound.ColumnNameS, "`"), bound.LowerS))
				preConditionArgsForLowerS = append(preConditionArgsForLowerS, bound.LowerS)
			}
		}

		if bound.HasUpper {
			if strings.EqualFold(rg.DBTypeS, constant.DatabaseTypeOracle) {
				if len(preConditionForUpperS) > 0 {
					upperConditionS = append(upperConditionS, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForUpperS, " AND "), stringutil.StringBuilder("\"", bound.ColumnNameS, "\""), upperSymbol, bound.UpperS))
				} else {
					upperConditionS = append(upperConditionS, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("\"", bound.ColumnNameS, "\""), upperSymbol, bound.UpperS))
				}
				preConditionForUpperS = append(preConditionForUpperS, fmt.Sprintf("%s = %s", stringutil.StringBuilder("\"", bound.ColumnNameS, "\""), bound.UpperS))
				preConditionArgsForUpperS = append(preConditionArgsForUpperS, bound.UpperS)
			}

			if strings.EqualFold(rg.DBTypeS, constant.DatabaseTypeMySQL) || strings.EqualFold(rg.DBTypeS, constant.DatabaseTypeTiDB) {
				if len(preConditionForUpperS) > 0 {
					upperConditionS = append(upperConditionS, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForUpperS, " AND "), stringutil.StringBuilder("`", bound.ColumnNameS, "`"), upperSymbol, bound.UpperS))
				} else {
					upperConditionS = append(upperConditionS, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("`", bound.ColumnNameS, "`"), upperSymbol, bound.UpperS))
				}
				preConditionForUpperS = append(preConditionForUpperS, fmt.Sprintf("%s = ?", stringutil.StringBuilder("`", bound.ColumnNameS, "`")))
				preConditionArgsForUpperS = append(preConditionArgsForUpperS, bound.UpperS)
			}

		}
	}

	var (
		toStringS string
	)

	if len(upperConditionS) == 0 && len(lowerConditionS) == 0 {
		toStringS = "1 = 1"
	}

	switch {
	case len(upperConditionS) == 0:
		toStringS = stringutil.StringJoin(lowerConditionS, " OR ")
	case len(lowerConditionS) == 0:
		toStringS = stringutil.StringJoin(upperConditionS, " OR ")
	default:
		toStringS = fmt.Sprintf("(%s) AND (%s)", stringutil.StringJoin(lowerConditionS, " OR "), stringutil.StringJoin(upperConditionS, " OR "))
	}

	return toStringS
}

func (rg *Range) ToStringT() string {
	/* for example:
	there is a bucket, the lowerbound and upperbound are (v1, v3), (v2, v4), and the columns are `a` and `b`,
	this bucket's data range is (a > v1 or (a == v1 and b > v3)) and (a < v2 or (a == v2 and b <= v4))
	*/

	lowerConditionT := make([]string, 0, 1)
	upperConditionT := make([]string, 0, 1)

	preConditionForLowerT := make([]string, 0, 1)
	preConditionForUpperT := make([]string, 0, 1)
	preConditionArgsForLowerT := make([]string, 0, 1)
	preConditionArgsForUpperT := make([]string, 0, 1)

	for i, bound := range rg.Bounds {
		lowerSymbol := constant.DataCompareSymbolGt
		upperSymbol := constant.DataCompareSymbolLt
		if i == len(rg.Bounds)-1 {
			upperSymbol = constant.DataCompareSymbolLte
		}

		if bound.HasLower {
			if strings.EqualFold(rg.DBTypeT, constant.DatabaseTypeOracle) {
				if len(preConditionForLowerT) > 0 {
					lowerConditionT = append(lowerConditionT, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForLowerT, " AND "), stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnNameT, "\"", bound.CollationT, ")"), lowerSymbol, stringutil.StringBuilder("NLSSORT('", bound.LowerT, "',", bound.CollationT, ")")))
				} else {
					lowerConditionT = append(lowerConditionT, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnNameT, "\"", bound.CollationT, ")"), lowerSymbol, stringutil.StringBuilder("NLSSORT('", bound.LowerT, "',", bound.CollationT, ")")))
				}
				preConditionForLowerT = append(preConditionForLowerT, fmt.Sprintf("%s = %s", stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnNameT, "\"", bound.CollationT, ")"), stringutil.StringBuilder("NLSSORT('", bound.LowerT, "',", bound.CollationT, ")")))
				preConditionArgsForLowerT = append(preConditionArgsForLowerT, stringutil.StringBuilder("NLSSORT('", bound.LowerT, "',", bound.CollationT, ")"))
			}

			if strings.EqualFold(rg.DBTypeT, constant.DatabaseTypeMySQL) || strings.EqualFold(rg.DBTypeT, constant.DatabaseTypeTiDB) {
				if len(preConditionForLowerT) > 0 {
					lowerConditionT = append(lowerConditionT, fmt.Sprintf("(%s AND %s%s %s %s)", stringutil.StringJoin(preConditionForLowerT, " AND "), stringutil.StringBuilder("`", bound.ColumnNameT, "`"), bound.CollationT, lowerSymbol, bound.LowerT))
				} else {
					lowerConditionT = append(lowerConditionT, fmt.Sprintf("(%s%s %s %s)", stringutil.StringBuilder("`", bound.ColumnNameT, "`"), bound.CollationT, lowerSymbol, bound.LowerT))
				}
				preConditionForLowerT = append(preConditionForLowerT, fmt.Sprintf("%s = %s", stringutil.StringBuilder("`", bound.ColumnNameT, "`"), bound.LowerT))
				preConditionArgsForLowerT = append(preConditionArgsForLowerT, bound.LowerT)
			}
		}

		if bound.HasUpper {
			if strings.EqualFold(rg.DBTypeT, constant.DatabaseTypeOracle) {
				if len(preConditionForUpperT) > 0 {
					upperConditionT = append(upperConditionT, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForUpperT, " AND "), stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnNameT, "\"", bound.CollationT, ")"), upperSymbol, stringutil.StringBuilder("NLSSORT('", bound.UpperT, "',", bound.CollationT, ")")))
				} else {
					upperConditionT = append(upperConditionT, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnNameT, "\"", bound.CollationT, ")"), upperSymbol, stringutil.StringBuilder("NLSSORT('", bound.UpperT, "',", bound.CollationT, ")")))
				}
				preConditionForUpperT = append(preConditionForUpperT, fmt.Sprintf("%s = %s", stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnNameT, "\"", bound.CollationT, ")"), stringutil.StringBuilder("NLSSORT('", bound.UpperT, "',", bound.CollationT, ")")))
				preConditionArgsForUpperT = append(preConditionArgsForUpperT, stringutil.StringBuilder("NLSSORT('", bound.UpperT, "',", bound.CollationT, ")"))
			}

			if strings.EqualFold(rg.DBTypeT, constant.DatabaseTypeMySQL) || strings.EqualFold(rg.DBTypeT, constant.DatabaseTypeTiDB) {
				if len(preConditionForUpperT) > 0 {
					upperConditionT = append(upperConditionT, fmt.Sprintf("(%s AND %s%s %s %s)", stringutil.StringJoin(preConditionForUpperT, " AND "), stringutil.StringBuilder("`", bound.ColumnNameT, "`"), bound.CollationT, upperSymbol, bound.UpperT))
				} else {
					upperConditionT = append(upperConditionT, fmt.Sprintf("(%s%s %s %s)", stringutil.StringBuilder("`", bound.ColumnNameT, "`"), bound.CollationT, upperSymbol, bound.UpperT))
				}
				preConditionForUpperT = append(preConditionForUpperT, fmt.Sprintf("%s = %s", stringutil.StringBuilder("`", bound.ColumnNameT, "`"), bound.UpperT))
				preConditionArgsForUpperT = append(preConditionArgsForUpperT, bound.UpperT)
			}
		}
	}

	if len(upperConditionT) == 0 && len(lowerConditionT) == 0 {
		return "1 = 1"
	}

	if len(upperConditionT) == 0 {
		return stringutil.StringJoin(lowerConditionT, " OR ")
	}
	if len(lowerConditionT) == 0 {
		return stringutil.StringJoin(upperConditionT, " OR ")
	}

	return fmt.Sprintf("(%s) AND (%s)", stringutil.StringJoin(lowerConditionT, " OR "), stringutil.StringJoin(upperConditionT, " OR "))
}

func (rg *Range) Update(taskFlow, dbCharsetS, dbCharsetT, columnS, columnT, collationS string, columnInfoMapS map[string]string, columnInfoMapT map[string]string, lower, upper string, updateLower, updateUpper bool) error {
	dbTypeSli := stringutil.StringSplit(taskFlow, constant.StringSeparatorAite)
	rg.DBTypeS = dbTypeSli[0]
	rg.DBTypeT = dbTypeSli[1]

	var (
		lowerS     string
		upperS     string
		lowerT     string
		upperT     string
		collationT string
	)
	switch {
	case strings.EqualFold(taskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(taskFlow, constant.TaskFlowOracleToTiDB):
		convertUtf8Raws, err := stringutil.CharsetConvert([]byte(lower), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(dbCharsetS)], constant.CharsetUTF8MB4)
		if err != nil {
			return err
		}
		lowerUtf8 := stringutil.BytesToString(convertUtf8Raws)

		convertUtf8Raws, err = stringutil.CharsetConvert([]byte(upper), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(dbCharsetS)], constant.CharsetUTF8MB4)
		if err != nil {
			return err
		}
		upperUtf8 := stringutil.BytesToString(convertUtf8Raws)

		if collationS != "" {
			switch {
			case strings.EqualFold(taskFlow, constant.TaskFlowOracleToTiDB):
				if stringutil.IsContainedString(constant.DataCompareMYSQLCompatibleDatabaseColumnDatatypeSupportCollation, stringutil.StringUpper(columnInfoMapT[columnT])) {
					collationT = fmt.Sprintf(" COLLATE '%s'", constant.MigrateTableStructureDatabaseCollationMap[constant.TaskFlowOracleToTiDB][stringutil.StringUpper(collationS)][constant.MigrateTableStructureDatabaseCharsetMap[constant.TaskFlowOracleToTiDB][stringutil.StringUpper(dbCharsetS)]])
				}
			case strings.EqualFold(taskFlow, constant.TaskFlowOracleToMySQL):
				if stringutil.IsContainedString(constant.DataCompareMYSQLCompatibleDatabaseColumnDatatypeSupportCollation, stringutil.StringUpper(columnInfoMapT[columnT])) {
					collationT = fmt.Sprintf(" COLLATE '%s'", constant.MigrateTableStructureDatabaseCollationMap[constant.TaskFlowOracleToMySQL][stringutil.StringUpper(collationS)][constant.MigrateTableStructureDatabaseCharsetMap[constant.TaskFlowOracleToMySQL][stringutil.StringUpper(dbCharsetS)]])
				}
			case strings.EqualFold(stringutil.StringBuilder(rg.DBTypeS, constant.StringSeparatorAite, rg.DBTypeT), constant.TaskFlowTiDBToOracle):
				if stringutil.IsContainedString(constant.DataCompareORACLECompatibleDatabaseColumnDatatypeSupportCollation, stringutil.StringUpper(columnInfoMapT[columnT])) {
					collationT = fmt.Sprintf(" 'NLS_SORT = %s'", constant.MigrateTableStructureDatabaseCollationMap[constant.TaskFlowTiDBToOracle][stringutil.StringUpper(collationS)][constant.MigrateTableStructureDatabaseCharsetMap[constant.TaskFlowTiDBToOracle][stringutil.StringUpper(dbCharsetS)]])
				}
			case strings.EqualFold(stringutil.StringBuilder(rg.DBTypeS, constant.StringSeparatorAite, rg.DBTypeT), constant.TaskFlowMySQLToOracle):
				if stringutil.IsContainedString(constant.DataCompareORACLECompatibleDatabaseColumnDatatypeSupportCollation, stringutil.StringUpper(columnInfoMapT[columnT])) {
					collationT = fmt.Sprintf(" 'NLS_SORT = %s'", constant.MigrateTableStructureDatabaseCollationMap[constant.TaskFlowMySQLToOracle][stringutil.StringUpper(collationS)][constant.MigrateTableStructureDatabaseCharsetMap[constant.TaskFlowMySQLToOracle][stringutil.StringUpper(dbCharsetS)]])
				}
			}
		}

		if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportNumberSubtypes, columnInfoMapS["DATA_TYPE"]) {
			lowerS = lowerUtf8
			upperS = upperUtf8
			lowerT = lowerUtf8
			upperT = upperUtf8
		}
		if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportVarcharSubtypes, columnInfoMapS["DATA_TYPE"]) {
			lowerS = stringutil.StringBuilder(`'`, lowerUtf8, `'`)
			upperS = stringutil.StringBuilder(`'`, upperUtf8, `'`)
			lowerT = stringutil.StringBuilder(`'`, lowerUtf8, `'`)
			upperT = stringutil.StringBuilder(`'`, upperUtf8, `'`)
		}
		if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, columnInfoMapS["DATA_TYPE"]) {
			lowerS = stringutil.StringBuilder(`TO_DATE('`, lowerUtf8, `','YYYY-MM-DD HH24:MI:SS')`)
			upperS = stringutil.StringBuilder(`TO_DATE('`, upperUtf8, `','YYYY-MM-DD HH24:MI:SS')`)
			lowerT = stringutil.StringBuilder(`'`, lowerUtf8, `'`)
			upperT = stringutil.StringBuilder(`'`, upperUtf8, `'`)
		}
		if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, columnInfoMapS["DATA_TYPE"]) {
			lowerS = stringutil.StringBuilder(`TO_TIMESTAMP('`, lowerUtf8, `','YYYY-MM-DD HH24:MI:SS.FF`, columnInfoMapS["DATA_SCALE"], `')`)
			upperS = stringutil.StringBuilder(`TO_TIMESTAMP('`, upperUtf8, `','YYYY-MM-DD HH24:MI:SS.FF`, columnInfoMapS["DATA_SCALE"], `')`)
			lowerT = stringutil.StringBuilder(`'`, lowerUtf8, `'`)
			upperT = stringutil.StringBuilder(`'`, upperUtf8, `'`)
		}
	default:
		return fmt.Errorf("the task_flow [%s] range chunk isn't support, please contact author or reselect", taskFlow)
	}

	if offset, ok := rg.BoundOffset[columnS]; ok {
		// update the bound
		if updateLower {
			rg.Bounds[offset].LowerS = lowerS
			rg.Bounds[offset].LowerT = lowerT
			rg.Bounds[offset].HasLower = true
		}
		if updateUpper {
			rg.Bounds[offset].UpperS = upperS
			rg.Bounds[offset].UpperT = upperT
			rg.Bounds[offset].HasUpper = true
		}
	}

	// add a new bound
	rg.addBound(&Bound{
		ColumnNameS: columnS,
		ColumnNameT: columnT,
		CollationS:  collationS,
		CollationT:  collationT,
		LowerS:      lowerS,
		UpperS:      upperS,
		LowerT:      lowerT,
		UpperT:      upperT,
		HasLower:    updateLower,
		HasUpper:    updateUpper,
	})
	return nil
}
