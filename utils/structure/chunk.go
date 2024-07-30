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
	"github.com/godror/godror"
	"strings"

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

// Bound represents a bound for a column
type Bound struct {
	ColumnName        string `json:"columnName"`
	Collation         string `json:"collation"`
	Datatype          string `json:"datatype"`
	DatetimePrecision string `json:"datetimePrecision"`
	Lower             string `json:"lower"`
	Upper             string `json:"upper"`

	HasLower bool `json:"hasLower"`
	HasUpper bool `json:"hasUpper"`
}

// Range represents chunk range
type Range struct {
	DBType        string         `json:"dbType"`
	DBCharsetFrom string         `json:"dbCharsetFrom"` // charset convert, from the charset (used to the data compare)
	DBCharsetDest string         `json:"dbCharsetDest"` // charset convert, dest the charset (used to the data compare)
	Bounds        []*Bound       `json:"bounds"`
	BoundOffset   map[string]int `json:"boundOffset"`
}

// NewChunkRange return a Range.
func NewChunkRange(dbType, dbCharsetFrom, dbCharsetDest string) *Range {
	return &Range{
		DBType:        dbType,
		DBCharsetFrom: dbCharsetFrom,
		DBCharsetDest: dbCharsetDest,
		Bounds:        make([]*Bound, 0, 2),
		BoundOffset:   make(map[string]int),
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
	rg.BoundOffset[bound.ColumnName] = len(rg.Bounds) - 1
}

func (rg *Range) ToString() (string, []interface{}) {
	// bound replace
	for _, b := range rg.Bounds {
		switch stringutil.StringUpper(rg.DBType) {
		case constant.DatabaseTypeOracle:
			if b.Collation != constant.DataCompareDisabledCollationSettingFillEmptyString {
				b.Collation = fmt.Sprintf("'NLS_SORT = %s'", b.Collation)
			}
		case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
			if b.Collation != constant.DataCompareDisabledCollationSettingFillEmptyString {
				b.Collation = fmt.Sprintf("COLLATE '%s'", b.Collation)
			}
		default:
			panic(fmt.Errorf("the database type [%s] range chunk isn't support, please contact author or reselect", rg.DBType))
		}
	}

	/* for example:
	there is a bucket , and the lowerbound and upperbound are (A, B1, C1), (A, B2, C2), and the columns are `a`, `b` and `c`,
	this bucket's data range is (a = A) AND (b > B1 or (b == B1 and c > C1)) AND (b < B2 or (b == B2 and c <= C2))
	*/
	sameCondition := make([]string, 0, 1)
	lowerCondition := make([]string, 0, 1)
	upperCondition := make([]string, 0, 1)

	sameArgs := make([]interface{}, 0, 1)
	lowerArgs := make([]interface{}, 0, 1)
	upperArgs := make([]interface{}, 0, 1)

	preConditionForLower := make([]string, 0, 1)
	preConditionForUpper := make([]string, 0, 1)
	preConditionArgsForLower := make([]interface{}, 0, 1)
	preConditionArgsForUpper := make([]interface{}, 0, 1)

	i := 0
	for ; i < len(rg.Bounds); i++ {
		bound := rg.Bounds[i]
		if !(bound.HasLower && bound.HasUpper) {
			break
		}

		if bound.Lower != bound.Upper {
			break
		}

		if strings.EqualFold(rg.DBType, constant.DatabaseTypeOracle) {
			if strings.EqualFold(bound.Collation, "") {
				if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, bound.Datatype) {
					sameCondition = append(sameCondition, fmt.Sprintf("%s = TO_DATE(?,'YYYY-MM-DD HH24:MI:SS')", stringutil.StringBuilder("\"", bound.ColumnName, "\"")))
				} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, bound.Datatype) {
					// datetimePrecision -> dataScale
					sameCondition = append(sameCondition, fmt.Sprintf("%s = TO_TIMESTAMP(?,'YYYY-MM-DD HH24:MI:SS.FF%s')", stringutil.StringBuilder("\"", bound.ColumnName, "\""), bound.DatetimePrecision))
				} else {
					sameCondition = append(sameCondition, fmt.Sprintf("%s = ?", stringutil.StringBuilder("\"", bound.ColumnName, "\"")))
				}
				sameArgs = append(sameArgs, bound.Lower)
			} else {
				if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, bound.Datatype) {
					sameCondition = append(sameCondition, fmt.Sprintf("%s = NLSSORT(TO_DATE(?,'YYYY-MM-DD HH24:MI:SS'),%s)", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), bound.Collation))
				} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, bound.Datatype) {
					// datetimePrecision -> dataScale
					sameCondition = append(sameCondition, fmt.Sprintf("%s = NLSSORT(TO_TIMESTAMP(?,'YYYY-MM-DD HH24:MI:SS.FF%s'),%s)", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), bound.DatetimePrecision, bound.Collation))
				} else {
					sameCondition = append(sameCondition, fmt.Sprintf("%s = NLSSORT(?,%s)", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), bound.Collation))
				}
				sameArgs = append(sameArgs, bound.Lower)
			}
		}

		if strings.EqualFold(rg.DBType, constant.DatabaseTypeMySQL) || strings.EqualFold(rg.DBType, constant.DatabaseTypeTiDB) {
			if strings.EqualFold(bound.Collation, "") {
				sameCondition = append(sameCondition, fmt.Sprintf("%s = ?", stringutil.StringBuilder("`", bound.ColumnName, "`")))
				sameArgs = append(sameArgs, bound.Lower)
			} else {
				sameCondition = append(sameCondition, fmt.Sprintf("%s %s = ?", stringutil.StringBuilder("CONVERT(`", bound.ColumnName, "` USING '", rg.DBCharsetDest, "')"), bound.Collation))
				sameArgs = append(sameArgs, bound.Lower)
			}
		}
	}

	if i == len(rg.Bounds) && i > 0 {
		// All the columns are equal in bounds, should return FALSE!
		return "1 = 0", nil
	}

	for ; i < len(rg.Bounds); i++ {
		bound := rg.Bounds[i]

		lowerSymbol := constant.DataCompareSymbolGt
		upperSymbol := constant.DataCompareSymbolLt
		if i == len(rg.Bounds)-1 {
			upperSymbol = constant.DataCompareSymbolLte
		}

		if bound.HasLower {
			if strings.EqualFold(rg.DBType, constant.DatabaseTypeOracle) {
				if strings.EqualFold(bound.Collation, "") {
					if len(preConditionForLower) > 0 {
						if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, bound.Datatype) {
							lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s TO_DATE(?,'YYYY-MM-DD HH24:MI:SS'))", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder("\"", bound.ColumnName, "\""), lowerSymbol))
						} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, bound.Datatype) {
							// datetimePrecision -> dataScale
							lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s TO_TIMESTAMP(?,'YYYY-MM-DD HH24:MI:SS.FF%s'))", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder("\"", bound.ColumnName, "\""), lowerSymbol, bound.DatetimePrecision))
						} else {
							lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s ?)", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder("\"", bound.ColumnName, "\""), lowerSymbol))
						}
						lowerArgs = append(append(lowerArgs, preConditionArgsForLower...), bound.Lower)
					} else {
						if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, bound.Datatype) {
							lowerCondition = append(lowerCondition, fmt.Sprintf("(%s %s TO_DATE(?,'YYYY-MM-DD HH24:MI:SS'))", stringutil.StringBuilder("\"", bound.ColumnName, "\""), lowerSymbol))
						} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, bound.Datatype) {
							// datetimePrecision -> dataScale
							lowerCondition = append(lowerCondition, fmt.Sprintf("(%s %s TO_TIMESTAMP(?,'YYYY-MM-DD HH24:MI:SS.FF%s'))", stringutil.StringBuilder("\"", bound.ColumnName, "\""), lowerSymbol, bound.DatetimePrecision))
						} else {
							lowerCondition = append(lowerCondition, fmt.Sprintf("(%s %s ?)", stringutil.StringBuilder("\"", bound.ColumnName, "\""), lowerSymbol))
						}
						lowerArgs = append(lowerArgs, bound.Lower)
					}

					if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, bound.Datatype) {
						preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = TO_DATE(?,'YYYY-MM-DD HH24:MI:SS')", stringutil.StringBuilder("\"", bound.ColumnName, "\"")))
					} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, bound.Datatype) {
						// datetimePrecision -> dataScale
						preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = TO_TIMESTAMP(?,'YYYY-MM-DD HH24:MI:SS.FF%s')", stringutil.StringBuilder("\"", bound.ColumnName, "\""), bound.DatetimePrecision))
					} else {
						preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = ?", stringutil.StringBuilder("\"", bound.ColumnName, "\"")))
					}
					preConditionArgsForLower = append(preConditionArgsForLower, bound.Lower)
				} else {
					if len(preConditionForLower) > 0 {
						if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, bound.Datatype) {
							lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s NLSSORT(TO_DATE(?,'YYYY-MM-DD HH24:MI:SS'),%s))", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), lowerSymbol, bound.Collation))
						} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, bound.Datatype) {
							// datetimePrecision -> dataScale
							lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s NLSSORT(TO_TIMESTAMP(?,'YYYY-MM-DD HH24:MI:SS.FF%s'),%s))", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), lowerSymbol, bound.DatetimePrecision, bound.Collation))
						} else {
							lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s NLSSORT(?,%s))", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), lowerSymbol, bound.Collation))
						}
						lowerArgs = append(append(lowerArgs, preConditionArgsForLower...), bound.Lower)
					} else {
						if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, bound.Datatype) {
							lowerCondition = append(lowerCondition, fmt.Sprintf("(%s %s NLSSORT(TO_DATE(?,'YYYY-MM-DD HH24:MI:SS'),%s))", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), lowerSymbol, bound.Collation))
						} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, bound.Datatype) {
							// datetimePrecision -> dataScale
							lowerCondition = append(lowerCondition, fmt.Sprintf("(%s %s NLSSORT(TO_TIMESTAMP(?,'YYYY-MM-DD HH24:MI:SS.FF%s'),%s))", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), lowerSymbol, bound.DatetimePrecision, bound.Collation))
						} else {
							lowerCondition = append(lowerCondition, fmt.Sprintf("(%s %s NLSSORT(?,%s))", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), lowerSymbol, bound.Collation))
						}
						lowerArgs = append(lowerArgs, bound.Lower)
					}

					if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, bound.Datatype) {
						preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = NLSSORT(TO_DATE(?,'YYYY-MM-DD HH24:MI:SS'),%s)", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), bound.Collation))
					} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, bound.Datatype) {
						// datetimePrecision -> dataScale
						preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = NLSSORT(TO_TIMESTAMP(?,'YYYY-MM-DD HH24:MI:SS.FF%s'),%s)", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), bound.DatetimePrecision, bound.Collation))
					} else {
						preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = NLSSORT(?,%s)", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), bound.Collation))
					}
					preConditionArgsForLower = append(preConditionArgsForLower, bound.Lower)
				}
			}

			if strings.EqualFold(rg.DBType, constant.DatabaseTypeMySQL) || strings.EqualFold(rg.DBType, constant.DatabaseTypeTiDB) {
				if strings.EqualFold(bound.Collation, "") {
					if len(preConditionForLower) > 0 {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s ?)", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder("`", bound.ColumnName, "`"), lowerSymbol))
						lowerArgs = append(append(lowerArgs, preConditionArgsForLower...), bound.Lower)
					} else {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s %s ?)", stringutil.StringBuilder("`", bound.ColumnName, "`"), lowerSymbol))
						lowerArgs = append(lowerArgs, bound.Lower)
					}
					preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = ?", stringutil.StringBuilder("`", bound.ColumnName, "`")))
					preConditionArgsForLower = append(preConditionArgsForLower, bound.Lower)
				} else {
					if len(preConditionForLower) > 0 {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s %s ?)", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder("CONVERT(`", bound.ColumnName, "` USING '", rg.DBCharsetDest, "')"), bound.Collation, lowerSymbol))
						lowerArgs = append(append(lowerArgs, preConditionArgsForLower...), bound.Lower)
					} else {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s %s %s ?)", stringutil.StringBuilder("CONVERT(`", bound.ColumnName, "` USING '", rg.DBCharsetDest, "')"), bound.Collation, lowerSymbol))
						lowerArgs = append(lowerArgs, bound.Lower)
					}
					preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s %s = ?", stringutil.StringBuilder("CONVERT(`", bound.ColumnName, "` USING '", rg.DBCharsetDest, "')"), bound.Collation))
					preConditionArgsForLower = append(preConditionArgsForLower, bound.Lower)
				}
			}
		}

		if bound.HasUpper {
			if strings.EqualFold(rg.DBType, constant.DatabaseTypeOracle) {
				if strings.EqualFold(bound.Collation, "") {
					if len(preConditionForUpper) > 0 {
						if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, bound.Datatype) {
							upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s %s TO_DATE(?,'YYYY-MM-DD HH24:MI:SS'))", stringutil.StringJoin(preConditionForUpper, " AND "), stringutil.StringBuilder("\"", bound.ColumnName, "\""), upperSymbol))
						} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, bound.Datatype) {
							// datetimePrecision -> dataScale
							upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s %s TO_TIMESTAMP(?,'YYYY-MM-DD HH24:MI:SS.FF%s'))", stringutil.StringJoin(preConditionForUpper, " AND "), stringutil.StringBuilder("\"", bound.ColumnName, "\""), bound.DatetimePrecision, upperSymbol))
						} else {
							upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s %s ?)", stringutil.StringJoin(preConditionForUpper, " AND "), stringutil.StringBuilder("\"", bound.ColumnName, "\""), upperSymbol))
						}
						upperArgs = append(append(upperArgs, preConditionArgsForUpper...), bound.Upper)
					} else {
						if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, bound.Datatype) {
							upperCondition = append(upperCondition, fmt.Sprintf("(%s %s TO_DATE(?,'YYYY-MM-DD HH24:MI:SS'))", stringutil.StringBuilder("\"", bound.ColumnName, "\""), upperSymbol))
						} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, bound.Datatype) {
							// datetimePrecision -> dataScale
							upperCondition = append(upperCondition, fmt.Sprintf("(%s %s TO_TIMESTAMP(?,'YYYY-MM-DD HH24:MI:SS.FF%s'))", stringutil.StringBuilder("\"", bound.ColumnName, "\""), bound.DatetimePrecision, upperSymbol))
						} else {
							upperCondition = append(upperCondition, fmt.Sprintf("(%s %s ?)", stringutil.StringBuilder("\"", bound.ColumnName, "\""), upperSymbol))
						}
						upperArgs = append(upperArgs, bound.Upper)
					}

					if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, bound.Datatype) {
						preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s = TO_DATE(?,'YYYY-MM-DD HH24:MI:SS')", stringutil.StringBuilder("\"", bound.ColumnName, "\"")))
					} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, bound.Datatype) {
						// datetimePrecision -> dataScale
						preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s = TO_TIMESTAMP(?,'YYYY-MM-DD HH24:MI:SS.FF%s')", stringutil.StringBuilder("\"", bound.ColumnName, "\""), bound.DatetimePrecision))
					} else {
						preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s = ?", stringutil.StringBuilder("\"", bound.ColumnName, "\"")))
					}
					preConditionArgsForUpper = append(preConditionArgsForUpper, bound.Upper)
				} else {
					if len(preConditionForUpper) > 0 {
						if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, bound.Datatype) {
							upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s %s NLSSORT(TO_DATE(?,'YYYY-MM-DD HH24:MI:SS'),%s))", stringutil.StringJoin(preConditionForUpper, " AND "), stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), upperSymbol, bound.Collation))
						} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, bound.Datatype) {
							// datetimePrecision -> dataScale
							upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s %s NLSSORT(TO_TIMESTAMP(?,'YYYY-MM-DD HH24:MI:SS.FF%s'),%s))", stringutil.StringJoin(preConditionForUpper, " AND "), stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), upperSymbol, bound.DatetimePrecision, bound.Collation))
						} else {
							upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s %s NLSSORT(?,%s))", stringutil.StringJoin(preConditionForUpper, " AND "), stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), upperSymbol, bound.Collation))
						}
						upperArgs = append(append(upperArgs, preConditionArgsForUpper...), bound.Upper)
					} else {
						if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, bound.Datatype) {
							upperCondition = append(upperCondition, fmt.Sprintf("(%s %s NLSSORT(TO_DATE(?,'YYYY-MM-DD HH24:MI:SS'),%s))", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), upperSymbol, bound.Collation))
						} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, bound.Datatype) {
							// datetimePrecision -> dataScale
							upperCondition = append(upperCondition, fmt.Sprintf("(%s %s NLSSORT(TO_TIMESTAMP(?,'YYYY-MM-DD HH24:MI:SS.FF%s'),%s))", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), upperSymbol, bound.DatetimePrecision, bound.Collation))
						} else {
							upperCondition = append(upperCondition, fmt.Sprintf("(%s %s NLSSORT(?,%s))", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), upperSymbol, bound.Collation))
						}
						upperArgs = append(upperArgs, bound.Upper)
					}

					if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, bound.Datatype) {
						preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s = NLSSORT(TO_DATE(?,'YYYY-MM-DD HH24:MI:SS'),%s)", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), bound.Collation))
					} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, bound.Datatype) {
						// datetimePrecision -> dataScale
						preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s = NLSSORT(TO_TIMESTAMP(?,'YYYY-MM-DD HH24:MI:SS.FF%s'),%s)", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), bound.DatetimePrecision, bound.Collation))
					} else {
						preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s = NLSSORT(?,%s)", stringutil.StringBuilder(`NLSSORT(CONVERT("`, bound.ColumnName, `",'`, rg.DBCharsetDest, `','`, rg.DBCharsetFrom, `'),`, bound.Collation, `)`), bound.Collation))
					}
					preConditionArgsForUpper = append(preConditionArgsForUpper, bound.Upper)
				}
			}

			if strings.EqualFold(rg.DBType, constant.DatabaseTypeMySQL) || strings.EqualFold(rg.DBType, constant.DatabaseTypeTiDB) {
				if strings.EqualFold(bound.Collation, "") {
					if len(preConditionForUpper) > 0 {
						upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s %s ?)", stringutil.StringJoin(preConditionForUpper, " AND "), stringutil.StringBuilder("`", bound.ColumnName, "`"), upperSymbol))
						upperArgs = append(append(upperArgs, preConditionArgsForUpper...), bound.Upper)
					} else {
						upperCondition = append(upperCondition, fmt.Sprintf("(%s %s ?)", stringutil.StringBuilder("`", bound.ColumnName, "`"), upperSymbol))
						upperArgs = append(upperArgs, bound.Upper)
					}
					preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s = ?", stringutil.StringBuilder("`", bound.ColumnName, "`")))
					preConditionArgsForUpper = append(preConditionArgsForUpper, bound.Upper)
				} else {
					if len(preConditionForUpper) > 0 {
						upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s %s %s ?)", stringutil.StringJoin(preConditionForUpper, " AND "), stringutil.StringBuilder("CONVERT(`", bound.ColumnName, "` USING '", rg.DBCharsetDest, "')"), bound.Collation, upperSymbol))
						upperArgs = append(append(upperArgs, preConditionArgsForUpper...), bound.Upper)
					} else {
						upperCondition = append(upperCondition, fmt.Sprintf("(%s %s %s ?)", stringutil.StringBuilder("CONVERT(`", bound.ColumnName, "` USING '", rg.DBCharsetDest, "')"), bound.Collation, upperSymbol))
						upperArgs = append(upperArgs, bound.Upper)
					}
					preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s %s = ?", stringutil.StringBuilder("CONVERT(`", bound.ColumnName, "` USING '", rg.DBCharsetDest, "')"), bound.Collation))
					preConditionArgsForUpper = append(preConditionArgsForUpper, bound.Upper)
				}
			}
		}
	}

	if len(sameCondition) == 0 {
		if len(upperCondition) == 0 && len(lowerCondition) == 0 {
			return "1 = 1", nil
		}

		if len(upperCondition) == 0 {
			switch rg.DBType {
			case constant.DatabaseTypeOracle:
				return godror.ReplaceQuestionPlacholders(strings.Join(lowerCondition, " OR ")), lowerArgs
			case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
				return strings.Join(lowerCondition, " OR "), lowerArgs
			default:
				panic(fmt.Errorf("the database type [%s] range chunk isn't support, please contact author or reselect", rg.DBType))
			}
		}

		if len(lowerCondition) == 0 {
			switch rg.DBType {
			case constant.DatabaseTypeOracle:
				return godror.ReplaceQuestionPlacholders(strings.Join(upperCondition, " OR ")), upperArgs
			case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
				return strings.Join(upperCondition, " OR "), upperArgs
			default:
				panic(fmt.Errorf("the database type [%s] range chunk isn't support, please contact author or reselect", rg.DBType))
			}
		}

		switch rg.DBType {
		case constant.DatabaseTypeOracle:
			return godror.ReplaceQuestionPlacholders(fmt.Sprintf("(%s) AND (%s)", strings.Join(lowerCondition, " OR "), strings.Join(upperCondition, " OR "))), append(lowerArgs, upperArgs...)
		case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
			return fmt.Sprintf("(%s) AND (%s)", strings.Join(lowerCondition, " OR "), strings.Join(upperCondition, " OR ")), append(lowerArgs, upperArgs...)
		default:
			panic(fmt.Errorf("the database type [%s] range chunk isn't support, please contact author or reselect", rg.DBType))
		}

	} else {
		if len(upperCondition) == 0 && len(lowerCondition) == 0 {
			switch rg.DBType {
			case constant.DatabaseTypeOracle:
				return godror.ReplaceQuestionPlacholders(strings.Join(sameCondition, " AND ")), sameArgs
			case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
				return strings.Join(sameCondition, " AND "), sameArgs
			default:
				panic(fmt.Errorf("the database type [%s] range chunk isn't support, please contact author or reselect", rg.DBType))
			}
		}

		if len(upperCondition) == 0 {
			switch rg.DBType {
			case constant.DatabaseTypeOracle:
				return godror.ReplaceQuestionPlacholders(fmt.Sprintf("(%s) AND (%s)", strings.Join(sameCondition, " AND "), strings.Join(lowerCondition, " OR "))), append(sameArgs, lowerArgs...)
			case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
				return fmt.Sprintf("(%s) AND (%s)", strings.Join(sameCondition, " AND "), strings.Join(lowerCondition, " OR ")), append(sameArgs, lowerArgs...)
			default:
				panic(fmt.Errorf("the database type [%s] range chunk isn't support, please contact author or reselect", rg.DBType))
			}
		}

		if len(lowerCondition) == 0 {
			switch rg.DBType {
			case constant.DatabaseTypeOracle:
				return godror.ReplaceQuestionPlacholders(fmt.Sprintf("(%s) AND (%s)", strings.Join(sameCondition, " AND "), strings.Join(upperCondition, " OR "))), append(sameArgs, upperArgs...)
			case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
				return fmt.Sprintf("(%s) AND (%s)", strings.Join(sameCondition, " AND "), strings.Join(upperCondition, " OR ")), append(sameArgs, upperArgs...)
			default:
				panic(fmt.Errorf("the database type [%s] range chunk isn't support, please contact author or reselect", rg.DBType))
			}
		}

		switch rg.DBType {
		case constant.DatabaseTypeOracle:
			return godror.ReplaceQuestionPlacholders(fmt.Sprintf("(%s) AND (%s) AND (%s)", strings.Join(sameCondition, " AND "), strings.Join(lowerCondition, " OR "), strings.Join(upperCondition, " OR "))), append(append(sameArgs, lowerArgs...), upperArgs...)
		case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
			return fmt.Sprintf("(%s) AND (%s) AND (%s)", strings.Join(sameCondition, " AND "), strings.Join(lowerCondition, " OR "), strings.Join(upperCondition, " OR ")), append(append(sameArgs, lowerArgs...), upperArgs...)
		default:
			panic(fmt.Errorf("the database type [%s] range chunk isn't support, please contact author or reselect", rg.DBType))
		}
	}
}

func (rg *Range) Update(columnName, collation, datatype string, datetimePrecision string, lower, upper string, updateLower, updateUpper bool) error {

	if offset, ok := rg.BoundOffset[columnName]; ok {
		// update the bound
		if updateLower {
			rg.Bounds[offset].Lower = lower
			rg.Bounds[offset].HasLower = true
		}
		if updateUpper {
			rg.Bounds[offset].Upper = upper
			rg.Bounds[offset].HasUpper = true
		}
	}

	// add a new bound
	rg.addBound(&Bound{
		ColumnName:        columnName,
		Collation:         collation,
		Datatype:          datatype,
		DatetimePrecision: datetimePrecision,
		Lower:             lower,
		Upper:             upper,
		HasLower:          updateLower,
		HasUpper:          updateUpper,
	})
	return nil
}

func (rg *Range) Copy() *Range {
	newChunk := NewChunkRange(rg.DBType, rg.DBCharsetFrom, rg.DBCharsetDest)
	for _, bound := range rg.Bounds {
		newChunk.addBound(&Bound{
			ColumnName:        bound.ColumnName,
			Collation:         bound.Collation,
			Datatype:          bound.Datatype,
			DatetimePrecision: bound.DatetimePrecision,
			Lower:             bound.Lower,
			Upper:             bound.Upper,
			HasLower:          bound.HasLower,
			HasUpper:          bound.HasUpper,
		})
	}
	return newChunk
}
