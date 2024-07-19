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
	ColumnName string `json:"columnName"`
	Collation  string `json:"collation"`
	Lower      string `json:"lower"`
	Upper      string `json:"upper"`

	HasLower bool `json:"hasLower"`
	HasUpper bool `json:"hasUpper"`
}

// Range represents chunk range
type Range struct {
	DBType      string         `json:"dbType"`
	Bounds      []*Bound       `json:"bounds"`
	BoundOffset map[string]int `json:"boundOffset"`
}

// NewChunkRange return a Range.
func NewChunkRange() *Range {
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
	rg.BoundOffset[bound.ColumnName] = len(rg.Bounds) - 1
}

func (rg *Range) ToString() string {
	/* for example:
	there is a bucket, the lowerbound and upperbound are (v1, v3), (v2, v4), and the columns are `a` and `b`,
	this bucket's data range is (a > v1 or (a == v1 and b > v3)) and (a < v2 or (a == v2 and b <= v4))
	*/

	lowerCondition := make([]string, 0, 1)
	upperCondition := make([]string, 0, 1)

	preConditionForLower := make([]string, 0, 1)
	preConditionForUpper := make([]string, 0, 1)
	preConditionArgsForLower := make([]string, 0, 1)
	preConditionArgsForUpper := make([]string, 0, 1)

	for i, bound := range rg.Bounds {
		lowerSymbol := constant.DataCompareSymbolGt
		upperSymbol := constant.DataCompareSymbolLt
		if i == len(rg.Bounds)-1 {
			upperSymbol = constant.DataCompareSymbolLte
		}

		if bound.HasLower {
			if strings.EqualFold(rg.DBType, constant.DatabaseTypeOracle) {
				if strings.EqualFold(bound.Collation, "") {
					if len(preConditionForLower) > 0 {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder("\"", bound.ColumnName, "\""), lowerSymbol, bound.Lower))
					} else {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("\"", bound.ColumnName, "\""), lowerSymbol, bound.Lower))
					}
					preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = %s", stringutil.StringBuilder("\"", bound.ColumnName, "\""), bound.Lower))
					preConditionArgsForLower = append(preConditionArgsForLower, bound.Lower)
				} else {
					if len(preConditionForLower) > 0 {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnName, "\"", bound.Collation, ")"), lowerSymbol, stringutil.StringBuilder("NLSSORT('", bound.Lower, "',", bound.Collation, ")")))
					} else {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s%s %s %s)", stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Collation, lowerSymbol, bound.Lower))
					}
					preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = %s", stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnName, "\"", bound.Collation, ")"), stringutil.StringBuilder("NLSSORT('", bound.Lower, "',", bound.Collation, ")")))
					preConditionArgsForLower = append(preConditionArgsForLower, stringutil.StringBuilder("NLSSORT('", bound.Lower, "',", bound.Collation, ")"))
				}
			}

			if strings.EqualFold(rg.DBType, constant.DatabaseTypeMySQL) || strings.EqualFold(rg.DBType, constant.DatabaseTypeTiDB) {
				if strings.EqualFold(bound.Collation, "") {
					if len(preConditionForLower) > 0 {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder("`", bound.ColumnName, "`"), lowerSymbol, bound.Lower))
					} else {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("`", bound.ColumnName, "`"), lowerSymbol, bound.Lower))
					}
					preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = %s", stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Lower))
					preConditionArgsForLower = append(preConditionArgsForLower, bound.Lower)
				} else {
					if len(preConditionForLower) > 0 {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s%s %s %s)", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Collation, lowerSymbol, bound.Lower))
					} else {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s%s %s %s)", stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Collation, lowerSymbol, bound.Lower))
					}
					preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = %s", stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Lower))
					preConditionArgsForLower = append(preConditionArgsForLower, bound.Lower)
				}
			}
		}

		if bound.HasUpper {
			if strings.EqualFold(rg.DBType, constant.DatabaseTypeOracle) {
				if strings.EqualFold(bound.Collation, "") {
					if len(preConditionForUpper) > 0 {
						upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForUpper, " AND "), stringutil.StringBuilder("\"", bound.ColumnName, "\""), upperSymbol, bound.Upper))
					} else {
						upperCondition = append(upperCondition, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("\"", bound.ColumnName, "\""), upperSymbol, bound.Upper))
					}
					preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s = %s", stringutil.StringBuilder("\"", bound.ColumnName, "\""), bound.Upper))
					preConditionArgsForUpper = append(preConditionArgsForUpper, bound.Upper)
				} else {
					if len(preConditionForUpper) > 0 {
						upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForUpper, " AND "), stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnName, "\"", bound.Collation, ")"), upperSymbol, stringutil.StringBuilder("NLSSORT('", bound.Upper, "',", bound.Collation, ")")))
					} else {
						upperCondition = append(upperCondition, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnName, "\"", bound.Collation, ")"), upperSymbol, stringutil.StringBuilder("NLSSORT('", bound.Upper, "',", bound.Collation, ")")))
					}
					preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s = %s", stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnName, "\"", bound.Collation, ")"), stringutil.StringBuilder("NLSSORT('", bound.Upper, "',", bound.Collation, ")")))
					preConditionArgsForUpper = append(preConditionArgsForUpper, stringutil.StringBuilder("NLSSORT('", bound.Upper, "',", bound.Collation, ")"))
				}
			}

			if strings.EqualFold(rg.DBType, constant.DatabaseTypeMySQL) || strings.EqualFold(rg.DBType, constant.DatabaseTypeTiDB) {
				if strings.EqualFold(bound.Collation, "") {
					if len(preConditionForUpper) > 0 {
						upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForUpper, " AND "), stringutil.StringBuilder("`", bound.ColumnName, "`"), upperSymbol, bound.Upper))
					} else {
						upperCondition = append(upperCondition, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("`", bound.ColumnName, "`"), upperSymbol, bound.Upper))
					}
					preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s = %s", stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Upper))
					preConditionArgsForUpper = append(preConditionArgsForUpper, bound.Upper)
				} else {
					if len(preConditionForUpper) > 0 {
						upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s%s %s %s)", stringutil.StringJoin(preConditionForUpper, " AND "), stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Collation, upperSymbol, bound.Upper))
					} else {
						upperCondition = append(upperCondition, fmt.Sprintf("(%s%s %s %s)", stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Collation, upperSymbol, bound.Upper))
					}
					preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s = %s", stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Upper))
					preConditionArgsForUpper = append(preConditionArgsForUpper, bound.Upper)
				}
			}
		}
	}

	if len(upperCondition) == 0 && len(lowerCondition) == 0 {
		return "1 = 1"
	}

	if len(upperCondition) == 0 {
		return stringutil.StringJoin(lowerCondition, " OR ")
	}
	if len(lowerCondition) == 0 {
		return stringutil.StringJoin(upperCondition, " OR ")
	}

	return fmt.Sprintf("(%s) AND (%s)", stringutil.StringJoin(lowerCondition, " OR "), stringutil.StringJoin(upperCondition, " OR "))
}

func (rg *Range) Update(dbType, dbCharset, columnName, collation, datatype string, datetimePrecision string, lower, upper string, updateLower, updateUpper bool) error {
	var (
		lowerS string
		upperS string
	)

	switch stringutil.StringUpper(dbType) {
	case constant.DatabaseTypeOracle:
		convertUtf8Raws, err := stringutil.CharsetConvert([]byte(lower), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(dbCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return err
		}
		lowerUtf8 := stringutil.BytesToString(convertUtf8Raws)

		convertUtf8Raws, err = stringutil.CharsetConvert([]byte(upper), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(dbCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return err
		}
		upperUtf8 := stringutil.BytesToString(convertUtf8Raws)
		if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportNumberSubtypes, datatype) {
			lowerS = lowerUtf8
			upperS = upperUtf8
		} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, datatype) {
			lowerS = stringutil.StringBuilder(`TO_DATE('`, lowerUtf8, `','YYYY-MM-DD HH24:MI:SS')`)
			upperS = stringutil.StringBuilder(`TO_DATE('`, upperUtf8, `','YYYY-MM-DD HH24:MI:SS')`)
		} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, datatype) {
			// datetimePrecision -> dataScale
			lowerS = stringutil.StringBuilder(`TO_TIMESTAMP('`, lowerUtf8, `','YYYY-MM-DD HH24:MI:SS.FF`, datetimePrecision, `')`)
			upperS = stringutil.StringBuilder(`TO_TIMESTAMP('`, upperUtf8, `','YYYY-MM-DD HH24:MI:SS.FF`, datetimePrecision, `')`)
		} else {
			lowerS = stringutil.StringBuilder(`'`, lowerUtf8, `'`)
			upperS = stringutil.StringBuilder(`'`, upperUtf8, `'`)
		}
	case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
		convertUtf8Raws, err := stringutil.CharsetConvert([]byte(lower), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(dbCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return err
		}
		lowerUtf8 := stringutil.BytesToString(convertUtf8Raws)

		convertUtf8Raws, err = stringutil.CharsetConvert([]byte(upper), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(dbCharset)], constant.CharsetUTF8MB4)
		if err != nil {
			return err
		}
		upperUtf8 := stringutil.BytesToString(convertUtf8Raws)
		if stringutil.IsContainedString(constant.DataCompareMYSQLCompatibleDatabaseSupportDecimalSubtypes, datatype) {
			lowerS = lowerUtf8
			upperS = upperUtf8
		} else {
			lowerS = stringutil.StringBuilder(`'`, lowerUtf8, `'`)
			upperS = stringutil.StringBuilder(`'`, upperUtf8, `'`)
		}
	default:
		return fmt.Errorf("the database type [%s] range chunk isn't support, please contact author or reselect", dbType)
	}

	if offset, ok := rg.BoundOffset[columnName]; ok {
		// update the bound
		if updateLower {
			rg.Bounds[offset].Lower = lowerS
			rg.Bounds[offset].HasLower = true
		}
		if updateUpper {
			rg.Bounds[offset].Upper = upperS
			rg.Bounds[offset].HasUpper = true
		}
	}

	// add a new bound
	rg.addBound(&Bound{
		ColumnName: columnName,
		Collation:  collation,
		Lower:      lower,
		Upper:      upper,
		HasLower:   updateLower,
		HasUpper:   updateUpper,
	})
	return nil
}

func (rg *Range) Copy() *Range {
	newChunk := NewChunkRange()
	for _, bound := range rg.Bounds {
		newChunk.addBound(&Bound{
			ColumnName: bound.ColumnName,
			Collation:  bound.Collation,
			Lower:      bound.Lower,
			Upper:      bound.Upper,
			HasLower:   bound.HasLower,
			HasUpper:   bound.HasUpper,
		})
	}

	return newChunk
}
