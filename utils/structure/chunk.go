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
	DBType      string         `json:"dbType"`
	DBCharset   string         `json:"dbCharset"`
	Bounds      []*Bound       `json:"bounds"`
	BoundOffset map[string]int `json:"boundOffset"`
}

// NewChunkRange return a Range.
func NewChunkRange(dbType string, dbCharset string) *Range {
	return &Range{
		DBType:      dbType,
		DBCharset:   dbCharset,
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

func (rg *Range) ToString() (string, error) {
	// bound replace
	for _, b := range rg.Bounds {
		switch stringutil.StringUpper(rg.DBType) {
		case constant.DatabaseTypeOracle:
			convertUtf8Raws, err := stringutil.CharsetConvert([]byte(b.Lower), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(rg.DBCharset)], constant.CharsetUTF8MB4)
			if err != nil {
				return "", err
			}
			lowerUtf8 := stringutil.BytesToString(convertUtf8Raws)

			convertUtf8Raws, err = stringutil.CharsetConvert([]byte(b.Upper), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(rg.DBCharset)], constant.CharsetUTF8MB4)
			if err != nil {
				return "", err
			}
			upperUtf8 := stringutil.BytesToString(convertUtf8Raws)
			if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportNumberSubtypes, b.Datatype) {
				b.Lower = lowerUtf8
				b.Upper = upperUtf8
			} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, b.Datatype) {
				b.Lower = stringutil.StringBuilder(`TO_DATE('`, lowerUtf8, `','YYYY-MM-DD HH24:MI:SS')`)
				b.Upper = stringutil.StringBuilder(`TO_DATE('`, upperUtf8, `','YYYY-MM-DD HH24:MI:SS')`)
			} else if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, b.Datatype) {
				// datetimePrecision -> dataScale
				b.Lower = stringutil.StringBuilder(`TO_TIMESTAMP('`, lowerUtf8, `','YYYY-MM-DD HH24:MI:SS.FF`, b.DatetimePrecision, `')`)
				b.Upper = stringutil.StringBuilder(`TO_TIMESTAMP('`, upperUtf8, `','YYYY-MM-DD HH24:MI:SS.FF`, b.DatetimePrecision, `')`)
			} else {
				b.Lower = stringutil.StringBuilder(`'`, lowerUtf8, `'`)
				b.Upper = stringutil.StringBuilder(`'`, upperUtf8, `'`)
			}
			if b.Collation != "" {
				b.Collation = fmt.Sprintf("'NLS_SORT = %s'", b.Collation)
			}
		case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
			convertUtf8Raws, err := stringutil.CharsetConvert([]byte(b.Lower), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(rg.DBCharset)], constant.CharsetUTF8MB4)
			if err != nil {
				return "", err
			}
			lowerUtf8 := stringutil.BytesToString(convertUtf8Raws)

			convertUtf8Raws, err = stringutil.CharsetConvert([]byte(b.Upper), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(rg.DBCharset)], constant.CharsetUTF8MB4)
			if err != nil {
				return "", err
			}
			upperUtf8 := stringutil.BytesToString(convertUtf8Raws)

			if stringutil.IsContainedString(constant.DataCompareMYSQLCompatibleDatabaseSupportDecimalSubtypes, b.Datatype) {
				b.Lower = lowerUtf8
				b.Upper = upperUtf8
			} else {
				b.Lower = stringutil.StringBuilder(`'`, lowerUtf8, `'`)
				b.Upper = stringutil.StringBuilder(`'`, upperUtf8, `'`)
			}
			if b.Collation != "" {
				b.Collation = fmt.Sprintf("COLLATE '%s'", b.Collation)
			}
		default:
			return "", fmt.Errorf("the database type [%s] range chunk isn't support, please contact author or reselect", rg.DBType)
		}
	}

	/* for example:
	there is a bucket , and the lowerbound and upperbound are (A, B1, C1), (A, B2, C2), and the columns are `a`, `b` and `c`,
	this bucket's data range is (a = A) AND (b > B1 or (b == B1 and c > C1)) AND (b < B2 or (b == B2 and c <= C2))
	*/
	sameCondition := make([]string, 0, 1)
	lowerCondition := make([]string, 0, 1)
	upperCondition := make([]string, 0, 1)

	preConditionForLower := make([]string, 0, 1)
	preConditionForUpper := make([]string, 0, 1)
	preConditionArgsForLower := make([]string, 0, 1)
	preConditionArgsForUpper := make([]string, 0, 1)

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
				sameCondition = append(sameCondition, fmt.Sprintf("%s = %s", stringutil.StringBuilder("\"", bound.ColumnName, "\""), bound.Lower))
			} else {
				sameCondition = append(sameCondition, fmt.Sprintf("%s = %s", stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnName, "\",", bound.Collation, ")"), stringutil.StringBuilder("NLSSORT(", bound.Upper, ",", bound.Collation, ")")))
			}
		}

		if strings.EqualFold(rg.DBType, constant.DatabaseTypeMySQL) || strings.EqualFold(rg.DBType, constant.DatabaseTypeTiDB) {
			if strings.EqualFold(bound.Collation, "") {
				sameCondition = append(sameCondition, fmt.Sprintf("%s = %s", stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Lower))
			} else {
				sameCondition = append(sameCondition, fmt.Sprintf("%s %s = %s", stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Collation, bound.Lower))
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
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder("\"", bound.ColumnName, "\""), lowerSymbol, bound.Lower))
					} else {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("\"", bound.ColumnName, "\""), lowerSymbol, bound.Lower))
					}
					preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = %s", stringutil.StringBuilder("\"", bound.ColumnName, "\""), bound.Lower))
					preConditionArgsForLower = append(preConditionArgsForLower, bound.Lower)
				} else {
					if len(preConditionForLower) > 0 {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnName, "\",", bound.Collation, ")"), lowerSymbol, stringutil.StringBuilder("NLSSORT(", bound.Lower, ",", bound.Collation, ")")))
					} else {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnName, "\",", bound.Collation, ")"), lowerSymbol, stringutil.StringBuilder("NLSSORT(", bound.Lower, ",", bound.Collation, ")")))
					}
					preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s = %s", stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnName, "\",", bound.Collation, ")"), stringutil.StringBuilder("NLSSORT(", bound.Lower, ",", bound.Collation, ")")))
					preConditionArgsForLower = append(preConditionArgsForLower, stringutil.StringBuilder("NLSSORT(", bound.Lower, ",", bound.Collation, ")"))
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
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND %s %s %s %s)", stringutil.StringJoin(preConditionForLower, " AND "), stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Collation, lowerSymbol, bound.Lower))
					} else {
						lowerCondition = append(lowerCondition, fmt.Sprintf("(%s %s %s %s)", stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Collation, lowerSymbol, bound.Lower))
					}
					preConditionForLower = append(preConditionForLower, fmt.Sprintf("%s %s = %s", stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Collation, bound.Lower))
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
						upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s %s %s)", stringutil.StringJoin(preConditionForUpper, " AND "), stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnName, "\",", bound.Collation, ")"), upperSymbol, stringutil.StringBuilder("NLSSORT(", bound.Upper, ",", bound.Collation, ")")))
					} else {
						upperCondition = append(upperCondition, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnName, "\",", bound.Collation, ")"), upperSymbol, stringutil.StringBuilder("NLSSORT(", bound.Upper, ",", bound.Collation, ")")))
					}
					preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s = %s", stringutil.StringBuilder("NLSSORT(", "\"", bound.ColumnName, "\",", bound.Collation, ")"), stringutil.StringBuilder("NLSSORT(", bound.Upper, ",", bound.Collation, ")")))
					preConditionArgsForUpper = append(preConditionArgsForUpper, stringutil.StringBuilder("NLSSORT(", bound.Upper, ",", bound.Collation, ")"))
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
						upperCondition = append(upperCondition, fmt.Sprintf("(%s AND %s %s %s %s)", stringutil.StringJoin(preConditionForUpper, " AND "), stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Collation, upperSymbol, bound.Upper))
					} else {
						upperCondition = append(upperCondition, fmt.Sprintf("(%s %s %s %s)", stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Collation, upperSymbol, bound.Upper))
					}
					preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("%s %s = %s", stringutil.StringBuilder("`", bound.ColumnName, "`"), bound.Collation, bound.Upper))
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
			return strings.Join(lowerCondition, " OR "), nil
		}

		if len(lowerCondition) == 0 {
			return strings.Join(upperCondition, " OR "), nil
		}

		return fmt.Sprintf("(%s) AND (%s)", strings.Join(lowerCondition, " OR "), strings.Join(upperCondition, " OR ")), nil
	} else {
		if len(upperCondition) == 0 && len(lowerCondition) == 0 {
			return strings.Join(sameCondition, " AND "), nil
		}

		if len(upperCondition) == 0 {
			return fmt.Sprintf("(%s) AND (%s)", strings.Join(sameCondition, " AND "), strings.Join(lowerCondition, " OR ")), nil
		}

		if len(lowerCondition) == 0 {
			return fmt.Sprintf("(%s) AND (%s)", strings.Join(sameCondition, " AND "), strings.Join(upperCondition, " OR ")), nil
		}

		return fmt.Sprintf("(%s) AND (%s) AND (%s)", strings.Join(sameCondition, " AND "), strings.Join(lowerCondition, " OR "), strings.Join(upperCondition, " OR ")), nil
	}
}

func (rg *Range) Update(columnName, collation, datatype string, datetimePrecision string, lower, upper string, updateLower, updateUpper bool) error {
	var (
		lowerS string
		upperS string
	)

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
	newChunk := NewChunkRange(rg.DBType, rg.DBCharset)
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
