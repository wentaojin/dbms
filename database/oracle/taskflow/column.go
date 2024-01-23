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
	"strconv"
	"strings"

	"github.com/wentaojin/dbms/model/buildin"
	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/model/migrate"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type Column struct {
	ColumnName    string
	Datatype      string
	CharUsed      string
	CharLength    string
	DataPrecision string
	DataLength    string
	DataScale     string
	DataDefault   string
	Nullable      string
	Comment       string
}

func DatabaseTableColumnMapMYSQLDatatypeRule(c *Column, buildinDatatypes []*buildin.BuildinDatatypeRule) (string, string, error) {
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
	dataPrecision, err := strconv.Atoi(c.DataPrecision)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("column [%s] data_precision string to int failed: %v", c.ColumnName, err)
	}
	dataScale, err := strconv.Atoi(c.DataScale)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("column [%s] data_scale string to int failed: %v", c.ColumnName, err)
	}

	// build-in column datatype rule
	buildinDatatypeMap := make(map[string]string)
	numberDatatypeMap := make(map[string]struct{})

	for _, b := range buildinDatatypes {
		buildinDatatypeMap[stringutil.StringUpper(b.DatatypeNameS)] = b.DatatypeNameT

		if strings.EqualFold(stringutil.StringUpper(b.DatatypeNameS), constant.BuildInOracleDatatypeNumber) {
			for _, c := range strings.Split(b.DatatypeNameT, "/") {
				numberDatatypeMap[stringutil.StringUpper(c)] = struct{}{}
			}
		}
	}

	switch stringutil.StringUpper(c.Datatype) {
	case constant.BuildInOracleDatatypeNumber:
		if _, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeNumber]; ok {
			switch {
			case dataScale > 0:
				switch {
				// oracle 真实数据类型 number(*) -> number(38,127)
				// number  -> number(38,127)
				// number(*,x) ->  number(38,x)
				// decimal(x,y) -> y max 30
				case dataPrecision == 38 && dataScale > 30:
					originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["DECIMAL"]; ok {
						buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", 65, 30)
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin mapping data type [DECIMAL]", c.ColumnName, originColumnType)
					}
				case dataPrecision == 38 && dataScale <= 30:
					originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["DECIMAL"]; ok {
						buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", 65, dataScale)
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin mapping data type [DECIMAL]", c.ColumnName, originColumnType)
					}
				default:
					if dataScale <= 30 {
						originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
						if _, ok = numberDatatypeMap["DECIMAL"]; ok {
							buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, dataScale)
						} else {
							return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin mapping data type [DECIMAL]", c.ColumnName, originColumnType)
						}
					} else {
						originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
						if _, ok = numberDatatypeMap["DECIMAL"]; ok {
							buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", dataPrecision, 30)
						} else {
							return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin mapping data type [DECIMAL]", c.ColumnName, originColumnType)
						}
					}
				}
			case dataScale == 0:
				switch {
				case dataPrecision >= 1 && dataPrecision < 3:
					originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["TINYINT"]; ok {
						buildInColumnType = "TINYINT"
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin mapping data type [TINYINT]", c.ColumnName, originColumnType)
					}
				case dataPrecision >= 3 && dataPrecision < 5:
					originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["SMALLINT"]; ok {
						buildInColumnType = "SMALLINT"
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin mapping data type [SMALLINT]", c.ColumnName, originColumnType)
					}
				case dataPrecision >= 5 && dataPrecision < 9:
					originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["INT"]; ok {
						buildInColumnType = "INT"
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin mapping data type [INT]", c.ColumnName, originColumnType)
					}
				case dataPrecision >= 9 && dataPrecision < 19:
					originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["BIGINT"]; ok {
						buildInColumnType = "BIGINT"
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin mapping data type [BIGINT]", c.ColumnName, originColumnType)
					}
				case dataPrecision >= 19 && dataPrecision <= 38:
					originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["DECIMAL"]; ok {
						buildInColumnType = fmt.Sprintf("DECIMAL(%d)", dataPrecision)
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin mapping data type [DECIMAL]", c.ColumnName, originColumnType)
					}
				default:
					originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInOracleDatatypeNumber, dataPrecision, dataScale)
					if _, ok = numberDatatypeMap["DECIMAL"]; ok {
						buildInColumnType = fmt.Sprintf("DECIMAL(%d,%d)", 65, dataScale)
					} else {
						return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin mapping data type [DECIMAL]", c.ColumnName, originColumnType)
					}
				}
			}
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeNumber)
		}
	case constant.BuildInOracleDatatypeBfile:
		originColumnType = constant.BuildInOracleDatatypeBfile
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeBfile]; ok {
			buildInColumnType = fmt.Sprintf("%s(255)", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeBfile)
		}
	case constant.BuildInOracleDatatypeChar:
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeChar]; ok {
			if strings.EqualFold(c.CharUsed, "C") {
				originColumnType = fmt.Sprintf("%s(%s)", constant.BuildInOracleDatatypeChar, c.CharLength)
				buildInColumnType = fmt.Sprintf("%s(%s)", stringutil.StringUpper(val), c.CharLength)
			} else {
				originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInOracleDatatypeChar, dataLength)
				buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), dataLength)
			}
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeChar)
		}
	case constant.BuildInOracleDatatypeCharacter:
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeCharacter]; ok {
			if strings.EqualFold(c.CharUsed, "C") {
				originColumnType = fmt.Sprintf("%s(%s)", constant.BuildInOracleDatatypeCharacter, c.CharLength)
				buildInColumnType = fmt.Sprintf("%s(%s)", stringutil.StringUpper(val), c.CharLength)
			} else {
				originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInOracleDatatypeCharacter, dataLength)
				buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), dataLength)
			}
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeCharacter)
		}
	case constant.BuildInOracleDatatypeClob:
		originColumnType = constant.BuildInOracleDatatypeClob
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeClob]; ok {
			buildInColumnType = stringutil.StringUpper(val)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeClob)
		}
	case constant.BuildInOracleDatatypeBlob:
		originColumnType = constant.BuildInOracleDatatypeBlob
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeBlob]; ok {
			buildInColumnType = stringutil.StringUpper(val)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeBlob)
		}
	case constant.BuildInOracleDatatypeDate:
		originColumnType = constant.BuildInOracleDatatypeDate
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeDate]; ok {
			buildInColumnType = stringutil.StringUpper(val)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeDate)
		}
	case constant.BuildInOracleDatatypeDecimal:
		originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInOracleDatatypeDecimal, dataPrecision, dataScale)
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeDecimal]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d,%d)", stringutil.StringUpper(val), dataPrecision, dataScale)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeDecimal)
		}
	case constant.BuildInOracleDatatypeDec:
		originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInOracleDatatypeDecimal, dataPrecision, dataScale)
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeDec]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d,%d)", stringutil.StringUpper(val), dataPrecision, dataScale)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeDec)
		}
	case constant.BuildInOracleDatatypeDoublePrecision:
		originColumnType = constant.BuildInOracleDatatypeDoublePrecision
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeDoublePrecision]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeDoublePrecision)
		}
	case constant.BuildInOracleDatatypeFloat:
		originColumnType = constant.BuildInOracleDatatypeFloat
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeFloat]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeFloat)
		}
	case constant.BuildInOracleDatatypeInteger:
		originColumnType = constant.BuildInOracleDatatypeInteger
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeInteger]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeInteger)
		}
	case constant.BuildInOracleDatatypeInt:
		originColumnType = constant.BuildInOracleDatatypeInteger
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeInt]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeInt)
		}
	case constant.BuildInOracleDatatypeLong:
		originColumnType = constant.BuildInOracleDatatypeLong
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeLong]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeLong)
		}
	case constant.BuildInOracleDatatypeLongRAW:
		originColumnType = constant.BuildInOracleDatatypeLongRAW
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeLongRAW]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeLongRAW)
		}
	case constant.BuildInOracleDatatypeBinaryFloat:
		originColumnType = constant.BuildInOracleDatatypeBinaryFloat
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeBinaryFloat]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeBinaryFloat)
		}
	case constant.BuildInOracleDatatypeBinaryDouble:
		originColumnType = constant.BuildInOracleDatatypeBinaryDouble
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeBinaryDouble]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeBinaryDouble)
		}
	case constant.BuildInOracleDatatypeNchar:
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeNchar]; ok {
			if strings.EqualFold(c.CharUsed, "C") {
				originColumnType = fmt.Sprintf("%s(%s)", constant.BuildInOracleDatatypeNchar, c.CharLength)
				buildInColumnType = fmt.Sprintf("%s(%s)", stringutil.StringUpper(val), c.CharLength)
			} else {
				originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInOracleDatatypeNchar, dataLength)
				buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), dataLength)
			}
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeNchar)
		}
	case constant.BuildInOracleDatatypeNcharVarying:
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeNcharVarying]; ok {
			if strings.EqualFold(c.CharUsed, "C") {
				originColumnType = fmt.Sprintf("%s(%s)", constant.BuildInOracleDatatypeNcharVarying, c.CharLength)
				buildInColumnType = fmt.Sprintf("%s(%s)", stringutil.StringUpper(val), c.CharLength)
			} else {
				originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInOracleDatatypeNcharVarying, dataLength)
				buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), dataLength)
			}
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeNcharVarying)
		}
	case constant.BuildInOracleDatatypeNclob:
		originColumnType = constant.BuildInOracleDatatypeNclob
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeNclob]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeNclob)
		}
	case constant.BuildInOracleDatatypeNumeric:
		originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInOracleDatatypeNumeric, dataPrecision, dataScale)
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeNumeric]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d,%d)", stringutil.StringUpper(val), dataPrecision, dataScale)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeNumeric)
		}
	case constant.BuildInOracleDatatypeNvarchar2:
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeNvarchar2]; ok {
			if strings.EqualFold(c.CharUsed, "C") {
				originColumnType = fmt.Sprintf("%s(%s)", constant.BuildInOracleDatatypeNvarchar2, c.CharLength)
				buildInColumnType = fmt.Sprintf("%s(%s)", stringutil.StringUpper(val), c.CharLength)
			} else {
				originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInOracleDatatypeNvarchar2, dataLength)
				buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), dataLength)
			}
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeNvarchar2)
		}
	case constant.BuildInOracleDatatypeRaw:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInOracleDatatypeRaw, dataLength)
		// Fixed: MySQL Binary 数据类型定长，长度不足补 0x00, 容易导致数据对比不一致，统一使用 Varbinary 数据类型
		//if dataLength < 256 {
		//	buildInColumnType = fmt.Sprintf("BINARY(%d)", dataLength)
		//} else {
		//	buildInColumnType = fmt.Sprintf("VARBINARY(%d)", dataLength)
		//}
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeRaw]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeRaw)
		}
	case constant.BuildInOracleDatatypeReal:
		originColumnType = constant.BuildInOracleDatatypeReal
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeReal]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeReal)
		}
	case constant.BuildInOracleDatatypeRowid:
		originColumnType = constant.BuildInOracleDatatypeRowid
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeRowid]; ok {
			buildInColumnType = fmt.Sprintf("%s(64)", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeRowid)
		}
	case constant.BuildInOracleDatatypeSmallint:
		originColumnType = constant.BuildInOracleDatatypeSmallint
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeSmallint]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeSmallint)
		}
	case constant.BuildInOracleDatatypeUrowid:
		originColumnType = constant.BuildInOracleDatatypeUrowid
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeUrowid]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), dataLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeUrowid)
		}
	case constant.BuildInOracleDatatypeVarchar2:
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeVarchar2]; ok {
			if strings.EqualFold(c.CharUsed, "C") {
				originColumnType = fmt.Sprintf("%s(%s)", constant.BuildInOracleDatatypeVarchar2, c.CharLength)
				buildInColumnType = fmt.Sprintf("%s(%s)", stringutil.StringUpper(val), c.CharLength)
			} else {
				originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInOracleDatatypeVarchar2, dataLength)
				buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), dataLength)
			}
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeVarchar2)
		}
	case constant.BuildInOracleDatatypeVarchar:
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeVarchar]; ok {
			if strings.EqualFold(c.CharUsed, "C") {
				originColumnType = fmt.Sprintf("%s(%s)", constant.BuildInOracleDatatypeVarchar, c.CharLength)
				buildInColumnType = fmt.Sprintf("%s(%s)", stringutil.StringUpper(val), c.CharLength)
			} else {
				originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInOracleDatatypeVarchar, dataLength)
				buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), dataLength)
			}
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeVarchar)
		}
	case constant.BuildInOracleDatatypeXmltype:
		originColumnType = constant.BuildInOracleDatatypeXmltype
		if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeXmltype]; ok {
			buildInColumnType = fmt.Sprintf("%s", stringutil.StringUpper(val))
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, constant.BuildInOracleDatatypeXmltype)
		}
	default:
		if strings.Contains(c.Datatype, "INTERVAL YEAR") {
			originColumnType = c.Datatype
			if val, ok := buildinDatatypeMap[stringutil.StringUpper(originColumnType)]; ok {
				buildInColumnType = fmt.Sprintf("%s(30)", stringutil.StringUpper(val))
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, stringutil.StringUpper(originColumnType))
			}
		} else if strings.Contains(c.Datatype, "INTERVAL DAY") {
			originColumnType = c.Datatype
			if val, ok := buildinDatatypeMap[constant.BuildInOracleDatatypeIntervalDay]; ok {
				buildInColumnType = fmt.Sprintf("%s(30)", stringutil.StringUpper(val))
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, stringutil.StringUpper(originColumnType))
			}
		} else if strings.Contains(c.Datatype, "TIMESTAMP") {
			originColumnType = c.Datatype
			if dataScale <= 6 {
				if val, ok := buildinDatatypeMap[stringutil.StringUpper(originColumnType)]; ok {
					buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), dataScale)
					return originColumnType, buildInColumnType, nil
				} else {
					return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, stringutil.StringUpper(originColumnType))
				}
			} else {
				if val, ok := buildinDatatypeMap[stringutil.StringUpper(originColumnType)]; ok {
					buildInColumnType = fmt.Sprintf("%s(%d)", stringutil.StringUpper(val), 6)
					return originColumnType, buildInColumnType, nil
				} else {
					return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql column type rule isn't exist, please checkin", c.ColumnName, stringutil.StringUpper(originColumnType))
				}
			}
		} else {
			originColumnType = c.Datatype
			buildInColumnType = "TEXT"
		}
		return originColumnType, buildInColumnType, nil
	}
}

// HandleColumnRuleWithPriority priority, return column datatype and default value
// column > table > schema > task
func HandleColumnRuleWithPriority(columnName string, originDatatype, buildInDatatype, originDefaultValue, sourceCharset, targetCharset string, buildinDefaultValueRules []*buildin.BuildinDefaultvalRule, taskRules []*migrate.TaskStructRule, schemaRules []*migrate.SchemaStructRule, tableRules []*migrate.TableStructRule, columnRules []*migrate.ColumnStructRule) (string, string, error) {
	var (
		datatype, defaultValue string
		err                    error
	)
	columnDatatypes, columnDefaultValues := handleColumnRuleWithColumnHighPriority(columnRules)

	globalDatatypes, globalDefaultValues := handelColumnRuleWithTableSchemaTaskPriority(buildinDefaultValueRules, taskRules, schemaRules, tableRules)

	// column default value
	defaultValue, err = handleColumnRuleWitheDefaultValuePriority(columnName, originDefaultValue, sourceCharset, targetCharset, columnDefaultValues, globalDefaultValues)
	if err != nil {
		return datatype, defaultValue, err
	}

	// column high priority
	if columnDatatypes == nil && columnDefaultValues == nil {
		// global priority
		for ruleColumnTypeS, ruleColumnTypeT := range globalDatatypes {
			datatype = handleColumnRuleWithDatatypeCompare(originDatatype, ruleColumnTypeS, ruleColumnTypeT)
			if !strings.EqualFold(datatype, constant.OracleDatabaseColumnDatatypeMatchRuleNotFound) {
				return datatype, defaultValue, nil
			}
		}

		datatype = buildInDatatype
		return datatype, defaultValue, nil
	}

	for colName, columnRuleMap := range columnDatatypes {
		if strings.EqualFold(columnName, colName) {
			for ruleColumnTypeS, ruleColumnTypeT := range columnRuleMap {
				datatype = handleColumnRuleWithDatatypeCompare(originDatatype, ruleColumnTypeS, ruleColumnTypeT)
				if !strings.EqualFold(datatype, constant.OracleDatabaseColumnDatatypeMatchRuleNotFound) {
					return datatype, defaultValue, nil
				}
			}
		}
	}
	datatype = buildInDatatype
	return datatype, defaultValue, nil
}

func handleColumnRuleWitheDefaultValuePriority(columnName, originDefaultValue, sourceCharset, targetCharset string, columnCustomDefaultValue map[string]map[string]string, columnGlobalDefaultValue map[string]string) (string, error) {
	// Additional processing of Oracle defaults ('6') or (5) or ('xsddd') that contain parentheses instead of defaults like '(xxxx)'
	// Oracle will automatically handle the default value ('xxx') or (xxx) internally, so needs to be processed into 'xxx' or xxx
	// default('0'  )
	// default('') default(sysdate )
	// default(0 ) default(0.1 )
	// default('0'
	//)
	var defaultVal string

	originDefaultValLen := len(originDefaultValue)

	rightBracketsIndex := strings.Index(originDefaultValue, "(")
	leftBracketsIndex := strings.LastIndex(originDefaultValue, ")")

	if rightBracketsIndex == -1 || leftBracketsIndex == -1 {
		defaultVal = originDefaultValue
	} else {
		// If the first one is a left parenthesis, the last one is either a right parenthesis ) or any space + a right parenthesis ), and it cannot be anything else
		if rightBracketsIndex == 0 {
			diffK := originDefaultValLen - leftBracketsIndex
			if diffK == 0 {
				defaultVal = originDefaultValue[1:leftBracketsIndex]
			} else {
				// Remove trailing spaces )
				diffV := strings.TrimSpace(originDefaultValue[leftBracketsIndex:])
				if len(diffV) == 1 && strings.EqualFold(diffV, ")") {
					defaultVal = originDefaultValue[1:leftBracketsIndex]
				} else {
					return defaultVal, fmt.Errorf("handle column [%s] default value [%s] rule failed", columnName, originDefaultValue)
				}
			}
		} else {
			// If the left parenthesis is not the first one, remove the spaces
			diffLeft := strings.TrimSpace(originDefaultValue[:rightBracketsIndex+1])
			//  (xxxx)
			if len(diffLeft) == 1 && strings.EqualFold(diffLeft, "(") && strings.LastIndex(originDefaultValue, ")") != -1 {
				defaultVal = originDefaultValue[rightBracketsIndex:leftBracketsIndex]
			} else if len(diffLeft) == 1 && strings.EqualFold(diffLeft, "'(") && strings.LastIndex(originDefaultValue, "'") != -1 {
				// ' xxx(sd)' or 'xxx(ssd '
				defaultVal = originDefaultValue
			} else {
				// ' (xxxs) '、sys_guid()、'xss()'、'x''()x)'、''
				defaultVal = originDefaultValue
			}

		}
	}

	if columnCustomDefaultValue == nil && columnGlobalDefaultValue == nil {
		// string data charset processing
		// MigrateStringDataTypeDatabaseCharsetMap
		dataDefault, err := handleColumnDefaultValueCharset(columnName, defaultVal, constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(sourceCharset)], constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(targetCharset)])
		if err != nil {
			return defaultVal, err
		}
		return dataDefault, nil
	}

	// priority
	// column > global
	if columnCustomDefaultValue != nil {
		if vals, exist := columnCustomDefaultValue[columnName]; exist {
			// The currently created table structure field A varchar2(10) does not have any attributes. If you need to specify changes, you need to specify the defaultValueS value, which is NULLSTRING.
			// The currently created table structure field A varchar2(10) default NULL. If you need to specify changes, you need to specify the defaultValueS value, which is NULL.
			// The currently created table structure field A varchar2(10) default ''. If you need to specify changes, you need to specify the defaultValueS value, which is ''.
			for k, v := range vals {
				if strings.EqualFold(strings.TrimSpace(k), strings.TrimSpace(defaultVal)) {
					defaultVal = v
					// break skip
					break
				}
			}
		}
	}

	if columnGlobalDefaultValue != nil {
		for k, v := range columnGlobalDefaultValue {
			if strings.EqualFold(strings.TrimSpace(k), strings.TrimSpace(defaultVal)) {
				defaultVal = v
				// break skip
				break
			}
		}
	}

	dataDefault, err := handleColumnDefaultValueCharset(columnName, strings.TrimSpace(defaultVal), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(targetCharset)])
	if err != nil {
		return defaultVal, err
	}
	return dataDefault, nil
}

func handleColumnRuleWithDatatypeCompare(originColumnType, ruleColumnTypeS, ruleColumnTypeT string) string {
	/*
		number datatype：function ->  GetDatabaseTableColumn
		- number(*,10) -> number(38,10)
		- number(*,0) -> number(38,0)
		- number(*) -> number(38,127)
		- number -> number(38,127)
		- number(5) -> number(5)
		- number(8,9) -> number(8,9)
	*/
	if strings.Contains(stringutil.StringUpper(ruleColumnTypeS), "NUMBER") {
		switch {
		case strings.Contains(stringutil.StringUpper(ruleColumnTypeS), "*") && strings.Contains(stringutil.StringUpper(ruleColumnTypeS), ","):
			if strings.EqualFold(strings.Replace(ruleColumnTypeS, "*", "38", -1), originColumnType) &&
				ruleColumnTypeT != "" {
				return stringutil.StringUpper(ruleColumnTypeT)
			}
		case strings.Contains(stringutil.StringUpper(ruleColumnTypeS), "*") && !strings.Contains(stringutil.StringUpper(ruleColumnTypeS), ","):
			if strings.EqualFold("NUMBER(38,127)", originColumnType) &&
				ruleColumnTypeT != "" {
				return stringutil.StringUpper(ruleColumnTypeT)
			}
		case !strings.Contains(stringutil.StringUpper(ruleColumnTypeS), "(") && !strings.Contains(stringutil.StringUpper(ruleColumnTypeS), ")"):
			if strings.EqualFold("NUMBER(38,127)", originColumnType) &&
				ruleColumnTypeT != "" {
				return stringutil.StringUpper(ruleColumnTypeT)
			}
		default:
			if strings.EqualFold(ruleColumnTypeS, originColumnType) && ruleColumnTypeT != "" {
				return stringutil.StringUpper(ruleColumnTypeT)
			}
		}
	} else {
		if strings.EqualFold(ruleColumnTypeS, originColumnType) && ruleColumnTypeT != "" {
			return stringutil.StringUpper(ruleColumnTypeT)
		}
	}
	// datatype rule isn't match
	return constant.OracleDatabaseColumnDatatypeMatchRuleNotFound
}

// handelColumnRuleWithTableSchemaTaskPriority priority
// table > schema > task -> server
func handelColumnRuleWithTableSchemaTaskPriority(buildinDefaultValueRules []*buildin.BuildinDefaultvalRule, taskRules []*migrate.TaskStructRule, schemaRules []*migrate.SchemaStructRule, tableRules []*migrate.TableStructRule) (map[string]string, map[string]string) {
	taskDatatype, taskDefaultVal := handleColumnRuleWithTaskLevel(taskRules)

	schemaDatatype, schemaDefaultVal := handleColumnRuleWithSchemaLevel(schemaRules)

	tableDatatype, tableDefaultVal := handleColumnRuleWithTableLevel(tableRules)

	buildinDefaultValues := handleColumnDefaultValueRuleWithServerLevel(buildinDefaultValueRules)

	globalColumnDatatype := stringutil.ExchangeStringDict(tableDatatype, stringutil.ExchangeStringDict(schemaDatatype, taskDatatype))

	globalColumnDefaultVal := stringutil.ExchangeStringDict(tableDefaultVal, stringutil.ExchangeStringDict(schemaDefaultVal, stringutil.ExchangeStringDict(taskDefaultVal, buildinDefaultValues)))

	return globalColumnDatatype, globalColumnDefaultVal
}

// handleColumnRuleWithColumnHighPriority priority
// column high priority
func handleColumnRuleWithColumnHighPriority(columnRules []*migrate.ColumnStructRule) (map[string]map[string]string, map[string]map[string]string) {
	columnDatatypeMap := make(map[string]map[string]string)
	columnDefaultValMap := make(map[string]map[string]string)

	if len(columnRules) == 0 {
		return columnDatatypeMap, columnDefaultValMap
	}

	columnNames := make(map[string]struct{})
	for _, cr := range columnRules {
		columnNames[cr.ColumnNameS] = struct{}{}
	}

	for c, _ := range columnNames {
		datatypeMap := make(map[string]string)
		defaultValMap := make(map[string]string)
		for _, cr := range columnRules {
			if strings.EqualFold(c, cr.ColumnNameS) {
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
			columnDatatypeMap[c] = datatypeMap
		}
		if defaultValMap != nil {
			columnDefaultValMap[c] = defaultValMap
		}
	}
	return columnDatatypeMap, columnDefaultValMap
}

func handleColumnDefaultValueRuleWithServerLevel(buildinRules []*buildin.BuildinDefaultvalRule) map[string]string {
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

func handleColumnRuleWithTaskLevel(taskRules []*migrate.TaskStructRule) (map[string]string, map[string]string) {
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

func handleColumnRuleWithSchemaLevel(schemaRules []*migrate.SchemaStructRule) (map[string]string, map[string]string) {
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

func handleColumnRuleWithTableLevel(tableRules []*migrate.TableStructRule) (map[string]string, map[string]string) {
	columnDatatypeMap := make(map[string]string)
	columnDefaultValMap := make(map[string]string)

	if len(tableRules) == 0 {
		return columnDatatypeMap, columnDefaultValMap
	}

	for _, t := range tableRules {
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

func handleColumnDefaultValueCharset(columnName, defaultVal, sourceCharset, targetCharset string) (string, error) {
	var dataDefault string

	// column default value is '', direct return
	if strings.EqualFold(defaultVal, "''") {
		return defaultVal, nil
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
		dataDefault = "'" + string(convertTargetRaw) + "'"
	} else {
		if strings.EqualFold(string(convertTargetRaw), constant.OracleDatabaseTableColumnDefaultValueWithStringNull) {
			dataDefault = "'" + string(convertTargetRaw) + "'"
		} else {
			dataDefault = string(convertTargetRaw)
		}
	}
	return dataDefault, nil
}
