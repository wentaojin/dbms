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
	"strconv"
	"strings"

	"github.com/wentaojin/dbms/model/buildin"
	"github.com/wentaojin/dbms/model/migrate"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

// https://dev.mysql.com/doc/workbench/en/wb-migration-database-postgresql-typemapping.html?spm=a2c6h.12873639.article-detail.10.23e618e7Hs0kzy
func PostgresDatabaseTableColumnMapMYSQLCompatibleDatatypeRule(taskFlow string, c *Column, buildinDatatypes []*buildin.BuildinDatatypeRule) (string, string, error) {
	var (
		// origin column datatype
		originColumnType string
		// build-in column datatype
		buildInColumnType string
	)

	charLength, err := strconv.Atoi(c.CharLength)
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
	datetimePrecision, err := strconv.Atoi(c.DatetimePrecision)
	if err != nil {
		return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datetime_precision string to int failed: %v", c.ColumnName, err)
	}

	// build-in column datatype rule
	buildinDatatypeMap := make(map[string]string)
	characterDatatypeMap := make(map[string]string)
	varcharacterDatatypeMap := make(map[string]string)

	for _, b := range buildinDatatypes {
		buildinDatatypeMap[stringutil.StringUpper(b.DatatypeNameS)] = stringutil.StringUpper(b.DatatypeNameT)
		if strings.EqualFold(stringutil.StringUpper(b.DatatypeNameS), constant.BuildInPostgresDatatypeCharacter) {
			for _, d := range strings.Split(b.DatatypeNameT, "/") {
				characterDatatypeMap[stringutil.StringUpper(d)] = stringutil.StringUpper(d)
			}
		}
		if strings.EqualFold(stringutil.StringUpper(b.DatatypeNameS), constant.BuildInPostgresDatatypeCharacterVarying) {
			for _, d := range strings.Split(b.DatatypeNameT, "/") {
				varcharacterDatatypeMap[stringutil.StringUpper(d)] = stringutil.StringUpper(d)
			}
		}
	}

	switch stringutil.StringUpper(c.Datatype) {
	case constant.BuildInPostgresDatatypeInteger:
		originColumnType = constant.BuildInPostgresDatatypeInteger
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeInteger]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeInteger)
		}
	case constant.BuildInPostgresDatatypeSmallInt:
		originColumnType = constant.BuildInPostgresDatatypeSmallInt
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeSmallInt]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeSmallInt)
		}
	case constant.BuildInPostgresDatatypeBigInt:
		originColumnType = constant.BuildInPostgresDatatypeBigInt
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeBigInt]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeBigInt)
		}
	case constant.BuildInPostgresDatatypeBit:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInPostgresDatatypeBigInt, charLength)
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeBit]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", val, charLength)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeBit)
		}
	case constant.BuildInPostgresDatatypeBoolean:
		originColumnType = constant.BuildInPostgresDatatypeBoolean
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeBoolean]; ok {
			buildInColumnType = fmt.Sprintf("%s(1)", val)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeBoolean)
		}
	case constant.BuildInPostgresDatatypeReal:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInPostgresDatatypeReal, dataPrecision)
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeReal]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", val, dataPrecision)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeReal)
		}
	case constant.BuildInPostgresDatatypeDoublePrecision:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInPostgresDatatypeDoublePrecision, dataPrecision)
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeDoublePrecision]; ok {
			buildInColumnType = fmt.Sprintf("%s(%d)", val, dataPrecision)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeDoublePrecision)
		}
	case constant.BuildInPostgresDatatypeNumeric:
		if dataPrecision == 0 && dataScale == 0 {
			originColumnType = constant.BuildInPostgresDatatypeNumeric
			if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeNumeric]; ok {
				buildInColumnType = val
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeNumeric)
			}
		} else {
			originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInPostgresDatatypeNumeric, dataPrecision, dataScale)
			if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeDoublePrecision]; ok {
				buildInColumnType = fmt.Sprintf("%s(%d,%d)", val, dataPrecision, dataScale)
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeNumeric)
			}
		}
	case constant.BuildInPostgresDatatypeDecimal:
		if dataPrecision == 0 && dataScale == 0 {
			originColumnType = constant.BuildInPostgresDatatypeDecimal
			if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeDecimal]; ok {
				buildInColumnType = val
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeDecimal)
			}
		} else {
			originColumnType = fmt.Sprintf("%s(%d,%d)", constant.BuildInPostgresDatatypeDecimal, dataPrecision, dataScale)
			if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeDecimal]; ok {
				buildInColumnType = fmt.Sprintf("%s(%d,%d)", val, dataPrecision, dataScale)
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeDecimal)
			}
		}
	case constant.BuildInPostgresDatatypeMoney:
		originColumnType = constant.BuildInPostgresDatatypeMoney
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeMoney]; ok {
			buildInColumnType = fmt.Sprintf("%s(19,2)", val)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeMoney)
		}
	case constant.BuildInPostgresDatatypeCharacter:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInPostgresDatatypeCharacter, charLength)
		if charLength <= 255 {
			if val, ok := characterDatatypeMap["CHAR"]; ok {
				buildInColumnType = fmt.Sprintf("%s(%d)", val, charLength)
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeCharacter)
			}
		} else if 255 < charLength && charLength <= 16382 {
			if val, ok := characterDatatypeMap["VARCHAR"]; ok {
				buildInColumnType = fmt.Sprintf("%s(%d)", val, charLength)
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeCharacter)
			}
		} else {
			if val, ok := characterDatatypeMap["LONGTEXT"]; ok {
				buildInColumnType = val
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeCharacter)
			}
		}
	case constant.BuildInPostgresDatatypeCharacterVarying:
		originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInPostgresDatatypeCharacterVarying, charLength)
		if charLength <= 16382 {
			if val, ok := varcharacterDatatypeMap["VARCHAR"]; ok {
				buildInColumnType = fmt.Sprintf("%s(%d)", val, charLength)
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeCharacterVarying)
			}
		} else if 16382 < charLength && charLength <= 4194303 {
			if val, ok := varcharacterDatatypeMap["MEDIUMTEXT"]; ok {
				buildInColumnType = val
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeCharacterVarying)
			}
		} else {
			if val, ok := varcharacterDatatypeMap["LONGTEXT"]; ok {
				buildInColumnType = val
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeCharacterVarying)
			}
		}
	case constant.BuildInPostgresDatatypeDate:
		if datetimePrecision == 0 {
			originColumnType = fmt.Sprintf("%s", constant.BuildInPostgresDatatypeDate)
			if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeDate]; ok {
				buildInColumnType = val
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeDate)
			}
		} else {
			originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInPostgresDatatypeDate, datetimePrecision)
			if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeDate]; ok {
				buildInColumnType = fmt.Sprintf("%s(%d)", val, datetimePrecision)
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeDate)
			}
		}
	case constant.BuildInPostgresDatatypeTimeWithoutTimeZone:
		if datetimePrecision == 0 {
			originColumnType = fmt.Sprintf("%s", constant.BuildInPostgresDatatypeTimeWithoutTimeZone)
			if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeTimeWithoutTimeZone]; ok {
				buildInColumnType = val
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeTimeWithoutTimeZone)
			}
		} else {
			originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInPostgresDatatypeTimeWithoutTimeZone, datetimePrecision)
			if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeTimeWithoutTimeZone]; ok {
				buildInColumnType = fmt.Sprintf("%s(%d)", val, datetimePrecision)
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeTimeWithoutTimeZone)
			}
		}
	case constant.BuildInPostgresDatatypeTimestampWithoutTimeZone:
		if datetimePrecision == 0 {
			originColumnType = fmt.Sprintf("%s", constant.BuildInPostgresDatatypeTimestampWithoutTimeZone)
			if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeTimestampWithoutTimeZone]; ok {
				buildInColumnType = val
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeTimestampWithoutTimeZone)
			}
		} else {
			originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInPostgresDatatypeTimestampWithoutTimeZone, datetimePrecision)
			if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeTimestampWithoutTimeZone]; ok {
				buildInColumnType = fmt.Sprintf("%s(%d)", val, datetimePrecision)
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeTimestampWithoutTimeZone)
			}
		}
	case constant.BuildInPostgresDatatypeInterval:
		if datetimePrecision == 0 {
			originColumnType = fmt.Sprintf("%s", constant.BuildInPostgresDatatypeInterval)
			if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeInterval]; ok {
				buildInColumnType = val
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeInterval)
			}
		} else {
			originColumnType = fmt.Sprintf("%s(%d)", constant.BuildInPostgresDatatypeInterval, datetimePrecision)
			if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeInterval]; ok {
				buildInColumnType = fmt.Sprintf("%s(%d)", val, datetimePrecision)
				return originColumnType, buildInColumnType, nil
			} else {
				return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeInterval)
			}
		}
	case constant.BuildInPostgresDatatypeBytea:
		originColumnType = constant.BuildInPostgresDatatypeBytea
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeBytea]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeBytea)
		}
	case constant.BuildInPostgresDatatypeText:
		originColumnType = constant.BuildInPostgresDatatypeText
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeText]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeText)
		}
	case constant.BuildInPostgresDatatypeCidr:
		originColumnType = constant.BuildInPostgresDatatypeCidr
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeCidr]; ok {
			buildInColumnType = fmt.Sprintf("%s(43)", val)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeCidr)
		}
	case constant.BuildInPostgresDatatypeInet:
		originColumnType = constant.BuildInPostgresDatatypeInet
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeInet]; ok {
			buildInColumnType = fmt.Sprintf("%s(43)", val)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeInet)
		}
	case constant.BuildInPostgresDatatypeMacaddr:
		originColumnType = constant.BuildInPostgresDatatypeMacaddr
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeMacaddr]; ok {
			buildInColumnType = fmt.Sprintf("%s(17)", val)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeMacaddr)
		}
	case constant.BuildInPostgresDatatypeUuid:
		originColumnType = constant.BuildInPostgresDatatypeUuid
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeUuid]; ok {
			buildInColumnType = fmt.Sprintf("%s(36)", val)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeUuid)
		}
	case constant.BuildInPostgresDatatypeXml:
		originColumnType = constant.BuildInPostgresDatatypeXml
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeXml]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeXml)
		}
	case constant.BuildInPostgresDatatypeJson:
		originColumnType = constant.BuildInPostgresDatatypeJson
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeJson]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeJson)
		}
	case constant.BuildInPostgresDatatypeTsvector:
		originColumnType = constant.BuildInPostgresDatatypeTsvector
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeTsvector]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeTsvector)
		}
	case constant.BuildInPostgresDatatypeTsquery:
		originColumnType = constant.BuildInPostgresDatatypeTsquery
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeTsquery]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeTsquery)
		}
	case constant.BuildInPostgresDatatypeArray:
		originColumnType = constant.BuildInPostgresDatatypeArray
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeArray]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeArray)
		}
	case constant.BuildInPostgresDatatypePoint:
		originColumnType = constant.BuildInPostgresDatatypePoint
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypePoint]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypePoint)
		}
	case constant.BuildInPostgresDatatypeLine:
		originColumnType = constant.BuildInPostgresDatatypeLine
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeLine]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeLine)
		}
	case constant.BuildInPostgresDatatypeLseg:
		originColumnType = constant.BuildInPostgresDatatypeLseg
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeLseg]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeLseg)
		}
	case constant.BuildInPostgresDatatypeBox:
		originColumnType = constant.BuildInPostgresDatatypeBox
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeBox]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeBox)
		}
	case constant.BuildInPostgresDatatypePath:
		originColumnType = constant.BuildInPostgresDatatypePath
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypePath]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypePath)
		}
	case constant.BuildInPostgresDatatypePolygon:
		originColumnType = constant.BuildInPostgresDatatypePolygon
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypePolygon]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypePolygon)
		}
	case constant.BuildInPostgresDatatypeCircle:
		originColumnType = constant.BuildInPostgresDatatypeCircle
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeCircle]; ok {
			buildInColumnType = val
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeCircle)
		}
	case constant.BuildInPostgresDatatypeTxidSnapshot:
		originColumnType = constant.BuildInPostgresDatatypeTxidSnapshot
		if val, ok := buildinDatatypeMap[constant.BuildInPostgresDatatypeTxidSnapshot]; ok {
			buildInColumnType = fmt.Sprintf("%s(256)", val)
			return originColumnType, buildInColumnType, nil
		} else {
			return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, constant.BuildInPostgresDatatypeTxidSnapshot)
		}
	default:
		return originColumnType, buildInColumnType, fmt.Errorf("column [%s] datatype [%s] map mysql compatible database table type rule isn't exist, please checkin", c.ColumnName, c.Datatype)
	}
}

// PostgresHandleColumnRuleWithPriority priority, return column datatype and default value
// column > table > schema > task
func PostgresHandleColumnRuleWithPriority(originSourceTable, originColumnName string, originDatatype, buildInDatatype, originDefaultValue, sourceCharset, targetCharset string, buildinDefaultValueRules []*buildin.BuildinDefaultvalRule, taskRules []*migrate.TaskStructRule, schemaRules []*migrate.SchemaStructRule, tableRules []*migrate.TableStructRule, columnRules []*migrate.ColumnStructRule) (string, string, error) {
	var (
		datatype, defaultValue string
		err                    error
	)
	columnDatatypes, columnDefaultValues := postgresHandleColumnRuleWithColumnHighPriority(originColumnName, columnRules)

	globalDatatypes, globalDefaultValues := postgresHandelColumnRuleWithTableSchemaTaskPriority(originSourceTable, buildinDefaultValueRules, taskRules, schemaRules, tableRules)

	// column default value
	defaultValue, err = PostgresHandleColumnRuleWitheDefaultValuePriority(originColumnName, originDefaultValue, sourceCharset, targetCharset, columnDefaultValues, globalDefaultValues)
	if err != nil {
		return datatype, defaultValue, err
	}

	// column high priority
	if len(columnDatatypes) == 0 && len(columnDefaultValues) == 0 {
		// global priority
		for ruleColumnTypeS, ruleColumnTypeT := range globalDatatypes {
			datatype = postgresHandleColumnRuleWithDatatypeCompare(originDatatype, ruleColumnTypeS, ruleColumnTypeT)
			if !strings.EqualFold(datatype, constant.PostgresDatabaseColumnDatatypeMatchRuleNotFound) {
				return datatype, defaultValue, nil
			}
		}

		datatype = buildInDatatype
		return datatype, defaultValue, nil
	}

	for customColName, columnRuleMap := range columnDatatypes {
		// global priority
		if len(columnRuleMap) == 0 {
			for ruleColumnTypeS, ruleColumnTypeT := range globalDatatypes {
				datatype = postgresHandleColumnRuleWithDatatypeCompare(originDatatype, ruleColumnTypeS, ruleColumnTypeT)
				if !strings.EqualFold(datatype, constant.PostgresDatabaseColumnDatatypeMatchRuleNotFound) {
					return datatype, defaultValue, nil
				}
			}

			datatype = buildInDatatype
			return datatype, defaultValue, nil
		}
		// column priority
		// case field rule
		if originColumnName == customColName {
			for ruleColumnTypeS, ruleColumnTypeT := range columnRuleMap {
				datatype = postgresHandleColumnRuleWithDatatypeCompare(originDatatype, ruleColumnTypeS, ruleColumnTypeT)
				if !strings.EqualFold(datatype, constant.PostgresDatabaseColumnDatatypeMatchRuleNotFound) {
					return datatype, defaultValue, nil
				}
			}
		}
	}
	datatype = buildInDatatype
	return datatype, defaultValue, nil
}

func postgresHandleColumnRuleWithDatatypeCompare(originColumnType, ruleColumnTypeS, ruleColumnTypeT string) string {
	if strings.EqualFold(ruleColumnTypeS, originColumnType) && ruleColumnTypeT != "" {
		return ruleColumnTypeT
	}
	// datatype rule isn't match
	return constant.PostgresDatabaseColumnDatatypeMatchRuleNotFound
}

func PostgresHandleColumnRuleWitheDefaultValuePriority(columnName, originDefaultValue, sourceCharset, targetCharset string, columnCustomDefaultValue map[string]map[string]string, columnGlobalDefaultValue map[string]string) (string, error) {
	if len(columnCustomDefaultValue) == 0 && len(columnGlobalDefaultValue) == 0 {
		// string data charset processing
		// MigrateStringDataTypeDatabaseCharsetMap
		dataDefault, err := postgresHandleColumnDefaultValueCharset(columnName, originDefaultValue, sourceCharset, targetCharset)
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
			// The currently created table structure field A varchar2(10) does not have any attributes. If you need to specify changes, you need to specify the defaultValueS value, which is NULLSTRING.
			// The currently created table structure field A varchar2(10) default NULL. If you need to specify changes, you need to specify the defaultValueS value, which is NULL.
			// The currently created table structure field A varchar2(10) default ''. If you need to specify changes, you need to specify the defaultValueS value, which is ''.
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

	dataDefault, err := postgresHandleColumnDefaultValueCharset(columnName, strings.TrimSpace(originDefaultValue), constant.CharsetUTF8MB4, targetCharset)
	if err != nil {
		return originDefaultValue, err
	}
	return dataDefault, nil
}

// postgresHandelColumnRuleWithTableSchemaTaskPriority priority
// table > schema > task -> server
func postgresHandelColumnRuleWithTableSchemaTaskPriority(sourceTable string, buildinDefaultValueRules []*buildin.BuildinDefaultvalRule, taskRules []*migrate.TaskStructRule, schemaRules []*migrate.SchemaStructRule, tableRules []*migrate.TableStructRule) (map[string]string, map[string]string) {
	taskDatatype, taskDefaultVal := postgresHandleColumnRuleWithTaskLevel(taskRules)

	schemaDatatype, schemaDefaultVal := postgresHandleColumnRuleWithSchemaLevel(schemaRules)

	tableDatatype, tableDefaultVal := postgresHandleColumnRuleWithTableLevel(sourceTable, tableRules)

	buildinDefaultValues := postgresHandleColumnDefaultValueRuleWithServerLevel(buildinDefaultValueRules)

	globalColumnDatatype := stringutil.ExchangeStringDict(tableDatatype, stringutil.ExchangeStringDict(schemaDatatype, taskDatatype))

	globalColumnDefaultVal := stringutil.ExchangeStringDict(tableDefaultVal, stringutil.ExchangeStringDict(schemaDefaultVal, stringutil.ExchangeStringDict(taskDefaultVal, buildinDefaultValues)))

	return globalColumnDatatype, globalColumnDefaultVal
}

// postgresHandleColumnRuleWithColumnHighPriority priority
// column high priority
func postgresHandleColumnRuleWithColumnHighPriority(columNameS string, columnRules []*migrate.ColumnStructRule) (map[string]map[string]string, map[string]map[string]string) {
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

func postgresHandleColumnDefaultValueRuleWithServerLevel(buildinRules []*buildin.BuildinDefaultvalRule) map[string]string {
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

func postgresHandleColumnRuleWithTaskLevel(taskRules []*migrate.TaskStructRule) (map[string]string, map[string]string) {
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

func postgresHandleColumnRuleWithSchemaLevel(schemaRules []*migrate.SchemaStructRule) (map[string]string, map[string]string) {
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

func postgresHandleColumnRuleWithTableLevel(sourceTable string, tableRules []*migrate.TableStructRule) (map[string]string, map[string]string) {
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

func postgresHandleColumnDefaultValueCharset(columnName, defaultVal, sourceCharset, targetCharset string) (string, error) {
	var dataDefault string
	// fixed nextval sequence nextval('marvin00_s00_seq'::regclass)
	if strings.Contains(defaultVal, "nextval(") {
		strSli := stringutil.StringSplit(defaultVal, "::regclass")
		return stringutil.StringBuilder(strSli[0], strSli[1]), nil
	}
	// fixed ::bpchar and ::regclass
	if strings.Contains(defaultVal, "::bpchar") || strings.Contains(defaultVal, "::regclass") {
		defaultVal = stringutil.StringSplit(defaultVal, "::")[0]
	}
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
		dataDefault = "'" + stringutil.BytesToString(convertTargetRaw) + "'"
	} else {
		if strings.EqualFold(stringutil.BytesToString(convertTargetRaw), constant.PostgresDatabaseTableColumnDefaultValueWithEmptyString) {
			dataDefault = "'" + stringutil.BytesToString(convertTargetRaw) + "'"
		} else if strings.EqualFold(stringutil.BytesToString(convertTargetRaw), constant.PostgresDatabaseTableColumnDefaultValueWithNULLSTRING) {
			dataDefault = constant.PostgresDatabaseTableColumnDefaultValueWithNULL
		} else {
			dataDefault = stringutil.BytesToString(convertTargetRaw)
		}
	}
	return dataDefault, nil
}
