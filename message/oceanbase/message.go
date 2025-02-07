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
package oceanbase

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/wentaojin/dbms/message"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

const (
	MsgRecordTypeInsert    = "INSERT"
	MsgRecordTypeUpdate    = "UPDATE"
	MsgRecordTypeDelete    = "DELETE"
	MsgRecordTypeHeartbeat = "HEARTBEAT"
	MsgRecordTypeDDL       = "DDL"
	MsgRecordTypeROW       = "ROW"
)

// https://www.oceanbase.com/docs/enterprise-oms-doc-cn-1000000001781775
// DefaultExtendColumnType JSON Message
// The DefaultExtendColumnType JSON message format adds a field __light_type to the postStruct image based on DEFAULT to indicate the data type of the field.
type Message struct {
	/*
		DELETE only has prevStruct, INSERT and DDL only have postStruct, UPDATE has both prevStruct and postStruct, HEARTBEAT (regular heartbeat message) does not have postStruct and postStruct.
	*/
	// the table record kv image before change
	PrevStruct map[string]interface{} `json:"prevStruct"`
	// the table record kv image after change
	PostStruct map[string]interface{} `json:"postStruct"`

	Metadata allMetadata `json:"allMetaData"`
	// INSERT/UPDATE/DELETE/HEARTBEAT/DDL/ROW
	RecordType string `json:"recordType"`
}

type allMetadata struct {
	Checkpoint         string `json:"checkpoint"`
	RecordPrimaryKey   string `json:"record_primary_key"`
	RecordPrimaryValue string `json:"record_primary_value"`
	SourceIdentity     string `json:"source_identity"`
	DbType             string `json:"dbType"`
	StoreDataSequence  uint64 `json:"storeDataSequence"`
	TableName          string `json:"table_name"`
	DbName             string `json:"db"`
	Timestamp          string `json:"timestamp"`
	UniqueID           string `json:"uniqueId"`
	TransID            string `json:"transId"`
	ClusterID          string `json:"clusterId"`
	DdlType            string `json:"ddlType"`
}

func (m *Message) String() string {
	js, _ := json.Marshal(m)
	return string(js)
}

// oceanbase message record database db has tenants, the format is tenant.schema
func (m *Message) DecodeDDLChangedEvent(caseFieldRuleS string) *DDLChangedEvent {
	var schemaName, tableName string
	schemaName = strings.Split(m.Metadata.DbName, ".")[1]
	tableName = m.Metadata.TableName
	switch caseFieldRuleS {
	case constant.ParamValueRuleCaseFieldNameUpper:
		schemaName = strings.ToUpper(schemaName)
		tableName = strings.ToUpper(tableName)
	case constant.ParamValueRuleCaseFieldNameLower:
		schemaName = strings.ToLower(schemaName)
		tableName = strings.ToLower(tableName)
	}

	return &DDLChangedEvent{
		SchemaName: schemaName,
		TableName:  tableName,
		DdlQuery:   m.PostStruct["ddl"].(string),
		DdlType:    m.Metadata.DdlType,
		CommitTs:   m.Metadata.StoreDataSequence,
	}
}

func (m *Message) DecodeRowChangedEvent(dbTypeS, caseFieldRuleS string) (*RowChangedEvent, error) {
	validUniqs := make(map[string]interface{})

	var schemaName, tableName string
	schemaName = strings.Split(m.Metadata.DbName, ".")[1]
	tableName = m.Metadata.TableName
	switch caseFieldRuleS {
	case constant.ParamValueRuleCaseFieldNameUpper:
		schemaName = strings.ToUpper(schemaName)
		tableName = strings.ToUpper(tableName)
	case constant.ParamValueRuleCaseFieldNameLower:
		schemaName = strings.ToLower(schemaName)
		tableName = strings.ToLower(tableName)
	}

	pkNames := strings.Split(m.Metadata.RecordPrimaryKey, "\u0001")
	pkValues := strings.Split(m.Metadata.RecordPrimaryValue, "\u0001")

	for i, n := range pkNames {
		var name string
		switch caseFieldRuleS {
		case constant.ParamValueRuleCaseFieldNameUpper:
			name = strings.ToUpper(n)
		case constant.ParamValueRuleCaseFieldNameLower:
			name = strings.ToLower(n)
		default:
			name = n
		}
		validUniqs[name] = pkValues[i]
	}
	// extract column type (__light_type)
	columnType := make(map[string]string)
	postStruct := make(map[string]interface{})
	prevStruct := make(map[string]interface{})
	isDefaultExtendColumnType := false

	for c, v := range m.PostStruct {
		if c == "__light_type" {
			for k, t := range v.(map[string]interface{}) {
				var name string
				switch caseFieldRuleS {
				case constant.ParamValueRuleCaseFieldNameUpper:
					name = strings.ToUpper(k)
				case constant.ParamValueRuleCaseFieldNameLower:
					name = strings.ToLower(k)
				default:
					name = k
				}
				columnType[name] = t.(map[string]interface{})["schemaType"].(string)
			}
			isDefaultExtendColumnType = true
		}
	}

	// Byte array, displayed in BASE64 encoding by default.
	// Note: For BIT fixed-length types, the high-order 0s will be removed after the byte array is received incrementally, but not for the full amount, so the BASE64 encoding you see may be inconsistent. However, the actual results are consistent, and the results after decoding are consistent.
	//	"TINYBLOB",
	//	"BLOB",
	//	"MEDIUMBLOB",
	//	"LONGBLOB",
	//	"BINARY",
	//	"VARBINARY",
	//	"BIT"
	for c, v := range m.PostStruct {
		if c != "__light_type" {
			var name string
			switch caseFieldRuleS {
			case constant.ParamValueRuleCaseFieldNameUpper:
				name = strings.ToUpper(c)
			case constant.ParamValueRuleCaseFieldNameLower:
				name = strings.ToLower(c)
			default:
				name = c
			}
			if v == nil {
				postStruct[name] = v
			} else {
				if strings.EqualFold(dbTypeS, constant.DatabaseTypeOceanbaseMYSQL) {
					if typs, ok := columnType[name]; ok {
						if stringutil.IsContainedStringIgnoreCase(message.OceanbaseMsgColumnBytesDatatype, typs) {
							decodedBytes, err := base64.StdEncoding.DecodeString(v.(string))
							if err != nil {
								return nil, err
							}
							postStruct[name] = decodedBytes
						} else if strings.EqualFold(typs, constant.BuildInMySQLDatatypeTimestamp) {
							// The OceanBase database converts TIMESTAMP values ​​from the current time zone to UTC for storage, and then converts from UTC back to the current time zone for retrieval. By default, the current time zone for each connection follows the server's time, but the time zone for each connection can be changed. If the same time zone is not used for bidirectional conversion, the retrieved value may be different from the stored value after changing the time zone. As long as the time zone setting remains unchanged, the stored value can be retrieved. The current time zone is available as the value of the time_zone system variable.
							times := stringutil.StringSplit(v.(string), ".")
							unixTs, err := strconv.ParseInt(times[0], 10, 64)
							if err != nil {
								return nil, err
							}
							var nanos int64
							if len(times) == 2 {
								nanos, err = strconv.ParseInt(times[1], 10, 64)
								if err != nil {
									return nil, err
								}
							} else {
								nanos = 0
							}
							timeObj, err := createTimeWithOffset(unixTs, nanos, upMetaCache.GetTimezone())
							if err != nil {
								return nil, err
							}
							postStruct[name] = formatTimeWithNanos(timeObj, 9)
						} else {
							postStruct[name] = v
						}
					}
				} else {
					postStruct[name] = v
				}
			}
		}
	}

	for c, v := range m.PrevStruct {
		var name string
		switch caseFieldRuleS {
		case constant.ParamValueRuleCaseFieldNameUpper:
			name = strings.ToUpper(c)
		case constant.ParamValueRuleCaseFieldNameLower:
			name = strings.ToLower(c)
		default:
			name = c
		}
		prevStruct[name] = v

		if c == "__light_type" {
			isDefaultExtendColumnType = true
		}
	}

	if !isDefaultExtendColumnType {
		return nil, fmt.Errorf("the message format require DefaultExtendColumnType JSON, not found __light_type colum")
	}

	return &RowChangedEvent{
		SchemaName: schemaName,
		TableName:  tableName,
		QueryType:  m.RecordType,
		CommitTs:   m.Metadata.StoreDataSequence,

		ValidUniqColumns: validUniqs,
		ColumnType:       columnType,
		NewColumnData:    postStruct,
		OldColumnData:    prevStruct,
	}, nil
}

// parseOffset 解析时区偏移量字符串（如 "+08:00" 或 "-07:00"）并返回小时偏移量
func parseOffset(offsetStr string) (int, error) {
	re := regexp.MustCompile(`([+-])(\d{2}):(\d{2})`)
	matches := re.FindStringSubmatch(offsetStr)
	if matches == nil || len(matches) != 4 {
		return 0, fmt.Errorf("invalid offset format: %s", offsetStr)
	}

	sign := matches[1]
	hours, err := strconv.Atoi(matches[2])
	if err != nil {
		return 0, err
	}
	minutes, err := strconv.Atoi(matches[3])
	if err != nil {
		return 0, err
	}

	offset := hours*60 + minutes
	if sign == "-" {
		offset = -offset
	}

	return offset, nil
}

// createTimeWithOffset creates a time.Time object from the given timestamp, nanoseconds, and time zone offset
func createTimeWithOffset(unixTimestamp int64, nanos int64, offsetStr string) (time.Time, error) {
	offsetMinutes, err := parseOffset(offsetStr)
	if err != nil {
		return time.Time{}, err
	}

	location := time.FixedZone(fmt.Sprintf("UTC%+d", offsetMinutes/60), offsetMinutes*60)
	return time.Unix(unixTimestamp, nanos).In(location), nil
}

// formatTimeWithNanos formats the time as a time string containing decimals (nanoseconds) of the specified precision
func formatTimeWithNanos(t time.Time, nanosPrecision int) string {
	if nanosPrecision < 0 || nanosPrecision > 9 {
		nanosPrecision = 9 // The default setting is maximum precision
	}

	// get the nanosecond part and truncate it according to the specified precision
	nanos := t.Nanosecond()
	precisionFactor := int64(1e9 / pow10(nanosPrecision))
	truncatedNanos := (int64(nanos) + precisionFactor/2) / precisionFactor * precisionFactor
	formattedNanos := fmt.Sprintf("%09d", truncatedNanos)[:nanosPrecision]

	layout := "2006-01-02 15:04:05." + formattedNanos

	return t.Format(layout)
}

// auxiliary function, calculate the power of 10
func pow10(exp int) int64 {
	result := int64(1)
	for i := 0; i < exp; i++ {
		result *= 10
	}
	return result
}
