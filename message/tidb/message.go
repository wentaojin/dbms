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
package tidb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/wentaojin/dbms/utils/constant"
)

// MsgEventType is the type of message, which is used by MqSink and RedoLog.
type MsgEventType int

const (
	// MsgEventTypeUnknown is unknown type of message key
	MsgEventTypeUnknown MsgEventType = iota
	// MsgEventTypeRow is row type of message key
	MsgEventTypeRow
	// MsgEventTypeDDL is ddl type of message key
	MsgEventTypeDDL
	// MsgEventTypeResolved is resolved type of message key
	MsgEventTypeResolved
)

type MessageEventKey struct {
	CommitTs     uint64       `json:"ts"`
	SchemaName   string       `json:"scm,omitempty"`
	TableName    string       `json:"tbl,omitempty"`
	RowID        int64        `json:"rid,omitempty"`
	MsgEventType MsgEventType `json:"t"`
}

type MessageDDLEventValue struct {
	DdlQuery string  `json:"q"`
	DdlType  DDLType `json:"t"`
}

type MessageRowEventValue struct {
	// Update Event 统一拆分 Delete/Insert，Before 对应 Delete
	// 如果 before 值存在则说明 Upsert 对应 Update Event，否则说明 Upsert 对应 Insert Event
	Upsert map[string]Column `json:"u,omitempty"`
	Before map[string]Column `json:"p,omitempty"`
	Delete map[string]Column `json:"d,omitempty"`
}

type Column struct {
	ColumnType  ColumnType     `json:"t"`
	WhereHandle bool           `json:"h,omitempty"`
	ColumnFlag  ColumnFlagType `json:"f"`
	ColumnValue interface{}    `json:"v"`
}

// ConvRowChangeColumn converts from a codec column to a row changed event column.
func (c *Column) ConvRowChangeColumn(name string) (*ConvColumn, error) {
	col := new(ConvColumn)
	col.ColumnFlag = c.ColumnFlag
	col.ColumnName = name
	col.ColumnType = c.ColumnType.ColumnType()
	col.ColumnValue = c.ColumnValue
	if c.ColumnValue == nil {
		return col, nil
	}

	/*
		BinaryFlag is meaningful only when the column is of BLOB/TEXT (including TINYBLOB/TINYTEXT, BINARY/CHAR, etc.) type.
		When the upstream column is of BLOB type, BinaryFlag is set to 1; when the upstream column is of TEXT type, BinaryFlag is set to 0.
	*/
	switch c.ColumnType {
	case TypeVarchar, TypeVarbinary:
		str := col.ColumnValue.(string)
		var err error

		// the value is encoded as UTF-8. When the upstream type is VARBINARY, the data has escaped invisible characters and needs to be reversed.
		if c.ColumnFlag.IsBinary() {
			str, err = strconv.Unquote("\"" + str + "\"")
			if err != nil {
				return col, fmt.Errorf("invalid column value [%s], strconv unquote failed: %v", c.String(), err)
			}
			col.ColumnValue = []byte(str)
			col.ColumnType = TypeVarbinary.ColumnType()
		} else {
			col.ColumnValue = str
			col.ColumnType = c.ColumnType.ColumnType()
		}
	case TypeChar:
		str := col.ColumnValue.(string)
		var err error

		// the value is encoded as UTF-8. When the upstream type is BINARY, the data has escaped invisible characters and needs to be reversed.
		if c.ColumnFlag.IsBinary() {
			str, err = strconv.Unquote("\"" + str + "\"")
			if err != nil {
				return col, fmt.Errorf("invalid column value [%s], strconv unquote failed: %v", c.String(), err)
			}
			col.ColumnValue = []byte(str)
			col.ColumnType = TypeBinary.ColumnType()
		} else {
			col.ColumnValue = str
			col.ColumnType = c.ColumnType.ColumnType()
		}
	case TypeTinyBlob:
		str := col.ColumnValue.(string)
		if c.ColumnFlag.IsBinary() {
			col.ColumnValue = []byte(str)
			col.ColumnType = c.ColumnType.ColumnType()
		} else {
			col.ColumnValue = str
			col.ColumnType = TypeTinyText.ColumnType()
		}

	case TypeMediumBlob:
		str := col.ColumnValue.(string)
		if c.ColumnFlag.IsBinary() {
			col.ColumnValue = []byte(str)
			col.ColumnType = c.ColumnType.ColumnType()
		} else {
			col.ColumnValue = str
			col.ColumnType = TypeMediumText.ColumnType()
		}
	case TypeLongBlob:
		str := col.ColumnValue.(string)
		if c.ColumnFlag.IsBinary() {
			col.ColumnValue = []byte(str)
			col.ColumnType = c.ColumnType.ColumnType()
		} else {
			col.ColumnValue = str
			col.ColumnType = TypeLongText.ColumnType()
		}
	case TypeBlob:
		str := col.ColumnValue.(string)
		if c.ColumnFlag.IsBinary() {
			col.ColumnValue = []byte(str)
			col.ColumnType = c.ColumnType.ColumnType()
		} else {
			col.ColumnValue = str
			col.ColumnType = TypeText.ColumnType()
		}
	case TypeFloat:
		col.ColumnValue = float32(col.ColumnValue.(float64))
	case TypeYear:
		col.ColumnValue = int64(col.ColumnValue.(uint64))
	case TypeEnum, TypeSet:
		val, err := col.ColumnValue.(json.Number).Int64()
		if err != nil {
			return col, fmt.Errorf("invalid column value for enum [%s], assert json.Number failed: %v", c.String(), err)
		}
		col.ColumnValue = uint64(val)
	case TypeBit:
		col.ColumnValue = strconv.FormatUint(col.ColumnValue.(uint64), 2)
	}

	return col, nil
}

func (m *Column) String() string {
	jsonBy, _ := json.Marshal(m)
	return string(jsonBy)
}

// Encode encodes the message key to a byte slice.
func (m *MessageEventKey) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return data, fmt.Errorf("marshal message event key failed: %v", err)
	}
	return data, nil
}

// Decode codes a message key from a byte slice.
func (m *MessageEventKey) Decode(data []byte) error {
	if err := json.Unmarshal(data, m); err != nil {
		return fmt.Errorf("unmarshal message event key failed: %v", err)
	}
	return nil
}

func (m *MessageRowEventValue) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return data, fmt.Errorf("marshal message event row failed: %v", err)
	}
	return data, nil
}

func (m *MessageRowEventValue) Decode(caseFieldRuleS string, data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err := decoder.Decode(m)
	if err != nil {
		return fmt.Errorf("unmarshal message event row failed: %v", err)
	}

	for colName, column := range m.Upsert {
		newColumn, err := FormatColumn(column)
		if err != nil {
			return err
		}
		switch caseFieldRuleS {
		case constant.ParamValueRuleCaseFieldNameUpper:
			colName = strings.ToUpper(colName)
		case constant.ParamValueRuleCaseFieldNameLower:
			colName = strings.ToLower(colName)
		}
		m.Upsert[colName] = newColumn
	}
	for colName, column := range m.Delete {
		newColumn, err := FormatColumn(column)
		if err != nil {
			return err
		}
		switch caseFieldRuleS {
		case constant.ParamValueRuleCaseFieldNameUpper:
			colName = strings.ToUpper(colName)
		case constant.ParamValueRuleCaseFieldNameLower:
			colName = strings.ToLower(colName)
		}
		m.Delete[colName] = newColumn
	}
	for colName, column := range m.Before {
		newColumn, err := FormatColumn(column)
		if err != nil {
			return err
		}
		switch caseFieldRuleS {
		case constant.ParamValueRuleCaseFieldNameUpper:
			colName = strings.ToUpper(colName)
		case constant.ParamValueRuleCaseFieldNameLower:
			colName = strings.ToLower(colName)
		}
		m.Before[colName] = newColumn
	}
	return nil
}

func (m *MessageDDLEventValue) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return data, fmt.Errorf("marshal ddl changed message event failed: %v", err)
	}
	return data, nil
}

func (m *MessageDDLEventValue) Decode(data []byte) error {
	if err := json.Unmarshal(data, m); err != nil {
		return fmt.Errorf("unmarshal ddl changed message event failed: %v", err)
	}
	return nil
}
