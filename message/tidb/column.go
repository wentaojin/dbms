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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
)

// BatchVersion represents the version of ticdc batch key-value format
const BatchVersion uint64 = 1

type ColumnType uint64

const (
	TypeUnspecified ColumnType = 0
	TypeTinyint     ColumnType = 1 // TINYINT / BOOL
	TypeSmallint    ColumnType = 2 // SMALLINT
	TypeInt         ColumnType = 3 // INT
	TypeFloat       ColumnType = 4
	TypeDouble      ColumnType = 5
	TypeNull        ColumnType = 6
	TypeTimestamp   ColumnType = 7
	TypeBigint      ColumnType = 8  // BIGINT
	TypeMediumint   ColumnType = 9  // MEDIUMINT
	TypeDate        ColumnType = 10 // Date -> 10/14
	TypeTime        ColumnType = 11
	TypeDatetime    ColumnType = 12
	TypeYear        ColumnType = 13
	TypeNewDate     ColumnType = 14
	TypeVarchar     ColumnType = 15
	TypeBit         ColumnType = 16

	TypeJSON    ColumnType = 245
	TypeDecimal ColumnType = 246
	TypeEnum    ColumnType = 247
	TypeSet     ColumnType = 248

	// value 编码为 Base64
	TypeTinyBlob   ColumnType = 249 // TINYTEXT/TINYBLOB -> 249
	TypeMediumBlob ColumnType = 250 // MEDIUMTEXT/MEDIUMBLOB -> 250
	TypeLongBlob   ColumnType = 251 // LONGTEXT/LONGBLOB -> 251
	TypeBlob       ColumnType = 252 // TEXT/BLOB -> 252
	TypeVarbinary  ColumnType = 253

	// value 编码为 UTF-8；当上游类型为 BINARY 时，将对不可见的字符转义
	TypeChar ColumnType = 254 // CHAR/BINARY

	/// not support
	TypeGeometry          ColumnType = 255
	TypeTiDBVectorFloat32 ColumnType = 256

	// 处理同个 ColumnType 代表不同数据类型 Case
	TypeBool   ColumnType = 1001
	TypeBinary ColumnType = 1002
)

var ColumnTypeMap = map[ColumnType]string{
	TypeUnspecified:       "NONE",
	TypeTinyint:           "TINYINT",
	TypeBool:              "BOOL",
	TypeSmallint:          "SMALLINT",
	TypeInt:               "INT",
	TypeFloat:             "FLOAT",
	TypeDouble:            "DOUBLE",
	TypeNull:              "NULL",
	TypeTimestamp:         "TIMESTAMP",
	TypeBigint:            "BIGINT",
	TypeMediumint:         "MEDIUMINT",
	TypeDate:              "DATE",
	TypeTime:              "TIME",
	TypeDatetime:          "DATETIME",
	TypeYear:              "YEAR",
	TypeNewDate:           "DATE",
	TypeVarchar:           "VARCHAR",
	TypeBit:               "BIT",
	TypeJSON:              "JSON",
	TypeDecimal:           "DECIMAL",
	TypeEnum:              "ENUM",
	TypeSet:               "SET",
	TypeTinyBlob:          "TINYBLOB",
	TypeMediumBlob:        "MEDIUMBLOB",
	TypeLongBlob:          "LONGBLOB",
	TypeBlob:              "BLOB",
	TypeVarbinary:         "VARBINARY",
	TypeChar:              "CHAR",
	TypeBinary:            "Binary",
	TypeGeometry:          "GEOMETRY",
	TypeTiDBVectorFloat32: "VECTOR",
}

func (c ColumnType) ColumnType() string {
	if val, ok := ColumnTypeMap[c]; ok {
		return val
	}
	return "NONE"
}

// Flag is a uint64 flag to show a 64 bit mask
type Flag uint64

// HasAll means has all flags
func (f Flag) HasAll(flags ...Flag) bool {
	for _, flag := range flags {
		if flag&f == 0 {
			return false
		}
	}
	return true
}

// HasOne means has one of the flags
func (f Flag) HasOne(flags ...Flag) bool {
	for _, flag := range flags {
		if flag&f != 0 {
			return true
		}
	}
	return false
}

// Add add flags
func (f *Flag) Add(flags ...Flag) {
	for _, flag := range flags {
		*f |= flag
	}
}

// Remove remove flags
func (f *Flag) Remove(flags ...Flag) {
	for _, flag := range flags {
		*f ^= flag
	}
}

// Clear clear all flags
func (f *Flag) Clear() {
	*f ^= *f
}

// ColumnFlagType is for encapsulating the flag operations for different flags.
type ColumnFlagType Flag

const (
	// BinaryFlag means the column charset is binary
	BinaryFlag ColumnFlagType = 1 << ColumnFlagType(iota)
	// HandleKeyFlag means the column is selected as the handle key
	// The handleKey is chosen by the following rules in the order:
	// 1. if the table has primary key, it's the handle key.
	// 2. If the table has not null unique key, it's the handle key.
	// 3. If the table has no primary key and no not null unique key, it has no handleKey.
	HandleKeyFlag
	// GeneratedColumnFlag means the column is a generated column
	GeneratedColumnFlag
	// PrimaryKeyFlag means the column is primary key
	PrimaryKeyFlag
	// UniqueKeyFlag means the column is unique key
	UniqueKeyFlag
	// MultipleKeyFlag means the column is multiple key
	MultipleKeyFlag
	// NullableFlag means the column is nullable
	NullableFlag
	// UnsignedFlag means the column stores an unsigned integer
	UnsignedFlag
)

// IsBinary shows whether BinaryFlag is set
func (b ColumnFlagType) IsBinary() bool {
	return (Flag)(b).HasAll(Flag(BinaryFlag))
}

// IsHandleKey shows whether HandleKey is set
func (b ColumnFlagType) IsHandleKey() bool {
	return (Flag)(b).HasAll(Flag(HandleKeyFlag))
}

// IsGeneratedColumn shows whether GeneratedColumn is set
func (b ColumnFlagType) IsGeneratedColumn() bool {
	return (Flag)(b).HasAll(Flag(GeneratedColumnFlag))
}

// IsPrimaryKey shows whether PrimaryKeyFlag is set
func (b ColumnFlagType) IsPrimaryKey() bool {
	return (Flag)(b).HasAll(Flag(PrimaryKeyFlag))
}

// IsUniqueKey shows whether UniqueKeyFlag is set
func (b ColumnFlagType) IsUniqueKey() bool {
	return (Flag)(b).HasAll(Flag(UniqueKeyFlag))
}

// IsMultipleKey shows whether MultipleKeyFlag is set
func (b ColumnFlagType) IsMultipleKey() bool {
	return (Flag)(b).HasAll(Flag(MultipleKeyFlag))
}

// IsNullable shows whether NullableFlag is set
func (b ColumnFlagType) IsNullable() bool {
	return (Flag)(b).HasAll(Flag(NullableFlag))
}

// IsUnsigned shows whether UnsignedFlag is set
func (b ColumnFlagType) IsUnsigned() bool {
	return (Flag)(b).HasAll(Flag(UnsignedFlag))
}

// FormatColumn formats a codec column.
func FormatColumn(c Column) (Column, error) {
	switch c.ColumnType {
	case TypeTinyBlob, TypeMediumBlob, TypeLongBlob, TypeBlob:
		if s, ok := c.ColumnValue.(string); ok {
			var err error
			c.ColumnValue, err = base64.StdEncoding.DecodeString(s)
			if err != nil {
				return c, fmt.Errorf("invalid column value [%v], base64 decoding string failed: %v", c.String(), err)
			}
		}
	case TypeFloat, TypeDouble:
		if s, ok := c.ColumnValue.(json.Number); ok {
			f64, err := s.Float64()
			if err != nil {
				return c, fmt.Errorf("invalid column value [%v], floa64 conv failed: %v", c.String(), err)
			}
			c.ColumnValue = f64
		}
	case TypeTinyint, TypeSmallint, TypeInt, TypeBigint, TypeMediumint, TypeYear:
		if s, ok := c.ColumnValue.(json.Number); ok {
			var err error
			if c.ColumnFlag.IsUnsigned() {
				c.ColumnValue, err = strconv.ParseUint(s.String(), 10, 64)
			} else {
				c.ColumnValue, err = strconv.ParseInt(s.String(), 10, 64)
			}
			if err != nil {
				return c, fmt.Errorf("invalid column value [%v], parse uint failed: %v", c.String(), err)
			}
		} else if f, ok := c.ColumnValue.(float64); ok {
			if c.ColumnFlag.IsUnsigned() {
				c.ColumnValue = uint64(f)
			} else {
				c.ColumnValue = int64(f)
			}
		}
	case TypeBit:
		if s, ok := c.ColumnValue.(json.Number); ok {
			intNum, err := s.Int64()
			if err != nil {
				return c, fmt.Errorf("invalid column value [%v], parse int64 failed: %v", c.String(), err)
			}
			c.ColumnValue = uint64(intNum)
		}
	}
	return c, nil
}
