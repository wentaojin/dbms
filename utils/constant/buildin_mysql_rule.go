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
package constant

// MYSQL Charset
const (
	BuildInMYSQLCharsetUTF8MB4 = "UTF8MB4"
)

// MYSQL datatype
const (
	BuildInMySQLDatatypeBigint          = "BIGINT"
	BuildInMySQLDatatypeDecimal         = "DECIMAL"
	BuildInMySQLDatatypeDouble          = "DOUBLE"
	BuildInMySQLDatatypeDoublePrecision = "DOUBLE PRECISION"
	BuildInMySQLDatatypeFloat           = "FLOAT"
	BuildInMySQLDatatypeInt             = "INT"
	BuildInMySQLDatatypeInteger         = "INTEGER"
	BuildInMySQLDatatypeMediumint       = "MEDIUMINT"
	BuildInMySQLDatatypeNumeric         = "NUMERIC"
	BuildInMySQLDatatypeReal            = "REAL"
	BuildInMySQLDatatypeSmallint        = "SMALLINT"
	BuildInMySQLDatatypeTinyint         = "TINYINT"
	BuildInMySQLDatatypeBit             = "BIT"
	BuildInMySQLDatatypeDate            = "DATE"
	BuildInMySQLDatatypeDatetime        = "DATETIME"
	BuildInMySQLDatatypeTimestamp       = "TIMESTAMP"
	BuildInMySQLDatatypeTime            = "TIME"
	BuildInMySQLDatatypeYear            = "YEAR"

	BuildInMySQLDatatypeBlob       = "BLOB"
	BuildInMySQLDatatypeChar       = "CHAR"
	BuildInMySQLDatatypeLongBlob   = "LONGBLOB"
	BuildInMySQLDatatypeLongText   = "LONGTEXT"
	BuildInMySQLDatatypeMediumBlob = "MEDIUMBLOB"
	BuildInMySQLDatatypeMediumText = "MEDIUMTEXT"
	BuildInMySQLDatatypeText       = "TEXT"
	BuildInMySQLDatatypeTinyBlob   = "TINYBLOB"
	BuildInMySQLDatatypeTinyText   = "TINYTEXT"
	BuildInMySQLDatatypeVarchar    = "VARCHAR"

	BuildInMySQLDatatypeBinary    = "BINARY"
	BuildInMySQLDatatypeVarbinary = "VARBINARY"

	// ORACLE ISN'T SUPPORT
	BuildInMySQLDatatypeSet  = "SET"
	BuildInMySQLDatatypeEnum = "ENUM"
)

// MYSQL TO ORACLE column datatype mapping rule
var BuildInMySQLM2ODatatypeNameMap = map[string]string{
	BuildInMySQLDatatypeSmallint:        "NUMBER",
	BuildInMySQLDatatypeTinyint:         "NUMBER",
	BuildInMySQLDatatypeBigint:          "NUMBER",
	BuildInMySQLDatatypeDecimal:         "DECIMAL",
	BuildInMySQLDatatypeDouble:          "BINARY_DOUBLE",
	BuildInMySQLDatatypeDoublePrecision: "BINARY_DOUBLE",
	BuildInMySQLDatatypeFloat:           "BINARY_FLOAT",
	BuildInMySQLDatatypeInt:             "NUMBER",
	BuildInMySQLDatatypeInteger:         "NUMBER",
	BuildInMySQLDatatypeMediumint:       "NUMBER",
	BuildInMySQLDatatypeNumeric:         "NUMBER",
	BuildInMySQLDatatypeReal:            "BINARY_FLOAT",
	BuildInMySQLDatatypeBit:             "RAW",
	BuildInMySQLDatatypeDate:            "DATE",
	BuildInMySQLDatatypeDatetime:        "DATE",
	BuildInMySQLDatatypeTimestamp:       "TIMESTAMP",
	BuildInMySQLDatatypeTime:            "DATE",
	BuildInMySQLDatatypeYear:            "NUMBER",
	BuildInMySQLDatatypeBlob:            "BLOB",
	BuildInMySQLDatatypeChar:            "CHAR",
	BuildInMySQLDatatypeLongBlob:        "BLOB",
	BuildInMySQLDatatypeLongText:        "CLOB",
	BuildInMySQLDatatypeMediumBlob:      "BLOB",
	BuildInMySQLDatatypeMediumText:      "CLOB",
	BuildInMySQLDatatypeText:            "CLOB",
	BuildInMySQLDatatypeTinyBlob:        "BLOB",
	BuildInMySQLDatatypeTinyText:        "VARCHAR2",
	BuildInMySQLDatatypeVarchar:         "VARCHAR2",
	BuildInMySQLDatatypeBinary:          "RAW",
	BuildInMySQLDatatypeVarbinary:       "RAW",
}
