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

const (
	// MYSQL database check constraint support version > 8.0.15
	MYSQLDatabaseCheckConstraintSupportVersion = "8.0.15"
	TIDBDatabaseCheckConstraintSupportVersion  = "7.2.0"

	MYSQLDatabaseTableColumnDefaultValueWithEmptyString = ""
	MYSQLDatabaseTableColumnDefaultValueWithNULLSTRING  = "NULLSTRING"
	MYSQLDatabaseTableColumnDefaultValueWithNULL        = "NULL"

	PostgresDatabaseTableColumnDefaultValueWithEmptyString = ""
	PostgresDatabaseTableColumnDefaultValueWithNULLSTRING  = "NULLSTRING"
	PostgresDatabaseTableColumnDefaultValueWithNULL        = "NULL"

	PostgresDatabaseColumnDatatypeMatchRuleNotFound = "NOT FOUND"

	MYSQLCompatibleDatabaseVersionDelimiter = "-"

	// MYSQL database expression index support version > 8.0.0
	MYSQLDatabaseExpressionIndexSupportVersion = "8.0.0"

	MYSQLDatabaseSequenceSupportVersion  = "8.0"
	TIDBDatabaseSequenceSupportVersion   = "4.0"
	TIDBDatabaseImportIntoSupportVersion = "7.5"
)

// TiDB database integer primary key menu
var TiDBDatabaseIntegerColumnDatatypePrimaryKey = []string{"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "DECIMAL"}

// The default value of mysql is not differentiated between character data and numerical data.
// It is used to match the default value of mysql string and determine whether single quotes are required.
// 1, The default value uuid() matches the end of xxx() brackets, no single quotes are required
// 2, The default value CURRENT_TIMESTAMP does not require parentheses and is converted to ORACLE SYSDATE built-in
// 3, Default value skp or 1 requires single quotes
var MYSQLCompatibleDatabaseTableColumnDatatypeStringDefaultValueApostrophe = []string{"TIME",
	"DATE",
	"DATETIME",
	"TIMESTAMP",
	"CHAR",
	"VARCHAR",
	"TINYTEXT",
	"TEXT", "MEDIUMTEX", "LONGTEXT", "BIT", "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB"}

// MYSQL compatibe database table datatype reverse oracle CLOB or NCLOB configure collation error, need configure columnCollation = ""
// ORA-43912: invalid collation specified for a CLOB or NCLOB value
var MYSQLCompatibleDatabaseTableBigTextColumnCollation = []string{"TINYTEXT",
	"TEXT",
	"MEDIUMTEXT",
	"LONGTEXT"}

var MYSQLCompatibleDatabaseTableIntegerColumnDatatypeWidthExcludeDecimal = map[string]string{
	"TINYINT":   "4",
	"SMALLINT":  "6",
	"MEDIUMINT": "9",
	"INT":       "11",
	"BIGINT":    "20",
}

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
