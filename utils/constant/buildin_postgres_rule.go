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
	// pg's transaction snapshot export function was initially supported in version 9.3
	// which allows multiple transactions to share a consistent view of the same current database state that has been obtained
	// but pg 9.5 begins to support insert on conflict update syntax, similar to mysql replace into, so unified version requirements pg 9.5
	// PostgresqlDatabaseSnapshotReadSupportVersion = "9.3"
	PostgresqlDatabaseSupportVersionRequire = "9.5"

	PostgresqlDatabasePartitionTableSupportVersionRequire         = "10.0"
	PostgresqlDatabaseVirtualGeneratedColumnSupportVersionRequire = "12"

	PostgresDatabaseTableColumnDefaultValueWithEmptyString = ""
	PostgresDatabaseTableColumnDefaultValueWithNULLSTRING  = "NULLSTRING"
	PostgresDatabaseTableColumnDefaultValueWithNULL        = "NULL"

	PostgresDatabaseColumnDatatypeMatchRuleNotFound = "NOT FOUND"

	// Allow Postgres database role PRIMARY、STANDBY
	PostgresDatabasePrimaryRole = "PRIMARY"
)

var (
	/*
		Postgres support the table column charset and collation datatype
	*/
	PostgresDatabaseTableColumnSupportCharsetCollationDatatype = []string{
		BuildInPostgresDatatypeText,
		BuildInPostgresDatatypeCharacterVarying,
		BuildInPostgresDatatypeCharacter,
	}

	PostgresDatabaseTableColumnBinaryDatatype = []string{
		BuildInPostgresDatatypeBytea,
	}

	PostgresDatabaseTableColumnIntergerDatatype = []string{
		BuildInPostgresDatatypeInteger,
		BuildInPostgresDatatypeSmallInt,
		BuildInPostgresDatatypeBigInt,
		BuildInPostgresDatatypeNumeric,
		BuildInPostgresDatatypeDecimal,
	}
)

/*
	Database Datatype
*/
// Postgres column datatype
const (
	BuildInPostgresDatatypeInteger                  = "INTEGER"
	BuildInPostgresDatatypeSmallInt                 = "SMALLINT"
	BuildInPostgresDatatypeBigInt                   = "BIGINT"
	BuildInPostgresDatatypeBit                      = "BIT"
	BuildInPostgresDatatypeBoolean                  = "BOOLEAN"
	BuildInPostgresDatatypeReal                     = "REAL"
	BuildInPostgresDatatypeDoublePrecision          = "DOUBLE PRECISION"
	BuildInPostgresDatatypeNumeric                  = "NUMERIC"
	BuildInPostgresDatatypeDecimal                  = "DECIMAL"
	BuildInPostgresDatatypeMoney                    = "MONEY"
	BuildInPostgresDatatypeCharacter                = "CHARACTER"
	BuildInPostgresDatatypeCharacterVarying         = "CHARACTER VARYING"
	BuildInPostgresDatatypeDate                     = "DATE"
	BuildInPostgresDatatypeTimeWithoutTimeZone      = "TIME WITHOUT TIME ZONE"
	BuildInPostgresDatatypeTimestampWithoutTimeZone = "TIMESTAMP WITHOUT TIME ZONE"
	BuildInPostgresDatatypeInterval                 = "INTERVAL"
	BuildInPostgresDatatypeBytea                    = "BYTEA"
	BuildInPostgresDatatypeText                     = "TEXT"
	BuildInPostgresDatatypeCidr                     = "CIDR"
	BuildInPostgresDatatypeInet                     = "INET"
	BuildInPostgresDatatypeMacaddr                  = "MACADDR"
	BuildInPostgresDatatypeUuid                     = "UUID"
	BuildInPostgresDatatypeXml                      = "XML"
	BuildInPostgresDatatypeJson                     = "JSON"
	BuildInPostgresDatatypeTsvector                 = "TSVECTOR"
	BuildInPostgresDatatypeTsquery                  = "TSQUERY"
	BuildInPostgresDatatypeArray                    = "ARRAY"
	BuildInPostgresDatatypePoint                    = "POINT"
	BuildInPostgresDatatypeLine                     = "LINE"
	BuildInPostgresDatatypeLseg                     = "LSEG"
	BuildInPostgresDatatypeBox                      = "BOX"
	BuildInPostgresDatatypePath                     = "PATH"
	BuildInPostgresDatatypePolygon                  = "POLYGON"
	BuildInPostgresDatatypeCircle                   = "CIRCLE"
	BuildInPostgresDatatypeTxidSnapshot             = "TXID_SNAPSHOT"
)

// Postgres TO MYSQL column datatype mapping rule
var BuildInPostgresP2MDatatypeNameMap = map[string]string{
	BuildInPostgresDatatypeInteger:                  "INT",
	BuildInPostgresDatatypeSmallInt:                 "SMALLINT",
	BuildInPostgresDatatypeBigInt:                   "BIGINT",
	BuildInPostgresDatatypeBit:                      "BIT",
	BuildInPostgresDatatypeBoolean:                  "TINYINT", // default TINYINT(1)
	BuildInPostgresDatatypeReal:                     "FLOAT",
	BuildInPostgresDatatypeDoublePrecision:          "DOUBLE",
	BuildInPostgresDatatypeNumeric:                  "DECIMAL",
	BuildInPostgresDatatypeDecimal:                  "DECIMAL",
	BuildInPostgresDatatypeMoney:                    "DECIMAL", // default DECIMAL(19,2)
	BuildInPostgresDatatypeCharacter:                "CHAR/VARCHAR/LONGTEXT",
	BuildInPostgresDatatypeCharacterVarying:         "VARCHAR/MEDIUMTEXT/LONGTEXT",
	BuildInPostgresDatatypeDate:                     "DATE",
	BuildInPostgresDatatypeTimeWithoutTimeZone:      "TIME",
	BuildInPostgresDatatypeTimestampWithoutTimeZone: "DATETIME",
	BuildInPostgresDatatypeInterval:                 "TIME",
	BuildInPostgresDatatypeBytea:                    "LONGBLOB",
	BuildInPostgresDatatypeText:                     "TEXT",
	BuildInPostgresDatatypeCidr:                     "VARCHAR", // default VARCHAR(43)
	BuildInPostgresDatatypeInet:                     "VARCHAR", // default VARCHAR(43)
	BuildInPostgresDatatypeMacaddr:                  "VARCHAR", // default VARCHAR(17)
	BuildInPostgresDatatypeUuid:                     "VARCHAR", // default VARCHAR(36)
	BuildInPostgresDatatypeXml:                      "LONGTEXT",
	BuildInPostgresDatatypeJson:                     "LONGTEXT",
	BuildInPostgresDatatypeTsvector:                 "LONGTEXT",
	BuildInPostgresDatatypeTsquery:                  "LONGTEXT",
	BuildInPostgresDatatypeArray:                    "LONGTEXT",
	BuildInPostgresDatatypePoint:                    "POINT",
	BuildInPostgresDatatypeLine:                     "LINESTRING",
	BuildInPostgresDatatypeLseg:                     "LINESTRING",
	BuildInPostgresDatatypeBox:                      "POLYGON",
	BuildInPostgresDatatypePath:                     "LINESTRING",
	BuildInPostgresDatatypePolygon:                  "POLYGON",
	BuildInPostgresDatatypeCircle:                   "POLYGON",
	BuildInPostgresDatatypeTxidSnapshot:             "VARCHAR", // default VARCHAR(256)
}
