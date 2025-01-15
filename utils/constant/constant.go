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

// Database Type
const (
	DatabaseTypeOracle         = "ORACLE"
	DatabaseTypeMySQL          = "MYSQL"
	DatabaseTypeTiDB           = "TIDB"
	DatabaseTypePostgresql     = "POSTGRES"
	DatabaseTypeOceanbaseMYSQL = "OB_MYSQL"
	DatabaseTypeOceanbase      = "OCEANBASE"
)

// DefaultRecordCreateBatchSize Model Create Record Default Batch Size
const (
	DefaultRecordCreateBatchSize   = 50
	DefaultRecordCreateWriteThread = 4
)

const (
	StringSeparatorComma         = ","
	StringSeparatorDot           = "."
	StringSeparatorBacktick      = "`"
	StringSeparatorSlash         = "/"
	StringSeparatorAite          = "@"
	StringSplicingSymbol         = "||"
	StringSeparatorSemicolon     = ";"
	StringSeparatorUnderline     = "_"
	StringSeparatorCenterLine    = "-"
	StringSeparatorComplexSymbol = "|+|"
	StringSeparatorDoubleQuotes  = "\""
	StringSeparatorDoubleColon   = ":"
	StringSeparatorAsterisk      = "*"
)

const (
	MigrateTaskTableIsExclude    = "YES"
	MigrateTaskTableIsNotExclude = "NO"
)

// DefaultMigrateTaskQueueSize used for queue channel size
const DefaultMigrateTaskQueueSize = 1024

// DefaultMigrateTaskBufferIOSize used for buffer io size
const DefaultMigrateTaskBufferIOSize = 4096

// DefaultDataEncryptDecryptKey used for data encrypt and decrypt key
const DefaultDataEncryptDecryptKey = "marvin@jwt!#$123qwer9797"

// MYSQL Compatible Database Bigint/Bigint unsigned Bound
const (
	DefaultMYSQLCompatibleBigintLowBound           = "-9223372036854775808"
	DefaultMYSQLCompatibleBigintUpperBound         = "9223372036854775807"
	DefaultMYSQLCompatibleBigintUnsignedLowBound   = "9223372036854775808"
	DefaultMYSQLCompatibleBigintUnsignedUpperBound = "18446744073709551615"
	DefaultMYSQLCompatibleDecimalLowerBound        = "18446744073709551616"
)
