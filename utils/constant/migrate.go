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
	// Allow Oracle database role PRIMARY、LOGICAL STANDBY、PHYSICAL STANDBY
	OracleDatabasePrimaryRole = "PRIMARY"
	// Allow Oracle table migrate rowid require version
	OracleDatabaseTableMigrateRowidRequireVersion = "11"
	// Allow Oracle table, field Collation, requires oracle 12.2g and above
	OracleDatabaseTableAndColumnSupportVersion = "12.2"

	// Oracle user、table、column default sort collation
	OracleDatabaseUserTableColumnDefaultCollation = "USING_NLS_COMP"

	// Oracle database table type
	OracleDatabaseTableTypeHeapTable                 = "HEAP"
	OracleDatabaseTableTypePartitionTable            = "PARTITIONED"
	OracleDatabaseTableTypeSessionTemporaryTable     = "SESSION TEMPORARY"
	OracleDatabaseTableTypeTransactionTemporaryTable = "TRANSACTION TEMPORARY"
	OracleDatabaseTableTypeClusteredTable            = "CLUSTERED"
	OracleDatabaseTableTypeMaterializedView          = "MATERIALIZED VIEW"

	// specify processing for oracle table attr null、nullstring and ""
	OracleDatabaseTableColumnDefaultValueWithNULLSTRING = "NULLSTRING"
	OracleDatabaseTableColumnDefaultValueWithStringNull = ""
	OracleDatabaseTableColumnDefaultValueWithNULL       = "NULL"

	OracleDatabaseColumnDatatypeMatchRuleNotFound = "NOT FOUND"

	// MYSQL database check constraint support version > 8.0.15
	MYSQLDatabaseCheckConstraintSupportVersion = "8.0.15"
	MYSQLDatabaseVersionDelimiter              = "-"
	// MYSQL database expression index support version > 8.0.0
	MYSQLDatabaseExpressionIndexSupportVersion = "8.0.0"

	// struct migrate type
	DatabaseStructMigrateSqlSchemaCategory   = "SCHEMA"
	DatabaseStructMigrateSqlTableCategory    = "TABLE"
	DatabaseStructMigrateSqlSequenceCategory = "SEQUENCE"

	MYSQLDatabaseSequenceSupportVersion = "8.0"
	TIDBDatabaseSequenceSupportVersion  = "4.0"
)

// TiDB database integer primary key menu
var TiDBDatabaseIntegerPrimaryKeyMenu = []string{"TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"}

// Data migration, data verification, table structure default values, comments
// Character type data mapping rules
// 1. Used by the program to connect to the source database to read character type data and write it to the downstream database in the corresponding character set.
// 2. Used for data table default values or comments
const (
	CharsetUTF8MB4 = "UTF8MB4"
	CharsetGB18030 = "GB18030"
	CharsetBIG5    = "BIG5"
	CharsetGBK     = "GBK"
)

var MigrateDataSupportCharset = []string{CharsetUTF8MB4, CharsetGBK, CharsetBIG5, CharsetGB18030}

var MigrateOracleCharsetStringConvertMapping = map[string]string{
	ORACLECharsetAL32UTF8:     CharsetUTF8MB4,
	ORACLECharsetZHT16BIG5:    CharsetBIG5,
	ORACLECharsetZHS16GBK:     CharsetGBK,
	ORACLECharsetZHS32GB18030: CharsetGB18030,
}

var MigrateMySQLCompatibleCharsetStringConvertMapping = map[string]string{
	MYSQLCharsetUTF8MB4: CharsetUTF8MB4,
	MYSQLCharsetUTF8:    CharsetUTF8MB4,
	MYSQLCharsetBIG5:    CharsetBIG5,
	MYSQLCharsetGBK:     CharsetGBK,
	MYSQLCharsetGB18030: CharsetGB18030,
}

// string data support charset list
var (
	MYSQLCharsetUTF8MB4       = "UTF8MB4"
	MYSQLCharsetUTF8          = "UTF8"
	MYSQLCharsetBIG5          = "BIG5"
	MYSQLCharsetGBK           = "GBK"
	MYSQLCharsetGB18030       = "GB18030"
	ORACLECharsetAL32UTF8     = "AL32UTF8"
	ORACLECharsetZHT16BIG5    = "ZHT16BIG5"
	ORACLECharsetZHS16GBK     = "ZHS16GBK"
	ORACLECharsetZHS32GB18030 = "ZHS32GB18030"
)

// table structure migration and table structure verification character set and sorting rules
var MigrateTableStructureDatabaseCharsetMap = map[string]map[string]string{
	TaskFlowOracleToMySQL: {
		ORACLECharsetAL32UTF8:     MYSQLCharsetUTF8MB4,
		ORACLECharsetZHT16BIG5:    MYSQLCharsetBIG5,
		ORACLECharsetZHS16GBK:     MYSQLCharsetGBK,
		ORACLECharsetZHS32GB18030: MYSQLCharsetGB18030,
	},
	// TiDB table structure and field attributes use UTF8MB4 character set, which is suitable for o2t and t2o in check, compare and reverse modes.
	TaskFlowOracleToTiDB: {
		ORACLECharsetAL32UTF8:     MYSQLCharsetUTF8MB4,
		ORACLECharsetZHT16BIG5:    MYSQLCharsetUTF8MB4,
		ORACLECharsetZHS16GBK:     MYSQLCharsetUTF8MB4,
		ORACLECharsetZHS32GB18030: MYSQLCharsetUTF8MB4,
	},
	TaskFlowMySQLToOracle: {
		MYSQLCharsetUTF8MB4: ORACLECharsetAL32UTF8,
		MYSQLCharsetUTF8:    ORACLECharsetAL32UTF8,
		MYSQLCharsetBIG5:    ORACLECharsetZHT16BIG5,
		MYSQLCharsetGBK:     ORACLECharsetZHS16GBK,
		MYSQLCharsetGB18030: ORACLECharsetZHS32GB18030,
	},
	TaskFlowTiDBToOracle: {
		MYSQLCharsetUTF8MB4: ORACLECharsetAL32UTF8,
		MYSQLCharsetUTF8:    ORACLECharsetAL32UTF8,
		MYSQLCharsetGBK:     ORACLECharsetZHS16GBK,
	},
}

/*
	MySQL 8.0
	utf8mb4_0900_as_cs accent-sensitive, case-sensitive sorting rules
	utf8mb4_0900_ai_ci accent-insensitive and case-insensitive collation
	utf8mb4_0900_as_ci accent-sensitive, case-insensitive sorting rules
*/
// Oracle field Collation mapping
var MigrateTableStructureDatabaseCollationMap = map[string]map[string]map[string]string{
	TaskFlowOracleToMySQL: {
		/*
			ORACLE 12.2 and the above versions
		*/
		// Not case-sensitive, but accent sensitive
		// MySQL 8.0 ONlY
		"BINARY_CI": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AS_CI",
			MYSQLCharsetUTF8:    "UTF8_0900_AS_CI",
			MYSQLCharsetBIG5:    "BIG5_CHINESE_CI",    // currently there is no such sorting rule, use BIG5_CHINESE_CI instead
			MYSQLCharsetGBK:     "GBK_CHINESE_CI",     // currently there is no such sorting rule, use GBK_CHINESE_CI instead
			MYSQLCharsetGB18030: "GB18030_CHINESE_CI", // currently there is no such sorting rule, use GB18030_CHINESE_CI instead, GB18030_UNICODE_520_CI case-sensitive, but not accent sensitive
		},
		// Not case and accent sensitive
		"BINARY_AI": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_GENERAL_CI",
			MYSQLCharsetUTF8:    "UTF8_GENERAL_CI",
			MYSQLCharsetBIG5:    "BIG5_CHINESE_CI",
			MYSQLCharsetGBK:     "GBK_CHINESE_CI",
			MYSQLCharsetGB18030: "GB18030_CHINESE_CI",
		},
		// Case and accent sensitive, this rule is the ORACLE default if no extension is used
		"BINARY_CS": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_BIN",
			MYSQLCharsetUTF8:    "UTF8_BIN",
			MYSQLCharsetBIG5:    "BIG5_BIN",
			MYSQLCharsetGBK:     "GBK_BIN",
			MYSQLCharsetGB18030: "GB18030_BIN",
		},
		/*
			ORACLE 12.2 and the below versions
		*/
		// Case and accent sensitive
		"BINARY": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_BIN",
			MYSQLCharsetUTF8:    "UTF8_BIN",
			MYSQLCharsetBIG5:    "BIG5_BIN",
			MYSQLCharsetGBK:     "GBK_BIN",
			MYSQLCharsetGB18030: "GB18030_BIN",
		},
	},
	/*
		Character sets are uniformly converted to utf8mb4
	*/
	TaskFlowOracleToTiDB: {
		/*
			ORACLE 12.2 and the above versions
		*/
		// Not case-sensitive, but accent sensitive
		// MySQL 8.0 ONlY
		"BINARY_CI": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AS_CI",
			MYSQLCharsetUTF8:    "UTF8_0900_AS_CI",
			MYSQLCharsetBIG5:    "BIG5_CHINESE_CI",    // currently there is no such sorting rule, use BIG5_CHINESE_CI instead
			MYSQLCharsetGBK:     "GBK_CHINESE_CI",     // currently there is no such sorting rule, use GBK_CHINESE_CI instead
			MYSQLCharsetGB18030: "GB18030_CHINESE_CI", // currently there is no such sorting rule, use GB18030_CHINESE_CI instead, GB18030_UNICODE_520_CI case-sensitive, but not accent sensitive
		},
		// Not case and accent sensitive
		"BINARY_AI": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_GENERAL_CI",
			MYSQLCharsetUTF8:    "UTF8_GENERAL_CI",
			MYSQLCharsetBIG5:    "BIG5_CHINESE_CI",
			MYSQLCharsetGBK:     "GBK_CHINESE_CI",
			MYSQLCharsetGB18030: "GB18030_CHINESE_CI",
		},
		// Case and accent sensitive, this rule is the ORACLE default if no extension is used
		"BINARY_CS": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_BIN",
			MYSQLCharsetUTF8:    "UTF8_BIN",
			MYSQLCharsetBIG5:    "BIG5_BIN",
			MYSQLCharsetGBK:     "GBK_BIN",
			MYSQLCharsetGB18030: "GB18030_BIN",
		},
		/*
			ORACLE 12.2 and the below versions
		*/
		// Case and accent sensitive
		"BINARY": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_BIN",
			MYSQLCharsetUTF8:    "UTF8_BIN",
			MYSQLCharsetBIG5:    "BIG5_BIN",
			MYSQLCharsetGBK:     "GBK_BIN",
			MYSQLCharsetGB18030: "GB18030_BIN",
		},
	},
	TaskFlowMySQLToOracle: {
		/*
			ORACLE 12.2 and above
		*/
		// Not case-sensitive, but accent sensitive
		// MySQL 8.0 ONLY
		// If there is no such sorting rule, use BIG5_CHINESE_CI instead
		// If there is no such sorting rule, use GBK_CHINESE_CI instead
		// If there is no such sorting rule, use GB18030_CHINESE_CI instead. gb18030_unicode_520_ci is case-sensitive, but not accent-sensitive.
		"UTF8MB4_0900_AS_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_CI",
			ORACLECharsetZHT16BIG5:    "BINARY_CI",
			ORACLECharsetZHS16GBK:     "BINARY_CI",
			ORACLECharsetZHS32GB18030: "BINARY_CI",
		},
		// Not case and accent sensitive
		"UTF8MB4_GENERAL_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI",
			ORACLECharsetZHS16GBK:     "BINARY_AI",
			ORACLECharsetZHS32GB18030: "BINARY_AI",
		},
		"UTF8_GENERAL_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI",
			ORACLECharsetZHS16GBK:     "BINARY_AI",
			ORACLECharsetZHS32GB18030: "BINARY_AI",
		},
		"BIG5_CHINESE_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS16GBK:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS32GB18030: "BINARY_AI/BINARY_CI",
		},
		"GBK_CHINESE_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS16GBK:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS32GB18030: "BINARY_AI/BINARY_CI",
		},
		"GB18030_CHINESE_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS16GBK:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS32GB18030: "BINARY_AI/BINARY_CI",
		},
		// Sensitive to case and accent. If no extension is used, this rule is ORACLE default -> BINARY_CS AND ORACLE 12.2 or below -> BINARY
		// Case and accent sensitive
		"UTF8MB4_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
		"UTF8_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
		"BIG5_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
		"GBK_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
		"GB18030_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
	},
	TaskFlowTiDBToOracle: {
		/*
			ORACLE 12.2 and above
		*/
		// Not case-sensitive, but accent sensitive
		//	MySQL 8.0 ONLY
		// If there is no such sorting rule, use BIG5_CHINESE_CI instead
		// If there is no such sorting rule, use GBK_CHINESE_CI instead
		// If there is no such sorting rule, use GB18030_CHINESE_CI instead. gb18030_unicode_520_ci is case-sensitive, but not accent-sensitive.
		"UTF8MB4_0900_AS_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_CI",
			ORACLECharsetZHT16BIG5:    "BINARY_CI",
			ORACLECharsetZHS16GBK:     "BINARY_CI",
			ORACLECharsetZHS32GB18030: "BINARY_CI",
		},
		// Not case and accent sensitive
		"UTF8MB4_GENERAL_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI",
			ORACLECharsetZHS16GBK:     "BINARY_AI",
			ORACLECharsetZHS32GB18030: "BINARY_AI",
		},
		"UTF8_GENERAL_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI",
			ORACLECharsetZHS16GBK:     "BINARY_AI",
			ORACLECharsetZHS32GB18030: "BINARY_AI",
		},
		"GBK_CHINESE_CI": {
			ORACLECharsetAL32UTF8:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHT16BIG5:    "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS16GBK:     "BINARY_AI/BINARY_CI",
			ORACLECharsetZHS32GB18030: "BINARY_AI/BINARY_CI",
		},
		// Sensitive to case and accent. If no extension is used, this rule is the ORACLE default value -> BINARY_CS, and ORACLE 12.2 or below -> BINARY
		// Case and accent sensitive
		"UTF8MB4_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
		"UTF8_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
		"GBK_BIN": {
			ORACLECharsetAL32UTF8:     "BINARY/BINARY_CS",
			ORACLECharsetZHT16BIG5:    "BINARY/BINARY_CS",
			ORACLECharsetZHS16GBK:     "BINARY/BINARY_CS",
			ORACLECharsetZHS32GB18030: "BINARY/BINARY_CS",
		},
	},
}

const (
	ParamValueRuleCaseFieldNameOrigin = "0"
	ParamValueRuleCaseFieldNameLower  = "1"
	ParamValueRuleCaseFieldNameUpper  = "2"
)

// migrate parameters
// struct migrate parameters
const (
	ParamNameStructMigrateMigrateThread      = "migrateThread"
	ParamNameStructMigrateCreateIfNotExist   = "createIfNotExist"
	ParamNameStructMigrateEnableDirectCreate = "enableDirectCreate"

	// ParamValueStructMigrateCaseFieldRuleOrigin case-field-name params value
	// - 0 represent keeping origin
	// - 1 represent keeping lower
	// - 2 represent keeping upper
	ParamValueStructMigrateCaseFieldRuleOrigin = "0"
	ParamValueStructMigrateCaseFieldRuleLower  = "1"
	ParamValueStructMigrateCaseFieldRuleUpper  = "2"
)

// statement migrate parameters
const (
	ParamNameStmtMigrateTableThread          = "tableThread"
	ParamNameStmtMigrateBatchSize            = "batchSize"
	ParamNameStmtMigrateChunkSize            = "chunkSize"
	ParamNameStmtMigrateSqlThreadS           = "sqlThreadS"
	ParamNameStmtMigrateSqlHintS             = "sqlHintS"
	ParamNameStmtMigrateSqlThreadT           = "sqlThreadT"
	ParamNameStmtMigrateSqlHintT             = "sqlHintT"
	ParamNameStmtMigrateCallTimeout          = "callTimeout"
	ParamNameStmtMigrateEnableCheckpoint     = "enableCheckpoint"
	ParamNameStmtMigrateEnableConsistentRead = "enableConsistentRead"

	// ParamValueDataMigrateCaseFieldRuleOrigin case-field-name params value
	// - 0 represent keeping origin
	// - 1 represent keeping lower
	// - 2 represent keeping upper
	ParamValueDataMigrateCaseFieldRuleOrigin = "0"
	ParamValueDataMigrateCaseFieldRuleLower  = "1"
	ParamValueDataMigrateCaseFieldRuleUpper  = "2"
)

// csv migrate parameters
const (
	ParamNameCsvMigrateTableThread          = "tableThread"
	ParamNameCsvMigrateBatchSize            = "batchSize"
	ParamNameCsvMigrateDiskUsageFactor      = "diskUsageFactor"
	ParamNameCsvMigrateHeader               = "header"
	ParamNameCsvMigrateSeparator            = "separator"
	ParamNameCsvMigrateTerminator           = "terminator"
	ParamNameCsvMigrateDataCharsetT         = "dataCharsetT"
	ParamNameCsvMigrateDelimiter            = "delimiter"
	ParamNameCsvMigrateNullValue            = "nullValue"
	ParamNameCsvMigrateEscapeBackslash      = "escapeBackslash"
	ParamNameCsvMigrateChunkSize            = "chunkSize"
	ParamNameCsvMigrateOutputDir            = "outputDir"
	ParamNameCsvMigrateSqlThreadS           = "sqlThreadS"
	ParamNameCsvMigrateSqlHintS             = "sqlHintS"
	ParamNameCsvMigrateCallTimeout          = "callTimeout"
	ParamNameCsvMigrateEnableCheckpoint     = "enableCheckpoint"
	ParamNameCsvMigrateEnableConsistentRead = "enableConsistentRead"
)

// sql migrate parameters
const (
	ParamNameSqlMigrateBatchSize            = "batchSize"
	ParamNameSqlMigrateSqlThreadS           = "sqlThreadS"
	ParamNameSqlMigrateSqlThreadT           = "sqlThreadT"
	ParamNameSqlMigrateSqlHintT             = "sqlHintT"
	ParamNameSqlMigrateCallTimeout          = "callTimeout"
	ParamNameSqlMigrateEnableConsistentRead = "enableConsistentRead"

	// ParamValueSqlMigrateCaseFieldRuleOrigin case-field-name params value
	// - 0 represent keeping origin
	// - 1 represent keeping lower
	// - 2 represent keeping upper
	ParamValueSqlMigrateCaseFieldRuleOrigin = "0"
	ParamValueSqlMigrateCaseFieldRuleLower  = "1"
	ParamValueSqlMigrateCaseFieldRuleUpper  = "2"
)

// data compare parameters
const (
	ParamNameDataCompareTableThread          = "tableThread"
	ParamNameDataCompareBatchSize            = "batchSize"
	ParamNameDataCompareSqlThread            = "sqlThread"
	ParamNameDataCompareSqlHintS             = "sqlHintS"
	ParamNameDataCompareSqlHintT             = "sqlHintT"
	ParamNameDataCompareCallTimeout          = "callTimeout"
	ParamNameDataCompareEnableCheckpoint     = "enableCheckpoint"
	ParamNameDataCompareEnableConsistentRead = "enableConsistentRead"
	ParamNameDataCompareOnlyCompareRow       = "onlyCompareRow"

	// ParamValueDataCompareCaseFieldRuleOrigin case-field-name params value
	// - 0 represent keeping origin
	// - 1 represent keeping lower
	// - 2 represent keeping upper
	ParamValueDataCompareCaseFieldRuleOrigin = "0"
	ParamValueDataCompareCaseFieldRuleLower  = "1"
	ParamValueDataCompareCaseFieldRuleUpper  = "2"
)

// assess migrate parameters
const (
	ParamNameAssessMigrateCaseFieldRuleS = "caseFieldRuleS"
	ParamNameAssessMigrateSchemaNameS    = "schemaNameS"

	// ParamValueAssessMigrateCaseFieldRuleOrigin case-field-name params value
	// - 0 represent keeping origin
	// - 1 represent keeping lower
	// - 2 represent keeping upper
	ParamValueAssessMigrateCaseFieldRuleOrigin = "0"
	ParamValueAssessMigrateCaseFieldRuleLower  = "1"
	ParamValueAssessMigrateCaseFieldRuleUpper  = "2"
)
