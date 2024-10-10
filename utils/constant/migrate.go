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
	// struct migrate type
	DatabaseMigrateSequenceCompatible    = "Y"
	DatabaseMigrateSequenceNotCompatible = "N"

	DatabaseMigrateTableStructDisabledMaterializedView = "MATERIALIZED VIEW"

	// struct compare
	StructCompareColumnsStructureJSONFormat   = "COLUMN"
	StructCompareIndexStructureJSONFormat     = "INDEX"
	StructComparePrimaryStructureJSONFormat   = "PK"
	StructCompareUniqueStructureJSONFormat    = "UK"
	StructCompareForeignStructureJSONFormat   = "FK"
	StructCompareCheckStructureJSONFormat     = "CK"
	StructComparePartitionStructureJSONFormat = "PARTITION"
)

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

var MigratePostgreSQLCompatibleCharsetStringConvertMapping = map[string]string{
	PostgreSQLCharsetEUC_TW: CharsetUTF8MB4,
	PostgreSQLCharsetUTF8:   CharsetUTF8MB4,
	PostgreSQLCharsetEUC_CN: CharsetUTF8MB4,
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
	// the BIG5、GBK and GB18030 charsets aren't support server charset encoding, but can use client charset encoding
	// detail reference: http://www.postgres.cn/docs/9.5/multibyte.html
	// EUC_TW -> BIG5
	// EUC_CN -> EUC_CN、UTF8
	PostgreSQLCharsetEUC_TW = "EUC_TW"
	PostgreSQLCharsetUTF8   = "UTF8"
	PostgreSQLCharsetEUC_CN = "EUC_CN"
)

// MigrateTableStructureDatabaseCharsetMap table structure migration and table structure verification character set and sorting rules
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
	// MYSQL/TiDB table structure and field attributes use UTF8MB4 character set, which is suitable for p2t and t2p in check, compare and reverse modes.
	TaskFlowPostgresToTiDB: {
		PostgreSQLCharsetEUC_TW: MYSQLCharsetUTF8MB4,
		PostgreSQLCharsetEUC_CN: MYSQLCharsetUTF8MB4,
		PostgreSQLCharsetUTF8:   MYSQLCharsetUTF8MB4,
	},
	TaskFlowPostgresToMySQL: {
		PostgreSQLCharsetEUC_TW: MYSQLCharsetUTF8MB4,
		PostgreSQLCharsetEUC_CN: MYSQLCharsetUTF8MB4,
		PostgreSQLCharsetUTF8:   MYSQLCharsetUTF8MB4,
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
	TaskFlowPostgresToTiDB: {
		"C": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_BIN",
		},
		"POSIX": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_BIN",
		},
		"ZH_TW": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AI_CI",
		},
		"ZH_TW.UTF8": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AI_CI",
		},
		"ZH_CN": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AI_CI",
		},
		"ZH_CN.UTF8": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AI_CI",
		},
		"EN_US": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AI_CI",
		},
		"EN_US.UTF8": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AI_CI",
		},
	},
	TaskFlowPostgresToMySQL: {
		"C": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_BIN",
		},
		"POSIX": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_BIN",
		},
		"ZH_TW": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AI_CI",
		},
		"ZH_TW.UTF8": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AI_CI",
		},
		"ZH_CN": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AI_CI",
		},
		"ZH_CN.UTF8": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AI_CI",
		},
		"EN_US": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AI_CI",
		},
		"EN_US.UTF8": {
			MYSQLCharsetUTF8MB4: "UTF8MB4_0900_AI_CI",
		},
	},
}
