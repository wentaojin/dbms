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
	ParamNameStructMigrateEnableCheckpoint   = "enableCheckpoint"
	ParamNameStructMigrateCallTimeout        = "callTimeout"

	// ParamValueStructMigrateCaseFieldRuleOrigin case-field-name params value
	// - 0 represent keeping origin
	// - 1 represent keeping lower
	// - 2 represent keeping upper
	ParamValueStructMigrateCaseFieldRuleOrigin = "0"
	ParamValueStructMigrateCaseFieldRuleLower  = "1"
	ParamValueStructMigrateCaseFieldRuleUpper  = "2"
)

// struct compare parameters
const (
	ParamNameStructCompareCompareThread     = "compareThread"
	ParamNameStructCompareEnableCheckpoint  = "enableCheckpoint"
	ParamNameStructCompareCallTimeout       = "callTimeout"
	ParamNameStructCompareIgnoreCaseCompare = "ignoreCaseCompare"

	// ParamValueStructCompareCaseFieldRuleOrigin case-field-name params value
	// - 0 represent keeping origin
	// - 1 represent keeping lower
	// - 2 represent keeping upper
	ParamValueStructCompareCaseFieldRuleOrigin = "0"
	ParamValueStructCompareCaseFieldRuleLower  = "1"
	ParamValueStructCompareCaseFieldRuleUpper  = "2"
)

// statement migrate parameters
const (
	ParamNameStmtMigrateTableThread          = "tableThread"
	ParamNameStmtMigrateWriteThread          = "writeThread"
	ParamNameStmtMigrateBatchSize            = "batchSize"
	ParamNameStmtMigrateChunkSize            = "chunkSize"
	ParamNameStmtMigrateSqlThreadS           = "sqlThreadS"
	ParamNameStmtMigrateSqlHintS             = "sqlHintS"
	ParamNameStmtMigrateSqlThreadT           = "sqlThreadT"
	ParamNameStmtMigrateSqlHintT             = "sqlHintT"
	ParamNameStmtMigrateCallTimeout          = "callTimeout"
	ParamNameStmtMigrateEnableCheckpoint     = "enableCheckpoint"
	ParamNameStmtMigrateEnableConsistentRead = "enableConsistentRead"
	ParamNameStmtMigrateEnableSafeMode       = "enableSafeMode"
	ParamNameStmtMigrateEnablePrepareStmt    = "enablePrepareStmt"
	ParamNameStmtMigrateGarbledCharReplace   = "garbledCharReplace"

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
	ParamNameCsvMigrateWriteThread          = "writeThread"
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
	ParamNameCsvMigrateEnableImportFeature  = "enableImportFeature"
	ParamNameCsvMigrateImportParams         = "csvImportParams"
	ParamNameCsvMigrateGarbledCharReplace   = "garbledCharReplace"
)

// sql migrate parameters
const (
	ParamNameSqlMigrateBatchSize            = "batchSize"
	ParamNameSqlMigrateWriteThread          = "writeThread"
	ParamNameSqlMigrateSqlThreadS           = "sqlThreadS"
	ParamNameSqlMigrateSqlThreadT           = "sqlThreadT"
	ParamNameSqlMigrateSqlHintT             = "sqlHintT"
	ParamNameSqlMigrateCallTimeout          = "callTimeout"
	ParamNameSqlMigrateEnableConsistentRead = "enableConsistentRead"
	ParamNameSqlMigrateEnableSafeMode       = "enableSafeMode"
	ParamNameSqlMigrateEnableCheckpoint     = "enableCheckpoint"
	ParamNameSqlMigrateEnablePrepareStmt    = "enablePrepareStmt"
	ParamNameSqlMigrateGarbledCharReplace   = "garbledCharReplace"
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
	ParamNameDataCompareTableThread            = "tableThread"
	ParamNameDataCompareBatchSize              = "batchSize"
	ParamNameDataCompareSqlThread              = "sqlThread"
	ParamNameDataCompareWriteThread            = "writeThread"
	ParamNameDataCompareSqlHintS               = "sqlHintS"
	ParamNameDataCompareSqlHintT               = "sqlHintT"
	ParamNameDataCompareCallTimeout            = "callTimeout"
	ParamNameDataCompareEnableCheckpoint       = "enableCheckpoint"
	ParamNameDataCompareEnableConsistentRead   = "enableConsistentRead"
	ParamNameDataCompareOnlyCompareRow         = "onlyCompareRow"
	ParamNameDataCompareConsistentReadPointS   = "consistentReadPointS"
	ParamNameDataCompareConsistentReadPointT   = "consistentReadPointT"
	ParamNameDataCompareChunkSize              = "chunkSize"
	ParamNameDataCompareIgnoreConditionFields  = "ignoreConditionFields"
	ParamNameDataCompareRepairStmtFlow         = "repairStmtFlow"
	ParamNameDataCompareEnableCollationSetting = "enableCollationSetting"
	ParamNameDataCompareDisableMd5Checksum     = "disableMd5Checksum"
	ParamNameDataCompareSeparator              = "separator"

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
	ParamNameAssessMigrateCallTimeout    = "callTimeout"

	// ParamValueAssessMigrateCaseFieldRuleOrigin case-field-name params value
	// - 0 represent keeping origin
	// - 1 represent keeping lower
	// - 2 represent keeping upper
	ParamValueAssessMigrateCaseFieldRuleOrigin = "0"
	ParamValueAssessMigrateCaseFieldRuleLower  = "1"
	ParamValueAssessMigrateCaseFieldRuleUpper  = "2"
)

// data scan parameters
const (
	ParamNameDataScanTableThread          = "tableThread"
	ParamNameDataScanWriteThread          = "writeThread"
	ParamNameDataScanBatchSize            = "batchSize"
	ParamNameDataScanChunkSize            = "chunkSize"
	ParamNameDataScanSqlThreadS           = "sqlThreadS"
	ParamNameDataScanSqlHintS             = "sqlHintS"
	ParamNameDataScanCallTimeout          = "callTimeout"
	ParamNameDataScanTableSamplerateS     = "tableSamplerateS"
	ParamNameDataScanEnableCheckpoint     = "enableCheckpoint"
	ParamNameDataScanEnableConsistentRead = "enableConsistentRead"

	// ParamValueDataScanCaseFieldRuleOrigin case-field-name params value
	// - 0 represent keeping origin
	// - 1 represent keeping lower
	// - 2 represent keeping upper
	ParamValueDataScanCaseFieldRuleOrigin = "0"
	ParamValueDataScanCaseFieldRuleLower  = "1"
	ParamValueDataScanCaseFieldRuleUpper  = "2"
)

// cdc consume parameters
const (
	ParamNameCdcConsumeTableThread           = "tableThread"
	ParamNameCdcConsumeMessageCompression    = "messageCompression"
	ParamNameCdcConsumeIdleResolvedThreshold = "idleResolvedThreshold"
	ParamNameCdcConsumeCallTimeout           = "callTimeout"
	ParamNameCdcConsumeServerAddress         = "serverAddress"
	ParamNameCdcConsumeSubscribeTopic        = "subscribeTopic"
	ParamNameCdcConsumeEnableCheckpoint      = "enableCheckpoint"
)
