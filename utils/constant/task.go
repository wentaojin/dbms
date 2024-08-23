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

// TaskMode represents a certain type of task
const (
	TaskModeAssessMigrate   = "ASSESS_MIGRATE"
	TaskModeStructMigrate   = "STRUCT_MIGRATE"
	TaskModeSequenceMigrate = "SEQUENCE_MIGRATE"
	TaskModeStmtMigrate     = "STMT_MIGRATE"
	TaskModeSqlMigrate      = "SQL_MIGRATE"
	TaskModeIncrMigrate     = "INCR_MIGRATE"
	TaskModeCSVMigrate      = "CSV_MIGRATE"

	TaskModeDataCompare   = "DATA_COMPARE"
	TaskModeStructCompare = "STRUCT_COMPARE"
	TaskModeDataScan      = "DATA_SCAN"
)

// Task represents a certain type of task run method
const (
	TaskModeDataCompareMethodTableCRC32 = "TABLE_CRC32_COMPARE"
	TaskModeDataCompareMethodTableCount = "TABLE_COUNT_COMPARE"
	TaskModeDataCompareMethodTableMD5   = "TABLE_MD5_COMPARE"

	TaskModeDataMigrateMethodTableChunk = "TABLE_CHUNK_SPLIT"
	TaskModeCsvMigrateMethodTableChunk  = "TABLE_CHUNK_SPLIT"
)

const (
	TaskDatabaseStatusWaiting  = "WAITING"
	TaskDatabaseStatusRunning  = "RUNNING"
	TaskDatabaseStatusStopped  = "STOPPED"
	TaskDatabaseStatusFailed   = "FAILED"
	TaskDatabaseStatusSuccess  = "SUCCESS"
	TaskDatabaseStatusEqual    = "EQUAL"
	TaskDatabaseStatusNotEqual = "NOT_EQUAL"
)

const (
	TaskFlowOracleToMySQL  = "ORACLE@MYSQL"
	TaskFlowOracleToTiDB   = "ORACLE@TIDB"
	TaskFlowMySQLToOracle  = "MYSQL@ORACLE"
	TaskFlowTiDBToOracle   = "TIDB@ORACLE"
	TaskFlowPostgresToTiDB = "POSTGRES@TIDB"
)

const (
	TaskOperationStart          = "START"
	TaskOperationStop           = "STOP"
	TaskOperationCrontabSubmit  = "CRONTAB SUBMIT"
	TaskOperationCrontabClear   = "CRONTAB CLEAR"
	TaskOperationCrontabDisplay = "CRONTAB DISPLAY"
	TaskOperationDelete         = "DELETE"
	TaskOperationStatus         = "STATUS"
)

const (
	TaskInitStatusFinished    = "Y"
	TaskInitStatusNotFinished = "N"

	TaskMigrateStatusFinished    = "Y"
	TaskMigrateStatusNotFinished = "N"
	TaskMigrateStatusSkipped     = "S"

	TaskScanStatusFinished    = "Y"
	TaskScanStatusNotFinished = "N"
	TaskScanStatusSkipped     = "S"

	TaskCompareStatusFinished    = "Y"
	TaskCompareStatusNotFinished = "N"
	TaskCompareStatusSkipped     = "S"

	TaskAssessStatusFinished    = "Y"
	TaskAssessStatusNotFinished = "N"
)

// ServiceDatabaseSqlQueryCallTimeout represent package service database request sql query timeout, uint: seconds
const ServiceDatabaseSqlQueryCallTimeout = 300
