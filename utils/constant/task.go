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
	TaskModeObjectAssess  = "OBJECT_ASSESS"
	TaskModeStructMigrate = "STRUCT_MIGRATE"
	TaskModeDATAMigrate   = "DATA_MIGRATE"
	TaskModeCSVMigrate    = "CSV_MIGRATE"

	TaskModeDataCompare   = "DATA_COMPARE"
	TaskModeStructCompare = "STRUCT_COMPARE"
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
	TaskDatabaseStatusWaiting = "WAITING"
	TaskDatabaseStatusRunning = "RUNNING"
	TaskDatabaseStatusKilled  = "KILLED"
	TaskDatabaseStatusFailed  = "FAILED"
	TaskDatabaseStatusSuccess = "SUCCESS"
)

const (
	TaskFlowOracleToMySQL = "ORACLE@MYSQL"
	TaskFlowOracleToTiDB  = "ORACLE@TIDB"
	TaskFlowMySQLToOracle = "MYSQL@ORACLE"
	TaskFlowTiDBToOracle  = "TIDB@ORACLE"
)
