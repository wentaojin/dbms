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
package database

// IDatabaseAssessMigrate used for database assess migrate
type IDatabaseAssessMigrate interface {
	GetDatabaseSoftVersion() (string, error)
	GetDatabasePlatformName() (string, string, string, error)
	GetDatabaseGlobalName() (string, error)
	GetDatabaseParameters() (string, string, string, string, error)
	GetDatabaseInstance() ([]map[string]string, error)
	GetDatabaseSize() (string, error)
	GetDatabaseSchemaNameALL() ([]string, error)
	GetDatabaseSchemaNameSingle(schemaNameS string) (string, error)
	GetDatabaseServerNumCPU() (string, error)
	GetDatabaseServerMemorySize() (string, error)
	GetDatabaseSessionMaxActiveCount() ([]map[string]string, error)
	GetDatabaseSchemaTableIndexOverview(schemaNameS []string) ([]map[string]string, error)
	GetDatabaseSchemaTableRowsTOP(schemaName []string, top int) ([]map[string]string, error)
	GetDatabaseSchemaCodeObject(schemaName []string) ([]map[string]string, error)
	GetDatabaseSchemaPartitionTableType(schemaName []string) ([]map[string]string, error)
	GetDatabaseSchemaTableAvgRowLengthTOP(schemaName []string, top int) ([]map[string]string, error)
	GetDatabaseSchemaSynonymObject(schemaName []string) ([]map[string]string, error)
	GetDatabaseSchemaMaterializedViewObject(schemaName []string) ([]map[string]string, error)
	GetDatabaseSchemaPartitionTableCountsOverLimit(schemaName []string, limit int) ([]map[string]string, error)
	GetDatabaseSchemaTableAvgRowLengthOverLimitMB(schemaName []string, limitMB int) ([]map[string]string, error)
	GetDatabaseSchemaTableIndexLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error)
	GetDatabaseSchemaTableColumnCountsOverLimit(schemaName []string, limit int) ([]map[string]string, error)
	GetDatabaseSchemaTableIndexCountsOverLimit(schemaName []string, limit int) ([]map[string]string, error)
	GetDatabaseUsernameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error)
	GetDatabaseSchemaTableNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error)
	GetDatabaseSchemaTableColumnNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error)
	GetDatabaseSchemaTableIndexNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error)
	GetDatabaseSchemaTableViewNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error)
	GetDatabaseSchemaTableSequenceNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error)
	GetDatabaseSchemaTableTypeCounts(schemaName []string) ([]map[string]string, error)
	GetDatabaseSchemaColumnDataDefaultCounts(schemaName []string) ([]map[string]string, error)
	GetDatabaseSchemaViewTypeCounts(schemaName []string) ([]map[string]string, error)
	GetDatabaseSchemaObjectTypeCounts(schemaName []string) ([]map[string]string, error)
	GetDatabaseSchemaPartitionTypeCounts(schemaName []string) ([]map[string]string, error)
	GetDatabaseSchemaSubPartitionTypeCounts(schemaName []string) ([]map[string]string, error)
	GetDatabaseSchemaTemporaryTableTypeCounts(schemaName []string) ([]map[string]string, error)
	GetDatabaseSchemaConstraintTypeCounts(schemaName []string) ([]map[string]string, error)
	GetDatabaseSchemaIndexTypeCounts(schemaName []string) ([]map[string]string, error)
	GetDatabaseSchemaColumnTypeCounts(schemaName []string) ([]map[string]string, error)
	GetDatabaseSchemaTableSpecialDatatype(schemaName []string, specialDatatype string) ([]map[string]string, error)
}
