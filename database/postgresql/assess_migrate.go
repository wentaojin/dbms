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
package postgresql

func (d *Database) GetDatabaseSoftVersion() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabasePlatformName() (string, string, string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseGlobalName() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseParameters() (string, string, string, string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GeDatabaseInstance() ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSize() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaNameALL() ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaNameSingle(schemaNameS string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseServerNumCPU() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseServerMemorySize() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSessionMaxActiveCount() ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTableIndexOverview(schemaNameS []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTableRowsTOP(schemaName []string, top int) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaCodeObject(schemaName []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaPartitionTableType(schemaName []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTableAvgRowLengthTOP(schemaName []string, top int) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaSynonymObject(schemaName []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaMaterializedViewObject(schemaName []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaPartitionTableCountsOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTableAvgRowLengthOverLimitMB(schemaName []string, limitMB int) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTableIndexLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTableColumnCountsOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTableIndexCountsOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseUsernameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTableNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTableColumnNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTableIndexNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTableViewNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTableSequenceNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTableTypeCounts(schemaName []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaColumnDataDefaultCounts(schemaName []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaViewTypeCounts(schemaName []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaObjectTypeCounts(schemaName []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaPartitionTypeCounts(schemaName []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaSubPartitionTypeCounts(schemaName []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTemporaryTableTypeCounts(schemaName []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaConstraintTypeCounts(schemaName []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaIndexTypeCounts(schemaName []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaColumnTypeCounts(schemaName []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaTableSpecialDatatype(schemaName []string, specialDatatype string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}
