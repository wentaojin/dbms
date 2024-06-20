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

func (d *Database) GetDatabaseSchema() ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTable(schemaName string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseCharset() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseCollation() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseVersion() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabasePartitionTable(schemaName string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTemporaryTable(schemaName string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseClusteredTable(schemaName string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseMaterializedView(schemaName string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableType(schemaName string) (map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnInfo(schemaName string, tableName string, collation bool) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTablePrimaryKey(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableUniqueKey(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableForeignKey(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCheckKey(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableNormalIndex(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableUniqueIndex(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableComment(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnComment(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCharset(schemaName string, tableName string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCollation(schemaName, tableName string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaCollation(schemaName string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableOriginStruct(schemaName, tableName, tableType string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSequence(schemaName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseRole() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseConsistentPos() (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnNameTableDimensions(schemaName, tableName string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnNameSqlDimensions(sqlStr string) ([]string, map[string]string, map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableRows(schemaName, tableName string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableSize(schemaName, tableName string) (float64, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableChunkTask(taskName, schemaName, tableName string, chunkSize uint64, callTimeout uint64) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableChunkData(querySQL string, batchSize, callTimeout int, dbCharsetS, dbCharsetT, columnDetailO string, dataChan chan []interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCsvData(querySQL string, callTimeout int, taskFlow, dbCharsetS, dbCharsetT, columnDetailO string, escapeBackslash bool, nullValue, separator, delimiter string, dataChan chan []string) error {
	//TODO implement me
	panic("implement me")
}

func (d *Database) FindDatabaseTableBestColumn(schemaNameS, tableNameS, columnNameS string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnAttribute(schemaNameS, tableNameS, columnNameS string, collationS bool) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnBucket(schemaNameS, tableNameS string, columnNameS, datatypeS string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCompareData(querySQL string, callTimeout int, dbCharsetS, dbCharsetT string) ([]string, uint32, map[string]int64, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTablePartitionExpress(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}
