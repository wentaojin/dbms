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

import "github.com/wentaojin/dbms/utils/structure"

func (d *Database) GetDatabaseTableConstraintIndexColumn(schemaNameS, tableNameS string) (map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableStatisticsBucket(schemeNameS, tableNameS string, consColumns map[string]string) (map[string][]structure.Bucket, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableStatisticsHistogram(schemeNameS, tableNameS string, consColumns map[string]string) (map[string]structure.Histogram, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableHighestSelectivityIndex(schemaNameS, tableNameS string, compareCondField string, ignoreCondFields []string) (*structure.HighestBucket, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnProperties(schemaNameS, tableNameS string, columnNameSli []string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableRandomValues(schemaNameS, tableNameS string, columns []string, conditions string, condArgs []interface{}, limit int, collations []string) ([][]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCompareData(querySQL string, callTimeout int, dbCharsetS, dbCharsetT string, queryArgs []interface{}) ([]string, uint32, map[string]int64, error) {
	//TODO implement me
	panic("implement me")
}
