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
package mysql

import "fmt"

func (d *Database) GetDatabaseTablePartitionExpress(schemaName string, tableName string) ([]map[string]string, error) {
	sqlStr := fmt.Sprintf("SELECT TRIM('`' FROM PARTITION_EXPRESSION) PARTITION_EXPRESSION,PARTITION_METHOD,TRIM('`' FROM SUBPARTITION_EXPRESSION) SUBPARTITION_EXPRESSION,SUBPARTITION_METHOD FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", schemaName, tableName)
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return nil, err
	}
	var result []map[string]string
	for _, r := range res {
		if r["PARTITION_EXPRESSION"] == "NULLABLE" && r["PARTITION_METHOD"] == "NULLABLE" && r["SUBPARTITION_EXPRESSION"] == "NULLABLE" && r["SUBPARTITION_METHOD"] == "NULLABLE" {
			// skip ignore
			continue
		} else {
			result = append(result, r)
		}
	}
	return result, nil
}
