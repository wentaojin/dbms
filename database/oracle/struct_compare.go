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
package oracle

import "fmt"

func (d *Database) GetDatabaseTablePartitionExpress(schemaName string, tableName string) ([]map[string]string, error) {
	sqlStr := fmt.Sprintf(`SELECT
	L.PARTITIONING_TYPE,
	L.SUBPARTITIONING_TYPE,
	L.PARTITION_EXPRESS,
	LISTAGG(SKC.COLUMN_NAME, ',') WITHIN GROUP (
	ORDER BY SKC.COLUMN_POSITION) AS SUBPARTITION_EXPRESS
FROM
	(
	SELECT
		PT.OWNER,
		PT.TABLE_NAME,
		PT.PARTITIONING_TYPE,
		PT.SUBPARTITIONING_TYPE,
		LISTAGG(PTC.COLUMN_NAME, ',') WITHIN GROUP (
		ORDER BY PTC.COLUMN_POSITION) AS PARTITION_EXPRESS
	FROM
		DBA_PART_TABLES PT,
		DBA_PART_KEY_COLUMNS PTC
	WHERE
		PT.OWNER = PTC.OWNER
		AND PT.TABLE_NAME = PTC.NAME
		AND PTC.OBJECT_TYPE = 'TABLE'
		AND PT.OWNER = '%s'
		AND PT.TABLE_NAME = '%s'
	GROUP BY
		PT.OWNER,
		PT.TABLE_NAME,
		PT.PARTITIONING_TYPE,
		PT.SUBPARTITIONING_TYPE) L,
	DBA_SUBPART_KEY_COLUMNS SKC
WHERE
	L.OWNER = SKC.OWNER
GROUP BY
	L.PARTITIONING_TYPE,
	L.SUBPARTITIONING_TYPE,
	L.PARTITION_EXPRESS`,
		schemaName,
		tableName)
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return nil, err
	}
	return res, nil
}
