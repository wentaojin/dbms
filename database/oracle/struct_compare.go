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
	// dba_tab_partitions、dba_tab_subpartitions
	sqlStr := fmt.Sprintf(`

SELECT
	P.OWNER,
	P.TABLE_NAME,
	P.PARTITIONING_TYPE,
	P.SUBPARTITIONING_TYPE,
	LTRIM(MAX(SYS_CONNECT_BY_PATH(P.COLUMN_NAME, ','))
	KEEP (DENSE_RANK LAST ORDER BY P.COLUMN_POSITION), ',') AS PARTITION_EXPRESS
FROM
	(
	SELECT
		PT.OWNER,
		PT.TABLE_NAME,
		PT.PARTITIONING_TYPE,
		PT.SUBPARTITIONING_TYPE,
		PTC.COLUMN_NAME,
		PTC.COLUMN_POSITION,
		ROW_NUMBER() OVER (PARTITION BY PTC.COLUMN_NAME
	ORDER BY
		PTC.COLUMN_POSITION)-1 RN_LAG
	FROM
		DBA_PART_TABLES PT,
		DBA_PART_KEY_COLUMNS PTC
	WHERE
		PT.OWNER = PTC.OWNER
		AND PT.TABLE_NAME = PTC.NAME
		AND PTC.OBJECT_TYPE = 'TABLE'
		AND PT.OWNER = '%s'
		AND PT.TABLE_NAME = '%s'
     ) P
GROUP BY
	P.OWNER,
	P.TABLE_NAME,
	P.PARTITIONING_TYPE,
	P.SUBPARTITIONING_TYPE
CONNECT BY
	P.RN_LAG = PRIOR P.COLUMN_POSITION
	AND P.COLUMN_NAME = PRIOR P.COLUMN_NAME
START WITH
	P.COLUMN_POSITION = 1
ORDER BY
	P.OWNER,
	P.TABLE_NAME,
	P.PARTITIONING_TYPE,
	P.SUBPARTITIONING_TYPE`, schemaName, tableName)
	_, resParts, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return nil, err
	}

	sqlStr = fmt.Sprintf(`SELECT
	OWNER,
	NAME,
	LTRIM(MAX(SYS_CONNECT_BY_PATH(COLUMN_NAME, ','))
	KEEP (DENSE_RANK LAST ORDER BY COLUMN_POSITION), ',') AS SUPARTITION_EXPRESS
FROM
	(
	SELECT
		PTC.OWNER,
		PTC.NAME,
		PTC.COLUMN_NAME,
		PTC.COLUMN_POSITION,
		ROW_NUMBER() OVER (PARTITION BY PTC.COLUMN_NAME
	ORDER BY
		PTC.COLUMN_POSITION)-1 RN_LAG
	FROM
		DBA_SUBPART_KEY_COLUMNS PTC
	WHERE
		PTC.OBJECT_TYPE = 'TABLE'
		AND PTC.OWNER = '%s'
		AND PTC.NAME = '%s'
     )
GROUP BY
	OWNER,
	NAME
CONNECT BY
	RN_LAG = PRIOR COLUMN_POSITION
	AND COLUMN_NAME = PRIOR COLUMN_NAME
START WITH
	COLUMN_POSITION = 1
ORDER BY
	OWNER,
	NAME`, schemaName, tableName)
	_, resSubparts, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return nil, err
	}

	for _, p := range resParts {
		for _, s := range resSubparts {
			if p["OWNER"] == s["OWNER"] && p["TABLE_NAME"] == s["NAME"] {
				p["SUBPARTITION_EXPRESS"] = s["SUPARTITION_EXPRESS"]
			}
		}
	}
	return resParts, nil
}
