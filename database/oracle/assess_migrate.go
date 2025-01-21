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

import (
	"fmt"
	"strings"
)

func (d *Database) GetDatabaseSchemaNameALL() ([]string, error) {
	_, res, err := d.GeneralQuery(`SELECT USERNAME FROM DBA_USERS WHERE USERNAME NOT IN (
				'HR',
				'DVF',
				'DVSYS',
				'LBACSYS',
				'MDDATA',
				'OLAPSYS',
				'ORDPLUGINS',
				'ORDDATA',
				'MDSYS',
				'SI_INFORMTN_SCHEMA',
				'ORDSYS',
				'CTXSYS',
				'OJVMSYS',
				'WMSYS',
				'ANONYMOUS',
				'XDB',
				'GGSYS',
				'GSMCATUSER',
				'APPQOSSYS',
				'DBSNMP',
				'SYS$UMF',
				'ORACLE_OCM',
				'DBSFWUSER',
				'REMOTE_SCHEDULER_AGENT',
				'XS$NULL',
				'DIP',
				'GSMROOTUSER',
				'GSMADMIN_INTERNAL',
				'GSMUSER',
				'OUTLN',
				'SYSBACKUP',
				'SYSDG',
				'SYSTEM',
				'SYSRAC',
				'AUDSYS',
				'SYSKM',
				'SYS',
				'OGG',
				'SPA',
				'APEX_050000',
				'SQL_MONITOR',
				'APEX_030200',
				'SYSMAN',
				'EXFSYS',
				'OWBSYS_AUDIT',
				'FLOWS_FILES',
				'OWBSYS'
			)`)
	if err != nil {
		return nil, err
	}

	var schemas []string
	for _, r := range res {
		schemas = append(schemas, r["USERNAME"])
	}

	return schemas, nil
}

func (d *Database) GetDatabaseSchemaNameSingle(schemaNameS string) (string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`	SELECT USERNAME FROM DBA_USERS WHERE USERNAME = '%v'`, schemaNameS))
	if err != nil {
		return "", err
	}
	return res[0]["USERNAME"], nil
}

func (d *Database) GetDatabaseSoftVersion() (string, error) {
	_, res, err := d.GeneralQuery(`SELECT BANNER FROM V$VERSION`)
	if err != nil {
		return "", err
	}
	if strings.Contains(res[0]["BANNER"], "Enterprise Edition Release") {
		return "Enterprise Edition Release", nil
	}
	if strings.Contains(res[0]["BANNER"], "Express Edition Release") {
		return "Express Edition Release", nil
	}
	return res[0]["BANNER"], nil
}

func (d *Database) GetDatabasePlatformName() (string, string, string, error) {
	_, res, err := d.GeneralQuery(`SELECT NAME AS DBNAME,PLATFORM_ID,PLATFORM_NAME FROM V$DATABASE`)
	if err != nil {
		return "", "", "", err
	}
	return res[0]["DBNAME"], res[0]["PLATFORM_ID"], res[0]["PLATFORM_NAME"], nil
}

func (d *Database) GetDatabaseGlobalName() (string, error) {
	_, res, err := d.GeneralQuery(`SELECT GLOBAL_NAME FROM GLOBAL_NAME`)
	if err != nil {
		return "", err
	}
	return res[0]["GLOBAL_NAME"], nil
}

func (d *Database) GetDatabaseParameters() (string, string, string, string, error) {
	var (
		dbBlockSize             string
		clusterDatabase         string
		CLusterDatabaseInstance string
		characterSet            string
	)
	var isExpress bool
	version, err := d.GetDatabaseSoftVersion()
	if err != nil {
		return "", "", "", "", err
	}
	if strings.EqualFold(version, "Express Edition Release") {
		isExpress = true
	} else {
		isExpress = false
	}

	_, res, err := d.GeneralQuery(`SELECT VALUE FROM V$PARAMETER WHERE	NAME = 'db_block_size'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err
	}
	dbBlockSize = res[0]["VALUE"]

	_, res, err = d.GeneralQuery(`SELECT VALUE FROM V$PARAMETER WHERE	NAME = 'cluster_database'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err
	}
	clusterDatabase = res[0]["VALUE"]

	if !isExpress {
		_, res, err = d.GeneralQuery(`SELECT VALUE FROM V$PARAMETER WHERE	NAME = 'cluster_database_instances'`)
		if err != nil {
			return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err
		}
		CLusterDatabaseInstance = res[0]["VALUE"]
	} else {
		// express database not support
		CLusterDatabaseInstance = "1"
	}

	_, res, err = d.GeneralQuery(`SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER='NLS_CHARACTERSET'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err
	}
	characterSet = res[0]["VALUE"]

	return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, nil
}

func (d *Database) GetDatabaseInstance() ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(`SELECT
	HOST_NAME,
	INSTANCE_NAME,
	INSTANCE_NUMBER,
	THREAD# THREAD_NUMBER 
FROM
	V$INSTANCE`)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSize() (string, error) {
	_, res, err := d.GeneralQuery(`SELECT
    (A.BYTES - F.BYTES)/1024/1024/1024 GB
FROM
    ( SELECT SUM(BYTES) BYTES
      FROM DBA_DATA_FILES WHERE TABLESPACE_NAME NOT IN('SYSTEM','SYSAUX')
     ) A
  , ( SELECT SUM(BYTES) BYTES
      FROM DBA_FREE_SPACE WHERE TABLESPACE_NAME NOT IN('SYSTEM','SYSAUX')
     ) F`)
	if err != nil {
		return "", err
	}
	return res[0]["GB"], nil
}

func (d *Database) GetDatabaseServerNumCPU() (string, error) {
	_, res, err := d.GeneralQuery(`SELECT VALUE FROM GV$OSSTAT WHERE STAT_NAME = 'NUM_CPUS'`)
	if err != nil {
		return "", err
	}
	return res[0]["VALUE"], nil
}

func (d *Database) GetDatabaseServerMemorySize() (string, error) {
	_, res, err := d.GeneralQuery(`SELECT VALUE/1024/1024/1024 MEM_GB FROM V$OSSTAT WHERE STAT_NAME='PHYSICAL_MEMORY_BYTES'`)
	if err != nil {
		return "", err
	}
	return res[0]["MEM_GB"], nil
}

func (d *Database) GetDatabaseSessionMaxActiveCount() ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(`SELECT
	ROWNUM,
	A.*
FROM
	(
	SELECT
		DBID,
		INSTANCE_NUMBER,
		SAMPLE_ID,
		SAMPLE_TIME,
		COUNT(*) SESSION_COUNT
	FROM
		DBA_HIST_ACTIVE_SESS_HISTORY T
	GROUP BY
		DBID,
		INSTANCE_NUMBER,
		SAMPLE_ID,
		SAMPLE_TIME
	ORDER BY
		SESSION_COUNT DESC NULLS LAST) A
WHERE
	ROWNUM <= 5`)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaTableIndexOverview(schemaNameS []string) ([]map[string]string, error) {
	var (
		userSQL string
		vals    []map[string]string
	)

	userSQL = fmt.Sprintf(`SELECT USERNAME FROM DBA_USERS WHERE USERNAME IN (%s)`, strings.Join(schemaNameS, ","))

	_, res, err := d.GeneralQuery(userSQL)
	if err != nil {
		return vals, err
	}

	for _, val := range res {
		owner := fmt.Sprintf("'%s'", strings.ToUpper(val["USERNAME"]))
		_, tableRes, err := d.GeneralQuery(fmt.Sprintf(`SELECT ROUND(NVL(SUM( BYTES )/ 1024 / 1024 / 1024,0),2) GB 
FROM
	DBA_SEGMENTS
WHERE
	OWNER IN (%s)  
	AND SEGMENT_TYPE IN ( 'TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION' )`, owner))
		if err != nil {
			return vals, err
		}

		_, indexRes, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	ROUND(NVL(SUM(BYTES)/ 1024 / 1024 / 1024, 0), 2) GB
FROM
	DBA_INDEXES I,
	DBA_SEGMENTS S
WHERE
	S.SEGMENT_NAME = I.INDEX_NAME
	AND S.OWNER = I.OWNER
	AND S.OWNER IN (%s)
	AND S.SEGMENT_TYPE IN ( 'INDEX', 'INDEX PARTITION', 'INDEX SUBPARTITION' )`, owner))
		if err != nil {
			return vals, err
		}

		_, lobTable, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	ROUND(NVL(SUM(BYTES)/ 1024 / 1024 / 1024, 0) , 2) GB
FROM
	DBA_LOBS L,
	DBA_SEGMENTS S
WHERE
	S.SEGMENT_NAME = L.SEGMENT_NAME
	AND S.OWNER = L.OWNER
	AND S.OWNER IN (%s)
	AND S.SEGMENT_TYPE IN ( 'LOBSEGMENT', 'LOB PARTITION' )`, owner))
		if err != nil {
			return vals, err
		}
		_, lobIndex, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	ROUND(NVL(SUM(BYTES)/ 1024 / 1024 / 1024, 0), 2) GB
FROM
	DBA_LOBS L,
	DBA_SEGMENTS S
WHERE
	S.SEGMENT_NAME = L.INDEX_NAME
	AND S.OWNER = L.OWNER
	AND S.OWNER IN (%s)
	AND S.SEGMENT_TYPE = 'LOBINDEX'`, owner))

		if err != nil {
			return vals, err
		}

		_, tableRows, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	NVL(SUM(NUM_ROWS),
	0) NUM_ROWS
FROM
	DBA_TABLES
WHERE
	OWNER IN (%s)`, owner))
		if err != nil {
			return vals, err
		}

		if tableRes[0]["GB"] == "" {
			tableRes[0]["GB"] = "0"
		}
		if indexRes[0]["GB"] == "" {
			indexRes[0]["GB"] = "0"
		}
		if lobTable[0]["GB"] == "" {
			lobTable[0]["GB"] = "0"
		}
		if lobIndex[0]["GB"] == "" {
			lobIndex[0]["GB"] = "0"
		}

		vals = append(vals, map[string]string{
			"SCHEMA":   strings.ToUpper(val["USERNAME"]),
			"TABLE":    tableRes[0]["GB"],
			"INDEX":    indexRes[0]["GB"],
			"LOBTABLE": lobTable[0]["GB"],
			"LOBINDEX": lobIndex[0]["GB"],
			"ROWCOUNT": tableRows[0]["NUM_ROWS"],
		})
	}
	return vals, nil
}

func (d *Database) GetDatabaseSchemaTableRowsTOP(schemaName []string, top int) ([]map[string]string, error) {
	var (
		userSQL string
		vals    []map[string]string
	)

	userSQL = fmt.Sprintf(`SELECT USERNAME FROM DBA_USERS WHERE USERNAME IN (%s)`, strings.Join(schemaName, ","))

	_, res, err := d.GeneralQuery(userSQL)
	if err != nil {
		return vals, err
	}
	for _, val := range res {
		owner := fmt.Sprintf("'%s'", strings.ToUpper(val["USERNAME"]))
		_, tableRes, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	*
FROM
	(
	SELECT
		S.OWNER,
		S.SEGMENT_NAME,
		S.SEGMENT_TYPE,
		ROUND(NVL(SUM(S.BYTES)/ 1024 / 1024 / 1024 , 0), 2) GB
	FROM
		DBA_SEGMENTS S
	WHERE
		S.OWNER IN (%s)
		AND S.SEGMENT_TYPE IN (
			'TABLE',
			'TABLE PARTITION',
		'TABLE SUBPARTITION')
		AND NOT EXISTS (
		SELECT 1
		FROM
			DBA_RECYCLEBIN B
		WHERE
			S.OWNER = B.OWNER
			AND S.SEGMENT_NAME = B.OBJECT_NAME
		)
	GROUP BY
		S.OWNER,
		S.SEGMENT_NAME,
		S.SEGMENT_TYPE
	ORDER BY
		GB DESC
)
WHERE
	ROWNUM <= %d`, owner, top))
		if err != nil {
			return vals, err
		}
		for _, res := range tableRes {
			vals = append(vals, map[string]string{
				"SCHEMA":       strings.ToUpper(val["USERNAME"]),
				"SEGMENT_NAME": res["SEGMENT_NAME"],
				"SEGMENT_TYPE": res["SEGMENT_TYPE"],
				"TABLE_SIZE":   res["GB"],
			})
		}

	}
	return vals, err
}

func (d *Database) GetDatabaseSchemaCodeObject(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	S.OWNER,
	S.NAME,
	S.TYPE,
	MAX(S.LINE) LINES
FROM
	DBA_SOURCE S
WHERE
	S.OWNER IN (%s)
	AND NOT EXISTS (
	SELECT
		1
	FROM
		DBA_RECYCLEBIN B
	WHERE
		S.OWNER = B.OWNER
		AND S.NAME = B.OBJECT_NAME
		)
GROUP BY
	S.OWNER,
	S.NAME,
	S.TYPE
ORDER BY
	LINES DESC`, strings.Join(schemaName, ","))

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaPartitionTableType(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT DISTINCT
	S.OWNER,
	S.TABLE_NAME,
	S.PARTITIONING_TYPE,
	S.SUBPARTITIONING_TYPE 
FROM
	DBA_PART_TABLES S
WHERE
	S.OWNER IN (%s)
	AND NOT EXISTS (
	SELECT
		1
	FROM
		DBA_RECYCLEBIN B
	WHERE
		S.OWNER = B.OWNER
		AND S.TABLE_NAME = B.OBJECT_NAME
)`, strings.Join(schemaName, ","))

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaTableAvgRowLengthTOP(schemaName []string, top int) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	ROWNUM,
	A.*
FROM
	(
	SELECT
		S.OWNER,
		S.TABLE_NAME,
		S.AVG_ROW_LEN
	FROM
		DBA_TABLES S
	WHERE
		S.OWNER IN (%s)
		AND NOT EXISTS (
		SELECT
			1
		FROM
			DBA_RECYCLEBIN B
		WHERE
			S.OWNER = B.OWNER
			AND S.TABLE_NAME = B.OBJECT_NAME)
	ORDER BY
		AVG_ROW_LEN DESC NULLS LAST) A
WHERE
	ROWNUM <= %d`, strings.Join(schemaName, ","), top)

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaSynonymObject(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT OWNER,SYNONYM_NAME,TABLE_OWNER,TABLE_NAME FROM DBA_SYNONYMS WHERE TABLE_OWNER IN (%s)`, strings.Join(schemaName, ","))
	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaMaterializedViewObject(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT OWNER,MVIEW_NAME,REWRITE_CAPABILITY,REFRESH_MODE,REFRESH_METHOD,FAST_REFRESHABLE FROM DBA_MVIEWS WHERE OWNER IN (%s)`, strings.Join(schemaName, ","))
	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaPartitionTableCountsOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	S.OWNER,
	S.TABLE_NAME,
	S.PARTITION_COUNT
FROM
	DBA_PART_TABLES S
WHERE
	S.OWNER IN (%s)
	AND S.PARTITION_COUNT > %d
	AND NOT EXISTS (
	SELECT
			1
	FROM
			DBA_RECYCLEBIN B
	WHERE
			S.OWNER = B.OWNER
		AND S.TABLE_NAME = B.OBJECT_NAME
	)`, strings.Join(schemaName, ","), limit)

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaTableAvgRowLengthOverLimitMB(schemaName []string, limitMB int) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	S.OWNER,
	S.TABLE_NAME,
	ROUND(S.AVG_ROW_LEN / 1024 / 1024, 2) AS AVG_ROW_LEN
FROM
	DBA_TABLES S
WHERE
	S.OWNER IN (%s)
	AND ROUND(S.AVG_ROW_LEN / 1024 / 1024, 2) >= %d
	AND NOT EXISTS (
	SELECT
			1
	FROM
			DBA_RECYCLEBIN B
	WHERE
			S.OWNER = B.OWNER
		AND S.TABLE_NAME = B.OBJECT_NAME
	)`, strings.Join(schemaName, ","), limitMB)

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaTableIndexLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	INDEX_OWNER,
	INDEX_NAME,
	TABLE_NAME,
	SUM(COLUMN_LENGTH)* 4 LENGTH_OVER
FROM
	DBA_IND_COLUMNS
WHERE
	TABLE_OWNER IN (%s)
GROUP BY
	INDEX_OWNER,
	INDEX_NAME,
	TABLE_NAME
HAVING
	SUM(COLUMN_LENGTH)* 4 > %d`, strings.Join(schemaName, ","), limit)

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaTableColumnCountsOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	OWNER,
	TABLE_NAME,
	COUNT(COLUMN_NAME) COUNT_OVER
FROM
	DBA_TAB_COLUMNS
WHERE
	OWNER IN (%s)
GROUP BY
	OWNER,
	TABLE_NAME
HAVING
	COUNT(COLUMN_NAME) > %d
ORDER BY
	OWNER,
	TABLE_NAME`, strings.Join(schemaName, ","), limit)

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaTableIndexCountsOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	TABLE_OWNER,
	TABLE_NAME,
	COUNT(INDEX_NAME) COUNT_OVER
FROM
	DBA_IND_COLUMNS
WHERE
	TABLE_OWNER IN (%s)
GROUP BY
	TABLE_OWNER,
	TABLE_NAME
HAVING
	COUNT(INDEX_NAME) > %d
ORDER BY
	TABLE_OWNER,
	TABLE_NAME`, strings.Join(schemaName, ","), limit)

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaTableSpecialDatatype(schemaName []string, specialDatatype string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	OWNER,
	TABLE_NAME,
	COLUMN_NAME,
	NVL(DATA_PRECISION,0) DATA_PRECISION,
	NVL(DATA_SCALE,0) DATA_SCALE
FROM
	DBA_TAB_COLUMNS
WHERE
	DATA_PRECISION IS NULL
	AND OWNER IN (%s)
	AND DATA_TYPE = '%s'
	AND DATA_PRECISION IS NULL
ORDER BY
	OWNER,
	COLUMN_NAME`, strings.Join(schemaName, ","), specialDatatype)
	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseUsernameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	USERNAME,
	ACCOUNT_STATUS,
	CREATED,
	LENGTH(USERNAME) LENGTH_OVER
FROM
	DBA_USERS
WHERE
	USERNAME IN (%s)
	AND LENGTH(USERNAME) > %d`, strings.Join(schemaName, ","), limit)
	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaTableNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	S.OWNER,
	S.TABLE_NAME,
	LENGTH(S.TABLE_NAME) LENGTH_OVER
FROM
	DBA_TABLES S
WHERE
	S.OWNER IN (%s)
	AND LENGTH(S.TABLE_NAME) > %d
	AND NOT EXISTS (
	SELECT
			1
	FROM
			DBA_RECYCLEBIN B
	WHERE
			S.OWNER = B.OWNER
		AND S.TABLE_NAME = B.OBJECT_NAME
	)
ORDER BY
	OWNER,
	TABLE_NAME
`, strings.Join(schemaName, ","), limit)

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaTableColumnNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	OWNER,
	TABLE_NAME,
	COLUMN_NAME,
	LENGTH(COLUMN_NAME) LENGTH_OVER
FROM
	DBA_TAB_COLUMNS
WHERE
	OWNER IN (%s)
	AND LENGTH(COLUMN_NAME) > %d
ORDER BY
	OWNER,
	TABLE_NAME,
	COLUMN_NAME`, strings.Join(schemaName, ","), limit)

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaTableIndexNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	INDEX_OWNER,
	TABLE_NAME,
	INDEX_NAME,
	LENGTH(COLUMN_NAME) LENGTH_OVER
FROM
	DBA_IND_COLUMNS
WHERE
	TABLE_OWNER IN (%s)
	AND LENGTH(COLUMN_NAME) > %d
ORDER BY
	INDEX_OWNER,
	TABLE_NAME,
	INDEX_NAME`, strings.Join(schemaName, ","), limit)

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaTableViewNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	// Oracle database 11g Release 2 and UP
	//	querySQL := fmt.Sprintf(`SELECT
	//	OWNER,
	//	VIEW_NAME,
	//	READ_ONLY,
	//	LENGTH(VIEW_NAME) LENGTH_OVER
	//FROM
	//	DBA_VIEWS
	//WHERE
	//	OWNER IN (%s)
	//	AND LENGTH(VIEW_NAME) > %d
	//ORDER BY
	//	OWNER,
	//	VIEW_NAME`, strings.Join(schemaName, ","), limit)
	// Oracle database 9i and UP
	querySQL := fmt.Sprintf(`SELECT
		OWNER,
		VIEW_NAME,
		TEXT AS READ_ONLY,
		LENGTH(VIEW_NAME) LENGTH_OVER
	FROM
		DBA_VIEWS
	WHERE
		OWNER IN (%s)
		AND LENGTH(VIEW_NAME) > %d
	ORDER BY
		OWNER,
		VIEW_NAME`, strings.Join(schemaName, ","), limit)
	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}

	for _, r := range res {
		if strings.Contains(r["READ_ONLY"], "WITH READ ONLY") {
			r["READ_ONLY"] = "Y"
		} else {
			r["READ_ONLY"] = "N"
		}
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaTableSequenceNameLengthOverLimit(schemaName []string, limit int) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	SEQUENCE_OWNER,
	SEQUENCE_NAME,
	ORDER_FLAG,
	LENGTH(SEQUENCE_NAME) LENGTH_OVER
FROM
	DBA_SEQUENCES
WHERE
	SEQUENCE_OWNER IN (%s)
	AND LENGTH(SEQUENCE_NAME) > %d
ORDER BY
	SEQUENCE_OWNER,
	SEQUENCE_NAME,
	ORDER_FLAG`, strings.Join(schemaName, ","), limit)

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaTableTypeCounts(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	TEMP.SCHEMA_NAME,
	TEMP.TABLE_TYPE,
	SUM(TEMP.OBJECT_SIZE) AS OBJECT_SIZE,
	COUNT(1) AS COUNTS
FROM
	(
	SELECT
		F.OWNER AS SCHEMA_NAME,
		(
	CASE
			WHEN F.CLUSTER_NAME IS NOT NULL THEN 'CLUSTERED'
			ELSE
		CASE
				WHEN F.IOT_TYPE = 'IOT' THEN
			CASE
					WHEN T.IOT_TYPE != 'IOT' THEN T.IOT_TYPE
					ELSE 'IOT'
				END
				ELSE
				CASE	
						WHEN F.PARTITIONED = 'YES' THEN 'PARTITIONED'
					ELSE
					CASE
							WHEN F.TEMPORARY = 'Y' THEN 
							DECODE(F.DURATION,'SYS$SESSION','SESSION TEMPORARY','SYS$TRANSACTION','TRANSACTION TEMPORARY')
						ELSE 'HEAP'
					END
				END
			END
		END ) TABLE_TYPE,
		ROUND(NVL(F.NUM_ROWS * F.AVG_ROW_LEN / 1024 / 1024 / 1024, 0), 2) AS OBJECT_SIZE
	FROM
		(
		SELECT
			TMP.OWNER,
			TMP.NUM_ROWS,
			TMP.AVG_ROW_LEN,
			TMP.TABLE_NAME,
			TMP.CLUSTER_NAME,
			TMP.PARTITIONED,
			TMP.TEMPORARY,
			TMP.DURATION,
			TMP.IOT_TYPE
		FROM
			DBA_TABLES TMP,
			DBA_TABLES W
		WHERE
			TMP.OWNER = W.OWNER
			AND TMP.TABLE_NAME = W.TABLE_NAME
			AND TMP.OWNER IN (%s)
		  	AND NOT EXISTS (
		  	    SELECT 
		  	        1
		  	    FROM 
		  	        DBA_RECYCLEBIN B
		  	    WHERE
		  	        TMP.OWNER = B.OWNER
		  	      AND TMP.TABLE_NAME = B.OBJECT_NAME
		  	)
			AND (W.IOT_TYPE IS NULL
				OR W.IOT_TYPE = 'IOT')) F
	LEFT JOIN (
		SELECT
			OWNER,
			IOT_NAME,
			IOT_TYPE
		FROM
			DBA_TABLES
		WHERE
			OWNER IN (%s)) T 
ON
		F.OWNER = T.OWNER
		AND F.TABLE_NAME = T.IOT_NAME) TEMP
GROUP BY
	TEMP.SCHEMA_NAME,
	TEMP.TABLE_TYPE`, strings.Join(schemaName, ","), strings.Join(schemaName, ","))

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaColumnDataDefaultCounts(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT 
	XS.OWNER,
	XS.DATA_DEFAULT,
	COUNT(1) COUNTS
FROM
	XMLTABLE (
		'/ROWSET/ROW' PASSING (
	SELECT
		DBMS_XMLGEN.GETXMLTYPE (
				Q'[SELECT T.OWNER,
	    T.DATA_DEFAULT
	FROM DBA_TAB_COLUMNS T, DBA_COL_COMMENTS C
	WHERE T.TABLE_NAME = C.TABLE_NAME
	AND T.COLUMN_NAME = C.COLUMN_NAME
	AND T.OWNER = C.OWNER
	AND T.OWNER IN (%s)]' )
	FROM
		DUAL ) COLUMNS OWNER VARCHAR2 (300) PATH 'OWNER',
		DATA_DEFAULT VARCHAR2 (4000)
	) XS
GROUP BY
	XS.OWNER,
	XS.DATA_DEFAULT`, strings.Join(schemaName, ","))
	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaViewTypeCounts(schemaName []string) ([]map[string]string, error) {
	// Type of the view if the view is a typed view -> view_type
	querySQL := fmt.Sprintf(`SELECT
	OWNER,
	NVL(VIEW_TYPE,'VIEW') VIEW_TYPE,
	VIEW_TYPE_OWNER,
	COUNT(1) AS COUNTS
FROM
	DBA_VIEWS
WHERE
	OWNER IN (%s)
GROUP BY
	OWNER,
	VIEW_TYPE,
	VIEW_TYPE_OWNER`, strings.Join(schemaName, ","))
	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaObjectTypeCounts(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	OWNER,
	OBJECT_TYPE,
	COUNT(1) COUNTS
FROM
	DBA_OBJECTS
WHERE
	OWNER IN (%s)
	AND OBJECT_TYPE NOT IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION', 'INDEX', 'VIEW')
GROUP BY
	OWNER,
	OBJECT_TYPE`, strings.Join(schemaName, ","))

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaPartitionTypeCounts(schemaName []string) ([]map[string]string, error) {
	// SUBPARTITIONING_TYPE = 'NONE' -> remove subpartition
	querySQL := fmt.Sprintf(`SELECT 
	TEMP.OWNER,
	TEMP.PARTITIONING_TYPE,
	COUNT(1) COUNTS
FROM
	(
	SELECT
		S.OWNER,
		S.PARTITIONING_TYPE
	FROM
		DBA_PART_TABLES S
	WHERE
		S.OWNER IN (%s)
		AND S.SUBPARTITIONING_TYPE = 'NONE'
		AND NOT EXISTS (
		SELECT
			1
		FROM
			DBA_RECYCLEBIN B
		WHERE
			S.OWNER = B.OWNER
			AND S.TABLE_NAME = B.OBJECT_NAME)) TEMP
GROUP BY
	TEMP.OWNER,
	TEMP.PARTITIONING_TYPE`, strings.Join(schemaName, ","))

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaSubPartitionTypeCounts(schemaName []string) ([]map[string]string, error) {
	// SUBPARTITIONING_TYPE <> 'NONE'  -> remove normal partition
	querySQL := fmt.Sprintf(`SELECT 
	TEMP.OWNER,
	TEMP.SUBPARTITIONING_TYPE,
	COUNT(1) COUNTS
FROM
	(
	SELECT
		S.OWNER,
		S.PARTITIONING_TYPE || '-' || S.SUBPARTITIONING_TYPE AS SUBPARTITIONING_TYPE
	FROM
		DBA_PART_TABLES S
	WHERE
		S.OWNER IN (%s)
		AND S.SUBPARTITIONING_TYPE <> 'NONE'
		AND NOT EXISTS (
		SELECT
			1
		FROM
			DBA_RECYCLEBIN B
		WHERE
			S.OWNER = B.OWNER
			AND S.TABLE_NAME = B.OBJECT_NAME)
) TEMP
GROUP BY
	TEMP.OWNER,
	TEMP.SUBPARTITIONING_TYPE`, strings.Join(schemaName, ","))

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaTemporaryTableTypeCounts(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	OWNER,
	DURATION AS TEMP_TYPE,
	COUNT(*) COUNT
FROM
	DBA_TABLES
WHERE
	OWNER IN (%s)
	AND TEMPORARY = 'Y'
	AND DURATION IS NOT NULL
GROUP BY
	OWNER,
	DURATION`, strings.Join(schemaName, ","))

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaConstraintTypeCounts(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	OWNER,
	CONSTRAINT_TYPE,
	COUNT(*) COUNT
FROM
	DBA_CONSTRAINTS
WHERE
	OWNER IN (%s)
GROUP BY
	OWNER,
	CONSTRAINT_TYPE
ORDER BY
	COUNT DESC`, strings.Join(schemaName, ","))
	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaIndexTypeCounts(schemaName []string) ([]map[string]string, error) {
	// Retain LOB INDEX, LOB INDEX is used for LOB fields, and the index automatically created by the Oracle database for this field
	querySQL := fmt.Sprintf(`SELECT
	TABLE_OWNER,
	INDEX_TYPE,
	COUNT(*) COUNT
FROM
	(
	SELECT
		DISTINCT A.TABLE_OWNER,
		A.TABLE_NAME,
		A.INDEX_OWNER,
		A.INDEX_NAME,
		INDEX_TYPE
	FROM
		DBA_IND_COLUMNS A,
		DBA_INDEXES B
	WHERE
		A.TABLE_OWNER = B.TABLE_OWNER
		AND A.TABLE_NAME = B.TABLE_NAME
		AND A.INDEX_OWNER = B.OWNER
		AND A.TABLE_OWNER IN (%s)
		AND B.INDEX_TYPE <> 'LOB'
		AND A.INDEX_NAME = B.INDEX_NAME
		AND A.INDEX_NAME NOT IN (
		SELECT
			INDEX_NAME
		FROM
			DBA_LOBS
		WHERE
			OWNER IN (%s))
)
GROUP BY
	TABLE_OWNER,
	INDEX_TYPE
ORDER BY
	COUNT DESC`, strings.Join(schemaName, ","), strings.Join(schemaName, ","))

	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaColumnTypeCounts(schemaName []string) ([]map[string]string, error) {
	querySQL := fmt.Sprintf(`SELECT
	OWNER,
	DATA_TYPE,
	COUNT(*) COUNT,
	CASE
		WHEN DATA_TYPE = 'NUMBER' THEN MAX(TO_NUMBER(DECODE(NVL(TO_CHAR(DATA_PRECISION), '*'), '*', '38', TO_CHAR(DATA_PRECISION))))
		WHEN REGEXP_LIKE(DATA_TYPE, 'INTERVAL YEAR.*') THEN MAX(TO_NUMBER(DECODE(NVL(TO_CHAR(DATA_PRECISION), '*'), '*', '38', TO_CHAR(DATA_PRECISION))))
		WHEN REGEXP_LIKE(DATA_TYPE, 'INTERVAL DAY.*') THEN MAX(TO_NUMBER(DECODE(NVL(TO_CHAR(DATA_SCALE), '*'), '*', '127', TO_CHAR(DATA_SCALE))))
		WHEN REGEXP_LIKE(DATA_TYPE, 'TIMESTAMP.*') THEN MAX(TO_NUMBER(DECODE(NVL(TO_CHAR(DATA_SCALE), '*'), '*', '127', TO_CHAR(DATA_SCALE))))
		ELSE
	 MAX(DATA_LENGTH)
	END AS MAX_DATA_LENGTH
FROM
	DBA_TAB_COLUMNS
WHERE
	OWNER IN (%s)
GROUP BY
	OWNER,
	DATA_TYPE
ORDER BY
	COUNT DESC`, strings.Join(schemaName, ","))
	_, res, err := d.GeneralQuery(querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}
