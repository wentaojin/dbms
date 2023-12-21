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
	"regexp"
	"strings"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/utils/stringutil"
)

func (d *Database) GetDatabasePartitionTable(schemaName string) ([]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT table_name AS TABLE_NAME
	FROM DBA_TABLES
 WHERE partitioned = 'YES'
   AND UPPER(owner) = UPPER('%s')`, schemaName))
	if err != nil {
		return []string{}, err
	}

	var tables []string
	for _, r := range res {
		tables = append(tables, r["TABLE_NAME"])
	}
	return tables, nil
}

func (d *Database) GetDatabaseTemporaryTable(schemaName string) ([]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`select table_name AS TABLE_NAME
  from dba_tables
 where TEMPORARY = 'Y'
   and upper(owner) = upper('%s')`, schemaName))
	if err != nil {
		return []string{}, err
	}

	var tables []string
	for _, r := range res {
		tables = append(tables, r["TABLE_NAME"])
	}
	return tables, nil
}

func (d *Database) GetDatabaseClusteredTable(schemaName string) ([]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`select table_name AS TABLE_NAME
  from dba_tables
 where CLUSTER_NAME IS NOT NULL
   and upper(owner) = upper('%s')`, schemaName))
	if err != nil {
		return []string{}, err
	}

	var tables []string
	for _, r := range res {
		tables = append(tables, r["TABLE_NAME"])
	}
	return tables, nil
}

func (d *Database) GetDatabaseMaterializedView(schemaName string) ([]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT OWNER,MVIEW_NAME FROM DBA_MVIEWS WHERE UPPER(OWNER) = UPPER('%s')`, schemaName))
	if err != nil {
		return []string{}, err
	}

	var tables []string
	for _, r := range res {
		tables = append(tables, r["MVIEW_NAME"])
	}
	return tables, nil
}

func (d *Database) GetDatabaseTableType(schemaName string) (map[string]string, error) {
	tableTypeMap := make(map[string]string)

	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	f.TABLE_NAME,
	(
	CASE
		WHEN f.CLUSTER_NAME IS NOT NULL THEN 'CLUSTERED'
		WHEN (SELECT COUNT(1) FROM DBA_MVIEWS v WHERE v.owner = f.owner AND f.table_name = v.MVIEW_NAME) = 1 THEN 'MATERIALIZED VIEW'
		ELSE
		CASE
			WHEN f.IOT_TYPE = 'IOT' THEN
			CASE
				WHEN t.IOT_TYPE != 'IOT' THEN t.IOT_TYPE
				ELSE 'IOT'
			END
			ELSE
				CASE	
						WHEN f.PARTITIONED = 'YES' THEN 'PARTITIONED'
				ELSE
					CASE
							WHEN f.TEMPORARY = 'Y' THEN 
							DECODE(f.DURATION, 'SYS$SESSION', 'SESSION TEMPORARY', 'SYS$TRANSACTION', 'TRANSACTION TEMPORARY')
					ELSE 'HEAP'
				END
			END
		END
	END ) TABLE_TYPE
FROM
	(
	SELECT
		tmp.owner,
		tmp.TABLE_NAME,
		tmp.CLUSTER_NAME,
		tmp.PARTITIONED,
		tmp.TEMPORARY,
		tmp.DURATION,
		tmp.IOT_TYPE
	FROM
		DBA_TABLES tmp,
		DBA_TABLES w
	WHERE
		tmp.owner = w.owner
		AND tmp.table_name = w.table_name
		AND tmp.owner = '%s'
		AND (w.IOT_TYPE IS NULL
			OR w.IOT_TYPE = 'IOT')) f
LEFT JOIN (
	SELECT
		owner,
		iot_name,
		iot_type
	FROM
		DBA_TABLES
	WHERE
		owner = '%s')t 
ON
	f.owner = t.owner
	AND f.table_name = t.iot_name`, strings.ToUpper(schemaName), strings.ToUpper(schemaName)))
	if err != nil {
		return tableTypeMap, err
	}
	if len(res) == 0 {
		return tableTypeMap, fmt.Errorf("oracle schema [%s] table type can't be null", schemaName)
	}

	for _, r := range res {
		if len(r) > 2 || len(r) == 0 || len(r) == 1 {
			return tableTypeMap, fmt.Errorf("oracle schema [%s] table type values should be 2, result: %v", schemaName, r)
		}
		tableTypeMap[r["TABLE_NAME"]] = r["TABLE_TYPE"]
	}

	return tableTypeMap, nil
}

func (d *Database) GetDatabaseCharset() (string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`select userenv('language') AS LANG from dual`))
	if err != nil {
		return res[0]["LANG"], err
	}
	return stringutil.StringSplit(res[0]["LANG"], ".")[1], nil
}

func (d *Database) GetDatabaseCharsetCollation() (string, string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT PARAMETER,VALUE from NLS_DATABASE_PARAMETERS WHERE PARAMETER IN ('NLS_SORT','NLS_COMP')`))
	if err != nil {
		return "", "", err
	}
	var (
		nlsComp string
		nlsSort string
	)
	for _, r := range res {
		if strings.EqualFold(r["PARAMETER"], "NLS_COMP") {
			nlsComp = r["VALUE"]
		} else if strings.EqualFold(r["PARAMETER"], "NLS_SORT") {
			nlsSort = r["VALUE"]
		} else {
			return "", "", fmt.Errorf("get database character set collation failed: [%v]", r)
		}
	}
	return nlsComp, nlsSort, nil
}

func (d *Database) GetDatabaseVersion() (string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`select VALUE from NLS_DATABASE_PARAMETERS WHERE PARAMETER='NLS_RDBMS_VERSION'`))
	if err != nil {
		return res[0]["VALUE"], err
	}
	return res[0]["VALUE"], nil
}

func (d *Database) GetDatabaseTableColumns(schemaName string, tableName string, collation bool) ([]map[string]string, error) {
	var queryStr string
	/*
			1、dataPrecision Accuracy range ORA-01727: numeric precision specifier is out of range (1 to 38)
			2、dataScale Accuracy range ORA-01728: numeric scale specifier is out of range (-84 to 127)
			3、oracle number datatype，view table struct by using command desc tableName
			- number(*,10) -> number(38,10)
			- number(*,0) -> number(38,0)
			- number(*) -> number
			- number -> number
		    - number(x,y) -> number(x,y)
			4、SQL Query
			- number(*,10) -> number(38,10)
			- number(*,0) -> number(38,0)
			- number(*) -> number(38,127)
			- number -> number(38,127)
			- number(x,y) -> number(x,y)
	*/
	if collation {
		queryStr = fmt.Sprintf(`SELECT 
		t.COLUMN_NAME,
	    t.DATA_TYPE,
		t.CHAR_LENGTH,
		NVL(t.CHAR_USED, 'UNKNOWN') CHAR_USED,
	    NVL(t.DATA_LENGTH, 0) AS DATA_LENGTH,
	    DECODE(NVL(TO_CHAR(t.DATA_PRECISION), '*'), '*', '38', TO_CHAR(t.DATA_PRECISION)) AS DATA_PRECISION,
	    DECODE(NVL(TO_CHAR(t.DATA_SCALE), '*'), '*', '127', TO_CHAR(t.DATA_SCALE)) AS DATA_SCALE,
		t.NULLABLE,
	    NVL(s.DATA_DEFAULT, 'NULLSTRING') DATA_DEFAULT,
		DECODE(t.COLLATION, 'USING_NLS_COMP',(SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_COMP'), t.COLLATION) COLLATION,
	    c.COMMENTS
FROM
	dba_tab_columns t,
	dba_col_comments c,
	(
	SELECT xs.OWNER,xs.TABLE_NAME,xs.COLUMN_NAME,xs.DATA_DEFAULT FROM
	XMLTABLE (
		'/ROWSET/ROW' PASSING (
	SELECT
		DBMS_XMLGEN.GETXMLTYPE (
				q'[SELECT d.OWNER,d.TABLE_NAME,d.COLUMN_NAME,d.DATA_DEFAULT FROM DBA_TAB_COLUMNS d WHERE upper(d.owner) = upper('%s') AND upper(d.table_name) = upper('%s')]')
	FROM
		DUAL ) COLUMNS OWNER VARCHAR2 (300) PATH 'OWNER', TABLE_NAME VARCHAR2(300) PATH 'TABLE_NAME', COLUMN_NAME VARCHAR2(300) PATH 'COLUMN_NAME', DATA_DEFAULT VARCHAR2(4000) PATH 'DATA_DEFAULT') xs
		) s
WHERE
	t.table_name = c.table_name
	AND t.column_name = c.column_name
	AND t.owner = c.owner
	AND c.owner = s.owner
	AND c.table_name = s.table_name
	AND c.column_name = s.column_name
	AND upper(t.owner) = upper('%s')
	AND upper(t.table_name) = upper('%s')
ORDER BY
	t.COLUMN_ID`, schemaName, tableName, schemaName, tableName)
	} else {
		queryStr = fmt.Sprintf(`SELECT 
		t.COLUMN_NAME,
	    t.DATA_TYPE,
		t.CHAR_LENGTH,
		NVL(t.CHAR_USED, 'UNKNOWN') CHAR_USED,
	    NVL(t.DATA_LENGTH, 0) AS DATA_LENGTH,
	    DECODE(NVL(TO_CHAR(t.DATA_PRECISION), '*'), '*', '38', TO_CHAR(t.DATA_PRECISION)) AS DATA_PRECISION,
	    DECODE(NVL(TO_CHAR(t.DATA_SCALE), '*'), '*', '127', TO_CHAR(t.DATA_SCALE)) AS DATA_SCALE,
		t.NULLABLE,
	    NVL(s.DATA_DEFAULT, 'NULLSTRING') DATA_DEFAULT,
	    c.COMMENTS
FROM
	dba_tab_columns t,
	dba_col_comments c,
	(
	SELECT xs.OWNER,xs.TABLE_NAME,xs.COLUMN_NAME,xs.DATA_DEFAULT FROM
	XMLTABLE (
		'/ROWSET/ROW' PASSING (
	SELECT
		DBMS_XMLGEN.GETXMLTYPE (
				q'[SELECT d.OWNER,d.TABLE_NAME,d.COLUMN_NAME,d.DATA_DEFAULT FROM DBA_TAB_COLUMNS d WHERE upper(d.owner) = upper('%s') AND upper(d.table_name) = upper('%s')]')
	FROM
		DUAL ) COLUMNS OWNER VARCHAR2 (300) PATH 'OWNER', TABLE_NAME VARCHAR2(300) PATH 'TABLE_NAME', COLUMN_NAME VARCHAR2(300) PATH 'COLUMN_NAME', DATA_DEFAULT VARCHAR2(4000) PATH 'DATA_DEFAULT') xs
		) s
WHERE
	t.table_name = c.table_name
	AND t.column_name = c.column_name
	AND t.owner = c.owner
	AND c.owner = s.owner
	AND c.table_name = s.table_name
	AND c.column_name = s.column_name
	AND upper(t.owner) = upper('%s')
	AND upper(t.table_name) = upper('%s')
ORDER BY
	t.COLUMN_ID`, schemaName, tableName, schemaName, tableName)
	}

	_, queryRes, err := d.GeneralQuery(queryStr)
	if err != nil {
		return queryRes, err
	}
	if len(queryRes) == 0 {
		return queryRes, fmt.Errorf("oracle table [%s.%s] column info cann't be null", schemaName, tableName)
	}

	// check constraints notnull
	// search_condition long datatype
	_, condRes, err := d.GeneralQuery(fmt.Sprintf(`SELECT
				col.COLUMN_NAME,
				cons.SEARCH_CONDITION
				FROM
				DBA_CONS_COLUMNS col,
				DBA_CONSTRAINTS cons
				WHERE
				col.OWNER = cons.OWNER
				AND col.TABLE_NAME = cons.TABLE_NAME
				AND col.CONSTRAINT_NAME = cons.CONSTRAINT_NAME
				AND cons.CONSTRAINT_TYPE = 'C'
				AND upper(col.OWNER) = '%s'
				AND upper(col.TABLE_NAME) = '%s'`, strings.ToUpper(schemaName), strings.ToUpper(tableName)))
	if err != nil {
		return queryRes, err
	}

	if len(condRes) == 0 {
		return queryRes, nil
	}

	rep, err := regexp.Compile(`(^.*)(?i:IS NOT NULL)`)
	if err != nil {
		return queryRes, fmt.Errorf("check notnull constraint regexp complile failed: %v", err)
	}
	for _, r := range queryRes {
		for _, c := range condRes {
			if r["COLUMN_NAME"] == c["COLUMN_NAME"] && r["NULLABLE"] == "Y" {
				// check constraint -> not null
				if rep.MatchString(c["SEARCH_CONDITION"]) {
					r["NULLABLE"] = "N"
				}
			}
		}
	}
	return queryRes, nil
}

func (d *Database) GetDatabaseTablePrimaryKey(schemaName string, tableName string) ([]map[string]string, error) {
	// for the primary key of an Engine table, you can use the following command to set whether the primary key takes effect.
	// disable the primary key: alter table tableName disable primary key;
	// enable the primary key: alter table tableName enable primary key;
	// primary key status Disabled will not do primary key processing
	queryStr := fmt.Sprintf(`select cu.constraint_name,
       LISTAGG(cu.column_name, ',') WITHIN GROUP(ORDER BY cu.POSITION) AS COLUMN_LIST
  from dba_cons_columns cu, dba_constraints au
 where cu.constraint_name = au.constraint_name
   and au.constraint_type = 'P'
   and au.STATUS = 'ENABLED'
   and cu.owner = au.owner
   and cu.table_name = au.table_name
   and upper(au.table_name) = upper('%s')
   and upper(cu.owner) = upper('%s')
 group by cu.constraint_name`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName))
	_, res, err := d.GeneralQuery(queryStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableUniqueKey(schemaName string, tableName string) ([]map[string]string, error) {
	queryStr := fmt.Sprintf(`select cu.constraint_name,au.index_name,
       LISTAGG(cu.column_name, ',') WITHIN GROUP(ORDER BY cu.POSITION) AS column_list
  from dba_cons_columns cu, dba_constraints au
 where cu.constraint_name = au.constraint_name
   and cu.owner = au.owner
   and cu.table_name = au.table_name
   and au.constraint_type = 'U'
   and au.STATUS = 'ENABLED'
   and upper(au.table_name) = upper('%s')
   and upper(cu.owner) = upper('%s')
 group by cu.constraint_name,au.index_name`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName))
	_, res, err := d.GeneralQuery(queryStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableForeignKey(schemaName string, tableName string) ([]map[string]string, error) {
	queryStr := fmt.Sprintf(`with temp1 as
 (select
         t1.OWNER,
         t1.r_owner,
         t1.constraint_name,
         t1.r_constraint_name,
         t1.DELETE_RULE,
         LISTAGG(a1.column_name, ',') WITHIN GROUP(ORDER BY a1.POSITION) AS COLUMN_LIST
    from dba_constraints t1, dba_cons_columns a1
   where t1.constraint_name = a1.constraint_name
     AND upper(t1.table_name) = upper('%s')
     AND upper(t1.owner) = upper('%s')
     AND t1.STATUS = 'ENABLED'
     AND t1.Constraint_Type = 'R'
   group by t1.OWNER, t1.r_owner, t1.constraint_name, t1.r_constraint_name,t1.DELETE_RULE),
temp2 as
 (select t1.owner,
         t1.TABLE_NAME,
         t1.constraint_name,
         LISTAGG(a1.column_name, ',') WITHIN GROUP(ORDER BY a1.POSITION) AS COLUMN_LIST
    from dba_constraints t1, dba_cons_columns a1
   where t1.constraint_name = a1.constraint_name
     AND upper(t1.owner) = upper('%s')
     AND t1.STATUS = 'ENABLED'
     AND t1.Constraint_Type = 'P'
   group by t1.owner,t1.TABLE_NAME, t1.r_owner, t1.constraint_name)
select x.constraint_name,
       x.COLUMN_LIST,
       x.r_owner,
       y.TABLE_NAME as RTABLE_NAME,
       y.COLUMN_LIST as RCOLUMN_LIST,
       x.DELETE_RULE
  from temp1 x, temp2 y
 where x.r_owner = y.owner
   and x.r_constraint_name = y.constraint_name`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName),
		strings.ToUpper(schemaName))
	_, res, err := d.GeneralQuery(queryStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableCheckKey(schemaName string, tableName string) ([]map[string]string, error) {
	queryStr := fmt.Sprintf(`select cu.constraint_name,au.SEARCH_CONDITION
          from dba_cons_columns cu, dba_constraints au
         where cu.owner=au.owner
           and cu.table_name=au.table_name
           and cu.constraint_name = au.constraint_name
           and au.constraint_type = 'C'
           and au.STATUS = 'ENABLED'
           and upper(au.table_name) = upper('%s')
           and upper(au.owner) = upper('%s')`,
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName),
	)
	_, res, err := d.GeneralQuery(queryStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableNormalIndex(schemaName string, tableName string) ([]map[string]string, error) {
	queryStr := fmt.Sprintf(`SELECT
	temp.TABLE_NAME,
	temp.UNIQUENESS,
	temp.INDEX_NAME,
	temp.INDEX_TYPE,
	temp.ITYP_OWNER,
	temp.ITYP_NAME,
	temp.PARAMETERS,
	LISTAGG ( temp.COLUMN_NAME, ',' ) WITHIN GROUP ( ORDER BY temp.COLUMN_POSITION ) AS COLUMN_LIST 
FROM
	(
SELECT
		T.TABLE_OWNER,
		T.TABLE_NAME,
		I.UNIQUENESS,
		T.INDEX_NAME,
		I.INDEX_TYPE,
		NVL(I.ITYP_OWNER,'') ITYP_OWNER,
		NVL(I.ITYP_NAME,'') ITYP_NAME,
		NVL(I.PARAMETERS,'') PARAMETERS,
		DECODE((SELECT
	COUNT( 1 ) 
FROM
	DBA_IND_EXPRESSIONS S
WHERE
	S.TABLE_OWNER = T.TABLE_OWNER
	AND S.TABLE_NAME = T.TABLE_NAME
	AND S.INDEX_OWNER = T.INDEX_OWNER
	AND S.INDEX_NAME = T.INDEX_NAME
	AND S.COLUMN_POSITION = T.COLUMN_POSITION),0,T.COLUMN_NAME, (SELECT
	xs.COLUMN_EXPRESSION
FROM
	XMLTABLE (
		'/ROWSET/ROW' PASSING ( SELECT DBMS_XMLGEN.GETXMLTYPE ( 
				q'[SELECT
	S.INDEX_OWNER,
	S.INDEX_NAME,
	S.COLUMN_EXPRESSION,
	S.COLUMN_POSITION
FROM
	DBA_IND_EXPRESSIONS S
WHERE
	S.TABLE_OWNER = '%s'
	AND S.TABLE_NAME = '%s']' ) FROM DUAL ) COLUMNS 
	INDEX_OWNER VARCHAR2 ( 30 ) PATH 'INDEX_OWNER',
	INDEX_NAME VARCHAR2 ( 30 ) PATH 'INDEX_NAME',
	COLUMN_POSITION VARCHAR2 ( 30 ) PATH 'COLUMN_POSITION',
	COLUMN_EXPRESSION VARCHAR2 ( 4000 ) 
	) xs 
WHERE
xs.INDEX_OWNER = T.INDEX_OWNER
	AND xs.INDEX_NAME = T.INDEX_NAME
AND xs.COLUMN_POSITION = T.COLUMN_POSITION)) COLUMN_NAME,
		T.COLUMN_POSITION
	FROM
		DBA_IND_COLUMNS T,
		DBA_INDEXES I 
	WHERE t.table_owner=i.table_owner 
		AND t.table_name=i.table_name
		AND t.index_name=i.index_name
		AND i.uniqueness='NONUNIQUE'
	  	AND T.TABLE_OWNER = '%s'
	  	AND T.TABLE_NAME = '%s'
		-- 排除约束索引
		AND not exists (
		select 1 from DBA_CONSTRAINTS C where 
		c.owner=i.table_owner 
		and c.table_name=i.table_name
		and c.index_name=i.index_name)) temp		
	GROUP BY
		temp.TABLE_OWNER,
		temp.TABLE_NAME,
		temp.UNIQUENESS,
		temp.INDEX_NAME,
		temp.INDEX_TYPE,
		temp.ITYP_OWNER,
		temp.ITYP_NAME,
		temp.PARAMETERS`,
		strings.ToUpper(schemaName),
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName),
		strings.ToUpper(tableName))
	_, res, err := d.GeneralQuery(queryStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableUniqueIndex(schemaName string, tableName string) ([]map[string]string, error) {
	queryStr := fmt.Sprintf(`SELECT
	temp.TABLE_NAME,
	temp.UNIQUENESS,
	temp.INDEX_NAME,
	temp.INDEX_TYPE,
	temp.ITYP_OWNER,
	temp.ITYP_NAME,
	temp.PARAMETERS,
	LISTAGG ( temp.COLUMN_NAME, ',' ) WITHIN GROUP ( ORDER BY temp.COLUMN_POSITION ) AS COLUMN_LIST 
FROM
	(
SELECT
	I.TABLE_OWNER,
	I.TABLE_NAME,
	I.UNIQUENESS,
	I.INDEX_NAME,
	I.INDEX_TYPE,
	NVL(I.ITYP_OWNER,'') ITYP_OWNER,
	NVL(I.ITYP_NAME,'') ITYP_NAME,
	NVL(I.PARAMETERS,'') PARAMETERS,
	DECODE((SELECT
	COUNT( 1 ) 
FROM
	DBA_IND_EXPRESSIONS S
WHERE
	S.TABLE_OWNER = T.TABLE_OWNER
	AND S.TABLE_NAME = T.TABLE_NAME
	AND S.INDEX_OWNER = T.INDEX_OWNER
	AND S.INDEX_NAME = T.INDEX_NAME
	AND S.COLUMN_POSITION = T.COLUMN_POSITION),0,T.COLUMN_NAME, (SELECT
	xs.COLUMN_EXPRESSION
FROM
	XMLTABLE (
		'/ROWSET/ROW' PASSING ( SELECT DBMS_XMLGEN.GETXMLTYPE ( 
				q'[SELECT
	S.INDEX_OWNER,
	S.INDEX_NAME,
	S.COLUMN_EXPRESSION,
	S.COLUMN_POSITION
FROM
	DBA_IND_EXPRESSIONS S
WHERE
	S.TABLE_OWNER = '%s'
	AND S.TABLE_NAME = '%s']' ) FROM DUAL ) COLUMNS 
	INDEX_OWNER VARCHAR2 ( 30 ) PATH 'INDEX_OWNER',
	INDEX_NAME VARCHAR2 ( 30 ) PATH 'INDEX_NAME',
	COLUMN_POSITION VARCHAR2 ( 30 ) PATH 'COLUMN_POSITION',
	COLUMN_EXPRESSION VARCHAR2 ( 4000 ) 
	) xs 
WHERE
xs.INDEX_OWNER = T.INDEX_OWNER
	AND xs.INDEX_NAME = T.INDEX_NAME
AND xs.COLUMN_POSITION = T.COLUMN_POSITION)) COLUMN_NAME,
		T.COLUMN_POSITION
FROM
	DBA_INDEXES I,
	DBA_IND_COLUMNS T 
WHERE
	I.INDEX_NAME = T.INDEX_NAME 
	AND I.TABLE_OWNER = T.TABLE_OWNER 
	AND I.TABLE_NAME = T.TABLE_NAME 
	AND I.UNIQUENESS = 'UNIQUE'
	AND I.TABLE_OWNER = '%s' 
	AND I.TABLE_NAME = '%s' 
	-- 排除主键、唯一约束索引
	AND NOT EXISTS (
	SELECT
		1 
	FROM
		DBA_CONSTRAINTS C 
	WHERE
		I.INDEX_NAME = C.INDEX_NAME 
		AND I.TABLE_OWNER = C.OWNER 
		AND I.TABLE_NAME = C.TABLE_NAME 
		AND C.CONSTRAINT_TYPE IN ('P','U')
	)) temp
		GROUP BY
		temp.TABLE_OWNER,
		temp.TABLE_NAME,
		temp.UNIQUENESS,
		temp.INDEX_NAME,
		temp.INDEX_TYPE,
		temp.ITYP_OWNER,
		temp.ITYP_NAME,
		temp.PARAMETERS`,
		strings.ToUpper(schemaName),
		strings.ToUpper(tableName),
		strings.ToUpper(schemaName),
		strings.ToUpper(tableName))
	_, res, err := d.GeneralQuery(queryStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableComment(schemaName string, tableName string) ([]map[string]string, error) {
	var (
		comments []map[string]string
		err      error
	)
	queryStr := fmt.Sprintf(`select table_name,table_type,comments 
from dba_tab_comments 
where 
table_type = 'TABLE'
and upper(owner)=upper('%s')
and upper(table_name)=upper('%s')`, strings.ToUpper(schemaName), strings.ToUpper(tableName))
	_, res, err := d.GeneralQuery(queryStr)
	if err != nil {
		return comments, err
	}
	if len(res) > 1 {
		return res, fmt.Errorf("database schema [%s] table [%s] comments exist multiple values: [%v]", schemaName, tableName, res)
	}

	return res, nil
}

func (d *Database) GetDatabaseTableColumnComment(schemaName string, tableName string) ([]map[string]string, error) {
	var queryStr string

	queryStr = fmt.Sprintf(`select t.COLUMN_NAME,
	    c.COMMENTS
	from dba_tab_columns t, dba_col_comments c
	where t.table_name = c.table_name
	and t.column_name = c.column_name
	and t.owner = c.owner
	and upper(t.owner) = upper('%s')
	and upper(t.table_name) = upper('%s')
	order by t.COLUMN_ID`,
		strings.ToUpper(schemaName),
		strings.ToUpper(tableName))

	_, queryRes, err := d.GeneralQuery(queryStr)
	if err != nil {
		return queryRes, err
	}
	if len(queryRes) == 0 {
		return queryRes, fmt.Errorf("database table [%s.%s] column info cann't be null", schemaName, tableName)
	}
	return queryRes, nil
}

func (d *Database) GetDatabaseSchemaCollation(schemaName string) (string, error) {
	queryStr := fmt.Sprintf(`SELECT DECODE(DEFAULT_COLLATION,
'USING_NLS_COMP',(SELECT VALUE from NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_COMP'),DEFAULT_COLLATION) DEFAULT_COLLATION FROM DBA_USERS WHERE USERNAME = '%s'`, strings.ToUpper(schemaName))
	_, res, err := d.GeneralQuery(queryStr)
	if err != nil {
		return "", err
	}
	return res[0]["DEFAULT_COLLATION"], nil
}

func (d *Database) GetDatabaseSchemaTableCollation(schemaName, schemaCollation string) (map[string]string, error) {
	var err error

	tablesMap := make(map[string]string)

	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT TABLE_NAME,DEFAULT_COLLATION FROM DBA_TABLES WHERE UPPER(owner) = UPPER('%s') AND (IOT_TYPE IS NUll OR IOT_TYPE='IOT')`, schemaName))
	if err != nil {
		return tablesMap, err
	}
	for _, r := range res {
		if strings.ToUpper(r["DEFAULT_COLLATION"]) == constant.OracleDatabaseUserTableColumnDefaultCollation {
			tablesMap[strings.ToUpper(r["TABLE_NAME"])] = strings.ToUpper(schemaCollation)
		} else {
			tablesMap[strings.ToUpper(r["TABLE_NAME"])] = strings.ToUpper(r["DEFAULT_COLLATION"])
		}
	}

	return tablesMap, nil
}

func (d *Database) GetDatabaseTableOriginStruct(schemaName, tableName, tableType string) (string, error) {
	_, err := d.ExecContext(`BEGIN
	DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'SQLTERMINATOR', true);
	DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'PRETTY', true);
	DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', false);
	DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'TABLESPACE', false);
	DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'STORAGE', false);
	END;`)
	if err != nil {
		return "", nil
	}
	ddlSql := fmt.Sprintf(`SELECT REGEXP_REPLACE(REPLACE(DBMS_METADATA.GET_DDL('%s','%s','%s'),'"'), ',\s*''NLS_CALENDAR=GREGORIAN''', '') ORIGIN_DDL FROM DUAL`, tableType, tableName, schemaName)

	rows, err := d.QueryContext(ddlSql)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var ddl string
	for rows.Next() {
		if err = rows.Scan(&ddl); err != nil {
			return ddl, fmt.Errorf("get database schema table origin ddl scan failed: %v", err)
		}
	}

	if err = rows.Close(); err != nil {
		return ddl, fmt.Errorf("get database schema table origin ddl close failed: %v", err)
	}
	if err = rows.Err(); err != nil {
		return ddl, fmt.Errorf("get database schema table origin ddl Err failed: %v", err)
	}

	return ddl, nil
}
