/*
Copyright �0�8 2020 Marvin

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
package main

import (
	"context"
	"fmt"

	"github.com/Pallinder/go-randomdata"
	"github.com/thanhpk/randstr"
	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/model/datasource"
)

func main() {
	ctx := context.Background()

	databaseS, err := database.NewDatabase(ctx, &datasource.Datasource{
		DbType:         "oracle",
		Username:       "findpt",
		Password:       "findpt",
		Host:           "10.2.103.33",
		Port:           1521,
		ConnectCharset: "zhs16gbk",
		ServiceName:    "gbk",
	}, "MARVIN")
	if err != nil {
		panic(err)
	}

	//_, err = databaseS.ExecContext(ctx, `CREATE TABLE MARVIN.MARVIN_COLUMN_T(
	//N1 NUMBER,
	//N2 NUMBER(2),
	//N3 NUMBER(4),
	//N4 NUMBER(8),
	//N5 NUMBER(12,0),
	//N6 NUMBER(13),
	//N7 NUMBER(30),
	//N8 NUMBER(30,2),
	//N9 NUMERIC(10,2),
	//N10 NUMERIC(10),
	//NBFILE BFILE,
	//VCHAR1 VARCHAR(10),
	//VCHAR2 VARCHAR(3000),
	//VCHAR3 VARCHAR2(10),
	//VCHAR4 VARCHAR2(3000),
	//CHAR1 CHAR(23),
	//CHAR2 CHAR(300),
	//CHAR3 CHARACTER(23),
	//CHAR4 CHARACTER(300),
	//CHAR5 NCHAR(23),
	//CHAR6 NCHAR(300),
	//CHAR7 NCHAR VARYING(10),
	//CHAR8 NCHAR VARYING(300),
	//CHAR9 NVARCHAR2(10),
	//CHAR10 NVARCHAR2(300),
	//DLOB CLOB,
	//CFLOB NCLOB,
	//NDATE DATE,
	//NDECIMAL1 DECIMAL,
	//NDECIMAL2 DECIMAL(10,2),
	//NDECIMAL3 DEC(10,2),
	//NDECIMAL4 DEC,
	//DP1 DOUBLE PRECISION,
	//FP1 FLOAT(2),
	//FP2 FLOAT,
	//FY2 INTEGER,
	//FY4 INT,
	//FY5 SMALLINT,
	//YT INTERVAL YEAR(5) TO MONTH,
	//YU INTERVAL DAY(6) TO SECOND(3),
	//HP LONG RAW ,
	//RW1 RAW(10),
	//RW2 RAW(300),
	//RL REAL,
	//RD1 ROWID,
	//RD2 UROWID(100),
	//TP1 TIMESTAMP,
	//TP2 TIMESTAMP(3),
	//TP3 TIMESTAMP(5),
	//TP4 TIMESTAMP(5) WITH TIME ZONE,
	//XT XMLTYPE
	//)`)
	//if err != nil {
	//	panic(err)
	//}

	_, err = databaseS.ExecContext(ctx, `CREATE OR REPLACE DIRECTORY BFILEDIR AS '/tmp/bfile'`)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10000; i++ {
		sqlStr := fmt.Sprintf(`INSERT INTO MARVIN.MARVIN_COLUMN_T (N1,N2,N3,N4,N5,N6,N7,N8,N9,N10,NBFILE,VCHAR1,VCHAR2,VCHAR3,VCHAR4,CHAR1,CHAR2,CHAR3,CHAR4,CHAR5,CHAR6,CHAR7,CHAR8,CHAR9,CHAR10,DLOB,CFLOB,NDATE,NDECIMAL1,NDECIMAL2,NDECIMAL3,NDECIMAL4,DP1,FP1,FP2,FY2,FY4,FY5,YT,YU,HP,RW1,RW2,RL,RD1,RD2,TP1,TP2,TP3,TP4,XT) VALUES (%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,'%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v',%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v)`,
			randomdata.Number(1, 1000000),
			randomdata.Number(1, 99),
			randomdata.Number(1, 9999),
			randomdata.Number(1, 99999999),
			randomdata.Number(1, 999999999999),
			randomdata.Number(1, 9999999999999),
			randomdata.Number(1, 1000000),
			randomdata.Decimal(0, 5000000, 2),
			randomdata.Decimal(0, 5000000, 2),
			randomdata.Number(1, 1000000),
			"BFILENAME('BFILEDIR', 'mb.txt')",
			randstr.String(10),
			randstr.String(3000),
			randstr.String(10),
			randstr.String(3000),
			randstr.String(23),
			randstr.String(300),
			randstr.String(23),
			randstr.String(300),
			randstr.String(23),
			randstr.String(300),
			randstr.String(10),
			randstr.String(300),
			randstr.String(10),
			randstr.String(300),
			fmt.Sprintf("to_clob('%v')", randstr.String(3000)),
			fmt.Sprintf("to_clob('%v')", randstr.String(3000)),
			"to_date(sysdate,'yyyy-mm-dd hh24:mi:ss')",
			randomdata.Decimal(1, 100),
			randomdata.Decimal(0, 5000000, 2),
			randomdata.Decimal(0, 5000000, 2),
			randomdata.Decimal(1, 1000),
			randomdata.Decimal(0, 5000000, 2),
			randomdata.Decimal(0, 5000000, 2),
			randomdata.Decimal(0, 5000000, 2),
			randomdata.Number(1, 1000),
			randomdata.Number(1, 1000),
			randomdata.Number(1, 1000),
			"INTERVAL '10-2' YEAR TO MONTH",
			"INTERVAL '2 3:04:11.333' DAY TO SECOND(3)",
			fmt.Sprintf("UTL_RAW.CAST_TO_RAW('%v')", randstr.String(1000)),
			fmt.Sprintf("UTL_RAW.CAST_TO_RAW('%v')", randstr.String(10)),
			fmt.Sprintf("UTL_RAW.CAST_TO_RAW('%v')", randstr.String(300)),
			randomdata.Decimal(1, 1000, 4),
			"NULL",
			"NULL",
			"systimestamp",
			"systimestamp",
			"systimestamp",
			"TIMESTAMP '2001-12-01 15:00:00.000000 UTC'",
			"xmltype(\n            '<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n            <list_book>\n                <book>1</book>\n                <book>2</book>\n                <book>3</book>\n            </list_book>'\n          )")

		_, err = databaseS.ExecContext(ctx, sqlStr)
		if err != nil {
			panic(err)
		}
	}
}
