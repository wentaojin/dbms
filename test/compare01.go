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
package main

import (
	"context"
	"fmt"
	"maps"

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

	databaseT, err := database.NewDatabase(ctx, &datasource.Datasource{
		DbType:         "tidb",
		Username:       "root",
		Password:       "",
		Host:           "120.92.77.145",
		Port:           4000,
		ConnectCharset: "utf8mb4",
	}, "")
	if err != nil {
		panic(err)
	}

	_, crc32S, dataS, err := databaseS.GetDatabaseTableCompareData(`SELECT "N1","N2","N3","N4","N5","N6","N7","N8","N9","N10","NBFILE","VCHAR1","VCHAR2","VCHAR3","VCHAR4","CHAR1","CHAR2","CHAR3","CHAR4","CHAR5","CHAR6","CHAR7","CHAR8","CHAR9","CHAR10","DLOB","CFLOB",TO_CHAR("NDATE",'yyyy-mm-dd hh24:mi:ss') AS "NDATE","NDECIMAL1","NDECIMAL2","NDECIMAL3","NDECIMAL4","DP1","FP1","FP2","FY2","FY4","FY5",TO_CHAR("YT") AS "YT",TO_CHAR("YU") AS "YU","HP","RW1","RW2","RL","RD1","RD2",TO_CHAR("TP1",'yyyy-mm-dd hh24:mi:ss.ff6') AS "TP1",TO_CHAR("TP2",'yyyy-mm-dd hh24:mi:ss.ff6') AS "TP2",TO_CHAR("TP3",'yyyy-mm-dd hh24:mi:ss.ff6') AS "TP3",TO_CHAR("TP4",'yyyy-mm-dd hh24:mi:ss.ff6') AS "TP4" FROM MARVIN.MARVIN_COLUMN_T WHERE (("N1" > 858244) OR ("N1" = 858244 AND "TP1" > TO_TIMESTAMP('2024-03-21 18:23:06','YYYY-MM-DD HH24:MI:SS.FF6'))) AND (("N1" < 864981) OR ("N1" = 864981 AND "TP1" <= TO_TIMESTAMP('2024-03-21 18:24:14','YYYY-MM-DD HH24:MI:SS.FF6')))`, 36000, "GBK", "UTF8MB4")
	if err != nil {
		panic(err)
	}

	_, crc32T, dataT, err := databaseT.GetDatabaseTableCompareData("SELECT `N1`,`N2`,`N3`,`N4`,`N5`,`N6`,`N7`,`N8`,`N9`,`N10`,`NBFILE`,`VCHAR1`,`VCHAR2`,`VCHAR3`,`VCHAR4`,`CHAR1`,`CHAR2`,`CHAR3`,`CHAR4`,`CHAR5`,`CHAR6`,`CHAR7`,`CHAR8`,`CHAR9`,`CHAR10`,`DLOB`,`CFLOB`,`NDATE`,`NDECIMAL1`,`NDECIMAL2`,`NDECIMAL3`,`NDECIMAL4`,`DP1`,`FP1`,`FP2`,`FY2`,`FY4`,`FY5`,`YT`,`YU`,`HP`,`RW1`,`RW2`,`RL`,`RD1`,`RD2`,`TP1`,`TP2`,`TP3`,`TP4` FROM STEVEN.MARVIN_COLUMN_T WHERE ((`N1` > 858244) OR (`N1` = 858244 AND `TP1` > '2024-03-21 18:23:06')) AND ((`N1` < 864981) OR (`N1` = 864981 AND `TP1` <= '2024-03-21 18:24:14'))", 36000, "UTF8MB4", "UTF8MB4")
	if err != nil {
		panic(err)
	}
	fmt.Println(crc32S, crc32T)
	s1, s2 := Cmp(dataT, dataS)
	fmt.Println(s1)
	fmt.Println(s2)
}

// Cmp used for the src and dest store key-value pair , and key data row and value data row counts
func Cmp(src map[string]int64, dest map[string]int64) (map[string]int64, map[string]int64) {
	// source element map mall
	columnDataMall := maps.Clone(src) // source and target element map temp malls

	addedSrcSets := make(map[string]int64)
	delSrcSets := make(map[string]int64)

	// data intersection
	intersectionSets := make(map[string]int64)

	// data comparison through set operations
	for dk, dv := range dest {
		if val, exist := columnDataMall[dk]; exist {
			if dv == val {
				intersectionSets[dk] = dv
			} else if dv < val {
				// target records is less than source records
				delSrcSets[dk] = val - dv
				delete(src, dk)
				delete(columnDataMall, dk)
			} else {
				// target records is more than source records
				addedSrcSets[dk] = dv - val
				delete(src, dk)
				delete(columnDataMall, dk)
			}
		} else {
			columnDataMall[dk] = dv
		}
	}

	for dk, _ := range intersectionSets {
		delete(columnDataMall, dk)
	}

	for dk, dv := range columnDataMall {
		if _, exist := src[dk]; exist {
			delSrcSets[dk] = dv
		} else {
			addedSrcSets[dk] = dv
		}
	}
	return addedSrcSets, delSrcSets
}
