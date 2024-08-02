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
	"github.com/wentaojin/dbms/database/mysql"
	"github.com/wentaojin/dbms/model/datasource"
)

func main() {
	ctx := context.Background()
	databaseS, err := mysql.NewDatabase(ctx, &datasource.Datasource{
		DbType:         "tidb",
		Username:       "root",
		Password:       "J^S",
		Host:           "10.2.10.16",
		Port:           5000,
		ConnectCharset: "utf8mb4",
	}, 600)
	if err != nil {
		panic(err)
	}

	//	var s []interface{}
	//	s[0] = "99716061760-07866897534-10853623002-16757842511-44400616623-64918258778-84747634712-26582176156-80045723441-68102414194"
	//	s[1] = "99999019891-32672521763-04429715883-01213059055-80583980995-54942792772-84177537049-62866496817-55325374451-52763126606"
	//rows, err := databaseS.QueryContext(context.Background(), "SELECT c1, c2 FROM t99 WHERE CONVERT(c1,'AL32UTF8','ZHS16GBK') < CONVERT('卡','AL32UTF8','ZHS16GBK')")
	//if err != nil {
	//	panic(err)
	//}
	//
	//for rows.Next() {
	//	var c1, c2 string
	//	err = rows.Scan(&c1, &c2)
	//	if err != nil {
	//		panic(err)
	//	}
	//	fmt.Println(c1, c2)
	//}
	//
	//err = rows.Err()
	//if err != nil {
	//	panic(err)
	//}

	_, res, err := databaseS.GeneralQuery("SELECT 1 FROM DUAL")
	if err != nil {
		panic(err)
	}

	fmt.Println(res)
}
