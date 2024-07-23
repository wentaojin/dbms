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
	"github.com/wentaojin/dbms/database/oracle"

	"github.com/wentaojin/dbms/model/datasource"
)

func main() {
	ctx := context.Background()
	databaseS, err := oracle.NewDatabase(ctx, &datasource.Datasource{
		DbType:         "oracle",
		Username:       "marvin",
		Password:       "marvin",
		Host:           "120.922.21.117",
		Port:           31521,
		ConnectCharset: "al32utf8",
		ServiceName:    "jem10g",
	}, "marvin")
	if err != nil {
		panic(err)
	}

	var s []interface{}
	s[0] = "99716061760-07866897534-10853623002-16757842511-44400616623-64918258778-84747634712-26582176156-80045723441-68102414194"
	s[1] = "99999019891-32672521763-04429715883-01213059055-80583980995-54942792772-84177537049-62866496817-55325374451-52763126606"
	rows, err := databaseS.QueryContext(context.Background(), `SELECT id FROM sbtest.sbtest1 WHERE ((NLSSORT("c",'NLS_SORT = al32utf8') > NLSSORT(:c,'NLS_SORT = al32utf8'))) AND ((NLSSORT("c",'NLS_SORT = al32utf8') <= NLSSORT(:c,'NLS_SORT = al32utf8')))`, s)
	if err != nil {
		panic(err)
	}

	var ids []int
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		if err != nil {
			panic(err)
		}
		ids = append(ids, id)
	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

	fmt.Println(ids)
}
