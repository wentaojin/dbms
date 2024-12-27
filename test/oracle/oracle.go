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

	"github.com/wentaojin/dbms/database/oracle"
	"github.com/wentaojin/dbms/model/datasource"
)

func main() {
	ctx := context.Background()
	d, err := oracle.NewDatabase(ctx, &datasource.Datasource{
		DatasourceName: "oracle",
		DbType:         "oracle",
		Username:       "scto",
		Password:       "tger",
		Host:           "192.18.19.151",
		Port:           1521,
		ServiceName:    "ifdb",
		ConnectCharset: "zhs16gbk",
	}, "scott", 300)
	if err != nil {
		panic(err)
	}
	// var s []interface{}
	// s = append(s, -95400)
	// s = append(s, "2020-02-20 02:20:20")
	// s = append(s, 2020)
	// s = append(s, 2)
	// s = append(s, "2020-02-20")
	// s = append(s, "2020-02-20 02:20:20")

	// stmt, err := d.PrepareContext(ctx, `INSERT INTO "MARVIN"."C_TIME" ("C_TIME","C_TIMESTAMP","C_YEAR","ID","C_DATE","C_DATETIME") VALUES (NUMTODSINTERVAL(:C_TIME, 'SECOND'),
	// TO_TIMESTAMP(:C_TIMESTAMP,'YYYY-MM-DD HH24:MI:SS.FF6'),:C_YEAR,:ID,TO_DATE(:C_DATE,'YYYY-MM-DD HH24:MI:SS'),TO_TIMESTAMP(:C_DATETIME,'YYYY-MM-DD HH24:MI:SS.FF6'))`)
	// if err != nil {
	// 	panic(err)
	// }
	// defer stmt.Close()
	// _, err = stmt.ExecContext(ctx, s...)
	// if err != nil {
	// 	panic(err)
	// }

	var sx []interface{}
	sx = append(sx, "test-2    ")

	_, err = d.ExecContext(ctx, `DELETE FROM "MARVIN"."C_CLUSTERED_T2" WHERE A = :1`, sx...)
	if err != nil {
		panic(err)
	}
}
