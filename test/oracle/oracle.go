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
	"github.com/wentaojin/dbms/utils/constant"
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
	dataC := make(chan []interface{})

	go func() {
		if err := d.GetDatabaseTableNonStmtData("ORACLE@TIDB", `select * from marvin01`, nil, 10, 30, constant.CharsetGBK, constant.CharsetUTF8MB4, "id,v00,v01", dataC); err != nil {
			panic(err)
		}
		close(dataC)
	}()

	for c := range dataC {
		fmt.Println(c)
	}
}
