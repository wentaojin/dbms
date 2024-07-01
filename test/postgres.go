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
	"github.com/wentaojin/dbms/database/postgresql"
	"github.com/wentaojin/dbms/model/datasource"
)

func main() {
	ctx := context.Background()
	database, err := postgresql.NewDatabase(ctx, &datasource.Datasource{
		Username:       "dbmsadmin",
		Password:       "dbmsadmin",
		Host:           "120.92.211.117",
		Port:           54322,
		DbName:         "marvin",
		ConnectCharset: "utf8",
	})
	if err != nil {
		panic(err)
	}

	table, err := database.GetDatabasePartitionTable("marvin")
	if err != nil {
		panic(err)
	}
	fmt.Println(table)
}
