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
package processor

import (
	"fmt"
	"github.com/wentaojin/dbms/utils/constant"
	"strings"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type Datasource struct {
	DBTypeS     string             `json:"dbTypeS"`
	DBVersionS  string             `json:"dbVersionS"`
	DatabaseS   database.IDatabase `json:"-"`
	SchemaNameS string             `json:"schemaNameS"`
	TableNameS  string             `json:"tableNameS"`
	TableTypeS  string             `json:"tableTypeS"`
}

func (d *Datasource) GetTablePrimaryKey() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTablePrimaryKey(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableUniqueKey() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableUniqueKey(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableForeignKey() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableForeignKey(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableCheckKey() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableCheckKey(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableUniqueIndex() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableUniqueIndex(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableNormalIndex() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableNormalIndex(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableComment() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableComment(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableColumns() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableColumnInfo(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableColumnComment() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableColumnComment(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableCharsetCollation() (string, string, error) {
	charset, err := d.DatabaseS.GetDatabaseCharset()
	if err != nil {
		return "", "", err
	}

	switch stringutil.StringUpper(d.DBTypeS) {
	case constant.DatabaseTypeOracle:
		if stringutil.VersionOrdinal(d.DBVersionS) >= stringutil.VersionOrdinal(constant.OracleDatabaseTableAndColumnSupportVersion) {
			tableCollation, err := d.DatabaseS.GetDatabaseTableCollation(d.SchemaNameS, d.TableNameS)
			if err != nil {
				return "", "", err
			}
			if strings.EqualFold(tableCollation, "") {
				schemaCollation, err := d.DatabaseS.GetDatabaseSchemaCollation(d.SchemaNameS)
				if err != nil {
					return "", "", err
				}
				return charset, schemaCollation, nil
			}
			return charset, tableCollation, nil
		} else {
			nlsComp, err := d.DatabaseS.GetDatabaseCollation()
			if err != nil {
				return "", "", err
			}
			return charset, nlsComp, nil
		}
	default:
		return charset, "", fmt.Errorf("the struct migrate task unsupported database type: %s", d.DBTypeS)
	}
}

func (d *Datasource) GetTableOriginStruct() (string, error) {
	return d.DatabaseS.GetDatabaseTableOriginStruct(d.SchemaNameS, d.TableNameS, d.TableTypeS)
}

func (d *Datasource) String() string {
	jsonStr, _ := stringutil.MarshalJSON(d)
	return jsonStr
}
