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
package datasource

import (
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/wentaojin/dbms/openapi"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type Config struct {
	Datasource []Datasource `json:"datasource"`
}

type Datasource struct {
	DatasourceName string `json:"datasourceName"`
	DbType         int64  `json:"dbType"`
	Username       string `json:"username"`
	Password       string `json:"password"`
	Host           string `json:"host"`
	Port           int64  `json:"port"`
	ConnectCharset string `json:"connectCharset"`
	ConnectParams  string `json:"connectParams"`
	ServiceName    string `json:"serviceName"`
	PdbName        string `json:"pdbName"`
}

func (d *Datasource) String() string {
	jsonStr, _ := stringutil.MarshalJSON(d)
	return jsonStr
}

func Upsert(serverAddr string, file string) error {
	var cfg = &Config{}
	if _, err := toml.DecodeFile(file, cfg); err != nil {
		return fmt.Errorf("failed decode toml config file %s: %v", file, err)
	}

	for _, ds := range cfg.Datasource {
		_, err := openapi.Request(openapi.RequestPUTMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APIDatasourcePath), []byte(ds.String()))
		if err != nil {
			return err
		}
	}
	return nil
}

func Delete(serverAddr string, name string) error {
	_, err := openapi.Request(openapi.RequestDELETEMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APIDatasourcePath), []byte(name))
	if err != nil {
		return err
	}
	return nil
}

func Get(serverAddr string, name string) (string, error) {
	resp, err := openapi.Request(openapi.RequestGETMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APIDatasourcePath), []byte(name))
	if err != nil {
		return "", err
	}

	jsonStr, err := stringutil.MarshalIndentJSON(resp)
	if err != nil {
		return "", err
	}

	return jsonStr, nil
}
