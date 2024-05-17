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
package cluster

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"strings"

	"github.com/wentaojin/dbms/utils/stringutil"

	"gopkg.in/yaml.v2"
)

// ReadYamlFile read yaml content from file`
func ReadYamlFile(file string) ([]byte, error) {
	yamlFile, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read topology yaml file [%s] failed: %v", file, err)
	}
	return yamlFile, nil
}

// ParseTopologyYaml read yaml content from `file` and unmarshal it to `out`
// ignoreGlobal ignore global variables in file, only ignoreGlobal with a index of 0 is effective
func ParseTopologyYaml(file string, out *Topology) error {
	yamlFile, err := ReadYamlFile(file)
	if err != nil {
		return err
	}

	if err = yaml.UnmarshalStrict(yamlFile, out); err != nil {
		return fmt.Errorf("please check the syntax of your topology file and try again, parse topology yaml file [%s] failed: %v, ", file, err)
	}
	return nil
}

// ExpandRelativeDir fill DeployDir, DataDir and LogDir to absolute path
func ExpandRelativeDir(topo *Topology) {
	expandRelativePath(deployUser(topo), topo)
}

func expandRelativePath(user string, topo any) {
	v := reflect.Indirect(reflect.ValueOf(topo).Elem())

	switch v.Kind() {
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			ref := reflect.New(v.Index(i).Type())
			ref.Elem().Set(v.Index(i))
			expandRelativePath(user, ref.Interface())
			v.Index(i).Set(ref.Elem())
		}
	case reflect.Struct:
		// We should deal with DeployDir first, because DataDir and LogDir depends on it
		dirs := []string{"DeployDir", "DataDir", "LogDir"}
		for _, dir := range dirs {
			f := v.FieldByName(dir)
			if !f.IsValid() || f.String() == "" {
				continue
			}
			switch dir {
			case "DeployDir":
				f.SetString(stringutil.Abs(user, f.String()))
			case "DataDir":
				// Some components supports multiple data dirs split by comma
				ds := strings.Split(f.String(), ",")
				ads := []string{}
				for _, d := range ds {
					if strings.HasPrefix(d, "/") {
						ads = append(ads, d)
					} else {
						ads = append(ads, path.Join(v.FieldByName("DeployDir").String(), d))
					}
				}
				f.SetString(strings.Join(ads, ","))
			case "LogDir":
				if !strings.HasPrefix(f.String(), "/") {
					f.SetString(path.Join(v.FieldByName("DeployDir").String(), f.String()))
				}
			}
		}
		// Deal with all fields (expandRelativePath will do nothing on string filed)
		for i := 0; i < v.NumField(); i++ {
			// We don't deal with GlobalOptions because relative path in GlobalOptions.Data has special meaning
			if v.Type().Field(i).Name == "GlobalOptions" {
				continue
			}
			ref := reflect.New(v.Field(i).Type())
			ref.Elem().Set(v.Field(i))
			expandRelativePath(user, ref.Interface())
			v.Field(i).Set(ref.Elem())
		}
	case reflect.Ptr:
		expandRelativePath(user, v.Interface())
	}
}

func deployUser(topo *Topology) string {
	if reflect.DeepEqual(topo.GlobalOptions, GlobalOptions{}) || topo.GlobalOptions.User == "" {
		return DefaultDeployUser
	}
	return topo.GlobalOptions.User
}
