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
	"fmt"
	"reflect"

	"github.com/wentaojin/dbms/utils/stringutil"
)

func main() {
	var param *StructMigrateParam
	param = &StructMigrateParam{
		unknownFields: "1",
		MigrateThread: 10,
	}
	fmt.Println(stringutil.GetJSONTagFieldValue(param))
}

type StructMigrateParam struct {
	unknownFields string

	MigrateThread uint64 `protobuf:"varint,2,opt,name=migrateThread,proto3" json:"migrateThread,omitempty"`
}

// GetFieldInfoWithJSONTag used for get field info with json tag
func GetFieldInfoWithJSONTag(obj interface{}) map[string]string {
	objValue := reflect.ValueOf(obj)
	fieldInfo := make(map[string]string)
	if objValue.Kind() == reflect.Ptr {
		objValue = objValue.Elem() // Dereference the pointer
	}

	objType := objValue.Type()

	for i := 0; i < objValue.NumField(); i++ {
		field := objValue.Field(i)
		fieldType := objType.Field(i)
		jsonTag := fieldType.Tag.Get("json")

		if jsonTag != "" {
			fieldName := fieldType.Name
			fieldValue := field.String()
			fieldInfo[fieldName] = fieldValue
		}
	}
	return fieldInfo
}
