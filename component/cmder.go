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
package component

import (
	"reflect"

	"github.com/spf13/cobra"
)

// Refer: https://github.com/yaegashi/cobra-cmder
type Cmder interface {
	Cmd() *cobra.Command
}

func Cmd(c Cmder) *cobra.Command {
	return recCmd(c, map[string]bool{})
}

// outTypes is a constant for the array of function output types
var outTypes = []reflect.Type{reflect.TypeOf((*Cmder)(nil)).Elem()}

func recCmd(c Cmder, mmap map[string]bool) *cobra.Command {
	cmd := c.Cmd()
	inV := reflect.ValueOf(c)
	inT := reflect.TypeOf(c)
	funcT := reflect.FuncOf([]reflect.Type{inT}, outTypes, false)
	var methods []reflect.Method
	for i := 0; i < inT.NumMethod(); i++ {
		m := inT.Method(i)
		if m.Func.Type() != funcT || mmap[m.Name] {
			continue
		}
		methods = append(methods, m)
	}
	for _, m := range methods {
		mmap[m.Name] = true
	}
	for _, m := range methods {
		subC := m.Func.Call([]reflect.Value{inV})[0].Interface().(Cmder)
		subCmd := recCmd(subC, mmap)
		cmd.AddCommand(subCmd)
	}
	for _, m := range methods {
		mmap[m.Name] = false
	}
	return cmd
}
