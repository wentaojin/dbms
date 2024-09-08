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
	"github.com/wentaojin/dbms/utils/etcdutil"
)

func main() {
	cli, err := etcdutil.CreateClient(context.Background(), []string{
		"192.168.0.101:2379",
	}, nil)
	if err != nil {
		panic(err)
	}

	member, err := etcdutil.StatusMember(cli)
	if err != nil {
		panic(err)
	}
	fmt.Println(member.Leader)
	leader, err := etcdutil.GetClusterLeader(cli)
	if err != nil {
		panic(err)
	}
	fmt.Println(leader)
}
