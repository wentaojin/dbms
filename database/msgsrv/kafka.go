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
package msgsrv

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func NewKafkaConn(ctx context.Context, addrs []string) (*kafka.Conn, error) {
	for i, addr := range addrs {
		conn, err := kafka.Dial("tcp", addr)
		if err == nil {
			return conn, nil
		}
		if i == len(addrs)-1 {
			return conn, err
		}
		// continue retring
		continue
	}
	return nil, fmt.Errorf("the kafka connection ping lost, please check the connectivity and whether there is any problem with the network address [%v] configuration", stringutil.StringJoin(addrs, constant.StringSeparatorComma))
}
