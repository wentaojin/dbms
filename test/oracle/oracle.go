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
	"math/rand"
	"time"

	"github.com/wentaojin/dbms/database/oracle"
	"github.com/wentaojin/dbms/model/datasource"
	"golang.org/x/sync/errgroup"
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

	// var sx []interface{}
	// sx = append(sx, "test-2    ")

	// _, err = d.ExecContext(ctx, `DELETE FROM "MARVIN"."C_CLUSTERED_T2" WHERE A = :1`, sx...)
	// if err != nil {
	// 	panic(err)
	// }

	g := &errgroup.Group{}
	g.SetLimit(1024)

	stime := time.Now()
	for i := 1; i < 1000000; i++ {
		g.Go(func() error {
			if i >= 60159 && i <= 66000 {
				// ascii
				if _, err := d.ExecContext(ctx, fmt.Sprintf(`INSERT INTO MARVIN.PINGAN (ID,COL1,COL2) VALUES (%d,CHR(%d),'%s')`, i, i, "normal_col2")); err != nil {
					return err
				}
			} else if i >= 200 && i <= 210 {
				// garbled
				invalidGBKData := generateInvalidGBKData(10)
				if _, err := d.ExecContext(ctx, fmt.Sprintf(`INSERT INTO MARVIN.PINGAN (ID,COL1,COL2) VALUES (%d,'%s','%s')`, i, "normal_col1", string(invalidGBKData))); err != nil {
					return err
				}
			} else {
				if _, err := d.ExecContext(ctx, fmt.Sprintf(`INSERT INTO MARVIN.PINGAN (ID,COL1,COL2) VALUES (%d,'%s','%s')`, i, "nromal_col1", "nromal_col2")); err != nil {
					return err
				}
			}
			fmt.Printf("successlly inserted sequence [%d], continue....\n", i)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		panic(err)
	}
	fmt.Printf("completed successully inserted, finished in %fs.\n", time.Since(stime).Seconds())
}

func init() {
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())
}

// generateInvalidGBKData generates a byte slice that contains invalid GBK encoding.
// This ensures the data cannot be correctly decoded as GBK and will appear as "garbled".
func generateInvalidGBKData(length int) []byte {
	data := make([]byte, length)
	for i := 0; i < length; {
		if rand.Intn(2) == 0 {
			// Generate a single-byte value that is not valid in GBK (not ASCII)
			// Valid ASCII is 0x00 to 0x7F; we avoid it to create garbage
			b := byte(rand.Intn(256))
			for b <= 0x7F {
				b = byte(rand.Intn(256))
			}
			data[i] = b
			i++
		} else {
			// Generate an invalid GBK double-byte sequence
			// First byte should NOT be in 0x81 - 0xFE (valid GBK first byte)
			first := byte(rand.Intn(256))
			for first >= 0x81 && first <= 0xFE {
				first = byte(rand.Intn(256))
			}
			data[i] = first
			i++

			if i < length {
				// Second byte should NOT be in valid GBK second byte ranges:
				// Not in 0x40 - 0x7E or 0x80 - 0xFE
				second := byte(rand.Intn(256))
				for (second >= 0x40 && second <= 0x7E) || (second >= 0x80 && second <= 0xFE) {
					second = byte(rand.Intn(256))
				}
				data[i] = second
				i++
			}
		}
	}
	return data
}
