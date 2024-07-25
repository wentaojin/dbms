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
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"os"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/model/datasource"
)

func main() {
	ctx := context.Background()

	databaseS, err := database.NewDatabase(ctx, &datasource.Datasource{
		DbType:         "oracle",
		Username:       "finpt",
		Password:       "finpt",
		Host:           "10.21.10.33",
		Port:           1521,
		ConnectCharset: "zhs16gbk",
		ServiceName:    "gbk",
	}, "finddpt", 600)
	if err != nil {
		panic(err)
	}

	//err = writeIMG(ctx, databaseS, "/Users/marvin/Downloads/alishan-2136879_1280.jpg")
	//if err != nil {
	//	panic(err)
	//}
	//err = SaveIMG(ctx, databaseS, "/Users/marvin/gostore/dbms/test/alishan-2136879_1280.jpg")
	//if err != nil {
	//	panic(err)
	//}
	//
	//err = queryBin(ctx, databaseS)
	//if err != nil {
	//	panic(err)
	//}

	databaseT, err := database.NewDatabase(ctx, &datasource.Datasource{
		DbType:   "mysql",
		Username: "root",
		Password: "",
		Host:     "120.92.77.145",
		Port:     4000,
	}, "", 600)
	if err != nil {
		panic(err)
	}
	err = writeDB(ctx, databaseS, databaseT)
	if err != nil {
		panic(err)
	}

	//err = SaveIMG(my.MySQLDB, "/Users/marvin/gostore/transferdb/test/202312006.jpeg")
	//if err != nil {
	//	panic(err)
	//}
}

func writeIMG(ctx context.Context, database database.IDatabase, imagePath string) error {
	// Open the image file
	imageFile, err := os.Open(imagePath)
	if err != nil {
		return fmt.Errorf("Error opening image file:", err)
	}
	defer imageFile.Close()

	// Read the entire file content into a byte slice
	imageBytes, err := io.ReadAll(imageFile)
	if err != nil {
		return fmt.Errorf("Error reading image file:", err)
	}

	// Prepare the insert statement
	insertSQL := "INSERT INTO marvin03_blob (id, bpeg,n) VALUES (:1, :2, :3)"

	_, err = database.ExecContext(ctx, insertSQL, 2, imageBytes, "kwp")
	if err != nil {
		return fmt.Errorf("Error inserting image data:", err)
	}

	fmt.Printf("Image data successfully inserted into the database\n")
	return nil
}

func SaveIMG(ctx context.Context, database database.IDatabase, outImgFile string) error {
	//_, res, err := oracle.Query(context.Background(), db, `SELECT b FROM marvin_blob`)
	//if err != nil {
	//	return err
	//}

	var imageBytes []byte
	rows, err := database.QueryContext(ctx, `SELECT bpeg FROM marvin.marvin_blob WHERE id=1`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&imageBytes)
		if err != nil {
			return err
		}
		img, _, err := image.Decode(bytes.NewReader(imageBytes))
		if err != nil {
			return err
		}
		out, _ := os.Create(outImgFile)
		defer out.Close()

		err = jpeg.Encode(out, img, nil)
		if err != nil {
			return err
		}

		fmt.Printf("Image data successfully success into the filename [%v]\n", outImgFile)
	}

	return nil
}

func writeDB(ctx context.Context, databaseS database.IDatabase, databaseT database.IDatabase) error {
	//_, rows, err := oracle.Query(ctx, db, `SELECT id,b FROM marvin_blob WHERE ID=1`)
	//if err != nil {
	//	return err
	//}
	//for _, r := range rows {
	//	_, err = my.Exec(fmt.Sprintf("INSERT INTO marvin.marvin_blob VALUES ('%v',X'%v')", r["ID"], r["B"]))
	//	if err != nil {
	//		return err
	//	}
	//}

	rows, err := databaseS.QueryContext(ctx, `SELECT id,bpeg,n FROM marvin03_blob`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			idBytes  []byte
			blobData []byte
			nData    []byte
		)
		err = rows.Scan(&idBytes, &blobData, &nData)
		if err != nil {
			return err
		}

		_, err = databaseT.ExecContext(ctx, "INSERT INTO marvin.marvin03_blob (id,bpeg,n) VALUES (?,?,?)", idBytes, blobData, nData)
		if err != nil {
			return fmt.Errorf("Error inserting BLOB data:", err)
		}
	}

	return nil
}

func queryBin(ctx context.Context, databaseS database.IDatabase) error {
	_, rs, err := databaseS.GeneralQuery(`SELECT * FROM findpt.marvin03_blob`)
	if err != nil {
		return err
	}
	fmt.Println(rs)
	return nil
}
