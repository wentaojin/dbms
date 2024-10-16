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
package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/golang/snappy"
	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/etcdutil"
	"github.com/wentaojin/dbms/utils/stringutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type CompareDecrypt struct {
	ChunkID      string `json:"chunkID"`
	SchemaNameS  string `json:"schemaNameS"`
	TableNameS   string `json:"tableNameS"`
	SchemaNameT  string `json:"schemaNameT"`
	TableNameT   string `json:"tableNameT"`
	ChunkDetailS string `json:"chunkDetailS"`
	ChunkArgsS   string `json:"chunkArgsS"`
	ChunkDetailT string `json:"chunkDetailT"`
	ChunkArgsT   string `json:"chunkArgsT"`
	TaskStatus   string `json:"taskStatus"`
}

type MigrateDecrypt struct {
	ChunkID       string `json:"chunkID"`
	SchemaNameS   string `json:"schemaNameS"`
	TableNameS    string `json:"tableNameS"`
	SchemaNameT   string `json:"schemaNameT"`
	TableNameT    string `json:"tableNameT"`
	ChunkDetailS  string `json:"chunkDetailS"`
	ChunkArgsS    string `json:"chunkArgsS"`
	ColumnDetailS string `json:"columnDetailS"`
	ColumnDetailT string `json:"columnDetailT"`
	TaskStatus    string `json:"taskStatus"`
}

type ScanDecrypt struct {
	ChunkID       string `json:"chunkID"`
	SchemaNameS   string `json:"schemaNameS"`
	TableNameS    string `json:"tableNameS"`
	ColumnDetailS string `json:"columnDetailS"`
	ChunkDetailS  string `json:"chunkDetailS"`
	ChunkArgsS    string `json:"chunkArgsS"`
	Samplerate    string `json:"samplerate"`
	TaskStatus    string `json:"taskStatus"`
}

func Decrypt(ctx context.Context, serverAddr, taskName, schema string, table string, chunk string, file *os.File) error {
	etcdClient, err := etcdutil.CreateClient(ctx, []string{stringutil.WithHostPort(serverAddr)}, nil)
	if err != nil {
		return err
	}
	keyResp, err := etcdutil.GetKey(etcdClient, constant.DefaultMasterDatabaseDBMSKey, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	switch {
	case len(keyResp.Kvs) > 1:
		return fmt.Errorf("get key [%v] values is over one record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	case len(keyResp.Kvs) == 1:
		// open database conn
		var dbCfg *model.Database
		err = json.Unmarshal(keyResp.Kvs[0].Value, &dbCfg)
		if err != nil {
			return fmt.Errorf("json unmarshal [%v] to struct database faild: [%v]", stringutil.BytesToString(keyResp.Kvs[0].Value), err)
		}
		err = model.CreateDatabaseReadWrite(dbCfg)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("get key [%v] values isn't exist record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	}

	taskinfo, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: taskName})
	if err != nil {
		return err
	}

	switch taskinfo.TaskMode {
	case constant.TaskModeDataCompare:
		compareTasks, err := model.GetIDataCompareTaskRW().QueryDataCompareTask(ctx, &task.DataCompareTask{
			TaskName:    taskName,
			SchemaNameS: schema,
			TableNameS:  table,
		})
		if err != nil {
			return err
		}

		var (
			clusterTable []CompareDecrypt
		)

		for _, v := range compareTasks {
			desChunkDetailS, err := stringutil.Decrypt(v.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return fmt.Errorf("error decrypt chunk_detail_s failed: %v", err)
			}
			decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
			if err != nil {
				return fmt.Errorf("error decode chunk_detail_s failed: %v", err)
			}

			desChunkDetailT, err := stringutil.Decrypt(v.ChunkDetailT, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return fmt.Errorf("error decrypt chunk_detail_s failed: %v", err)
			}
			decChunkDetailT, err := snappy.Decode(nil, []byte(desChunkDetailT))
			if err != nil {
				return fmt.Errorf("error decode chunk_detail_s failed: %v", err)
			}

			if !strings.EqualFold(chunk, "") {
				if strings.EqualFold(v.ChunkDetailS, chunk) {
					clusterTable = append(clusterTable, CompareDecrypt{
						ChunkID:      v.ChunkID,
						SchemaNameS:  v.SchemaNameS,
						TableNameS:   v.TableNameS,
						SchemaNameT:  v.SchemaNameT,
						TableNameT:   v.TableNameT,
						ChunkDetailS: stringutil.BytesToString(decChunkDetailS),
						ChunkArgsS:   v.ChunkDetailArgS,
						ChunkDetailT: stringutil.BytesToString(decChunkDetailT),
						ChunkArgsT:   v.ChunkDetailArgT,
						TaskStatus:   v.TaskStatus,
					})
				}
			} else {
				clusterTable = append(clusterTable, CompareDecrypt{
					ChunkID:      v.ChunkID,
					SchemaNameS:  v.SchemaNameS,
					TableNameS:   v.TableNameS,
					SchemaNameT:  v.SchemaNameT,
					TableNameT:   v.TableNameT,
					ChunkDetailS: stringutil.BytesToString(decChunkDetailS),
					ChunkArgsS:   v.ChunkDetailArgS,
					ChunkDetailT: stringutil.BytesToString(decChunkDetailT),
					ChunkArgsT:   v.ChunkDetailArgT,
					TaskStatus:   v.TaskStatus,
				})
			}
		}

		bf := bytes.NewBuffer([]byte{})
		jsonEncoder := json.NewEncoder(bf)
		jsonEncoder.SetEscapeHTML(false)
		jsonEncoder.SetIndent("", "    ")
		err = jsonEncoder.Encode(clusterTable)
		if err != nil {
			return err
		}
		_, err = file.WriteString(bf.String())
		if err != nil {
			return err
		}
		return nil
	case constant.TaskModeStmtMigrate, constant.TaskModeCSVMigrate:
		migrateTasks, err := model.GetIDataMigrateTaskRW().QueryDataMigrateTask(ctx, &task.DataMigrateTask{
			TaskName:    taskName,
			SchemaNameS: schema,
			TableNameS:  table,
		})
		if err != nil {
			return err
		}

		var clusterTable []MigrateDecrypt

		for _, v := range migrateTasks {
			desChunkDetailS, err := stringutil.Decrypt(v.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return fmt.Errorf("error decrypt chunk_detail_s failed: %v", err)
			}
			decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
			if err != nil {
				return fmt.Errorf("error decode chunk_detail_s failed: %v", err)
			}

			if !strings.EqualFold(chunk, "") {
				if strings.EqualFold(v.ChunkDetailS, chunk) {
					clusterTable = append(clusterTable, MigrateDecrypt{
						ChunkID:       v.ChunkID,
						SchemaNameS:   v.SchemaNameS,
						TableNameS:    v.TableNameS,
						SchemaNameT:   v.SchemaNameT,
						TableNameT:    v.TableNameT,
						ChunkDetailS:  stringutil.BytesToString(decChunkDetailS),
						ChunkArgsS:    v.ChunkDetailArgS,
						ColumnDetailS: v.ColumnDetailS,
						ColumnDetailT: v.ColumnDetailT,
						TaskStatus:    v.TaskStatus,
					})
				}
			} else {
				clusterTable = append(clusterTable, MigrateDecrypt{
					ChunkID:       v.ChunkID,
					SchemaNameS:   v.SchemaNameS,
					TableNameS:    v.TableNameS,
					SchemaNameT:   v.SchemaNameT,
					TableNameT:    v.TableNameT,
					ChunkDetailS:  stringutil.BytesToString(decChunkDetailS),
					ChunkArgsS:    v.ChunkDetailArgS,
					ColumnDetailS: v.ColumnDetailS,
					ColumnDetailT: v.ColumnDetailT,
					TaskStatus:    v.TaskStatus,
				})
			}
		}
		bf := bytes.NewBuffer([]byte{})
		jsonEncoder := json.NewEncoder(bf)
		jsonEncoder.SetEscapeHTML(false)
		jsonEncoder.SetIndent("", "    ")
		err = jsonEncoder.Encode(clusterTable)
		if err != nil {
			return err
		}
		_, err = file.WriteString(bf.String())
		if err != nil {
			return err
		}
		return nil
	case constant.TaskModeDataScan:
		compareTasks, err := model.GetIDataScanTaskRW().QueryDataScanTask(ctx, &task.DataScanTask{
			TaskName:    taskName,
			SchemaNameS: schema,
			TableNameS:  table,
		})
		if err != nil {
			return err
		}

		var clusterTable []ScanDecrypt

		for _, v := range compareTasks {
			desChunkDetailS, err := stringutil.Decrypt(v.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return fmt.Errorf("error decrypt chunk_detail_s failed: %v", err)
			}
			decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
			if err != nil {
				return fmt.Errorf("error decode chunk_detail_s failed: %v", err)
			}

			if !strings.EqualFold(chunk, "") {
				if strings.EqualFold(v.ChunkDetailS, chunk) {
					clusterTable = append(clusterTable, ScanDecrypt{
						ChunkID:       v.ChunkID,
						SchemaNameS:   v.SchemaNameS,
						TableNameS:    v.TableNameS,
						ColumnDetailS: v.ColumnDetailS,
						ChunkDetailS:  stringutil.BytesToString(decChunkDetailS),
						ChunkArgsS:    v.ChunkDetailArgS,
						Samplerate:    v.Samplerate,
						TaskStatus:    v.TaskStatus,
					})
				}
			} else {
				clusterTable = append(clusterTable, ScanDecrypt{
					ChunkID:       v.ChunkID,
					SchemaNameS:   v.SchemaNameS,
					TableNameS:    v.TableNameS,
					ColumnDetailS: v.ColumnDetailS,
					ChunkDetailS:  stringutil.BytesToString(decChunkDetailS),
					ChunkArgsS:    v.ChunkDetailArgS,
					Samplerate:    v.Samplerate,
					TaskStatus:    v.TaskStatus,
				})
			}
		}
		bf := bytes.NewBuffer([]byte{})
		jsonEncoder := json.NewEncoder(bf)
		jsonEncoder.SetEscapeHTML(false)
		jsonEncoder.SetIndent("", "    ")
		err = jsonEncoder.Encode(clusterTable)
		if err != nil {
			return err
		}
		_, err = file.WriteString(bf.String())
		if err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("the invalid task mode [%s], not support decrypt", taskName)
	}
}

type CompareCounts struct {
	SchemaNameS    string          `json:"schemaNameS"`
	TableNameS     string          `json:"tableNameS"`
	ChunkTotals    int64           `json:"chunkTotals"`
	CompareDetails []CompareDetail `json:"compareDetails"`
}

type CompareDetail struct {
	ChunkID      string `json:"chunkID"`
	SchemaNameS  string `json:"schemaNameS"`
	TableNameS   string `json:"tableNameS"`
	SchemaNameT  string `json:"schemaNameT"`
	TableNameT   string `json:"tableNameT"`
	ChunkCounts  int64  `json:"chunkCounts"`
	ChunkDetailS string `json:"chunkDetailS"`
	ChunkArgsS   string `json:"chunkArgsS"`
	ChunkDetailT string `json:"chunkDetailT"`
	ChunkArgsT   string `json:"chunkArgsT"`
	TaskStatus   string `json:"taskStatus"`
}

type MigrateCounts struct {
	SchemaNameS    string          `json:"schemaNameS"`
	TableNameS     string          `json:"tableNameS"`
	ChunkTotals    int64           `json:"chunkTotals"`
	MigrateDetails []MigrateDetail `json:"migrateDetails"`
}

type MigrateDetail struct {
	ChunkID       string `json:"chunkID"`
	SchemaNameS   string `json:"schemaNameS"`
	TableNameS    string `json:"tableNameS"`
	SchemaNameT   string `json:"schemaNameT"`
	TableNameT    string `json:"tableNameT"`
	ChunkCounts   int64  `json:"chunkCounts"`
	ChunkDetailS  string `json:"chunkDetailS"`
	ChunkArgsS    string `json:"chunkArgsS"`
	ColumnDetailS string `json:"columnDetailS"`
	ColumnDetailT string `json:"columnDetailT"`
	TaskStatus    string `json:"taskStatus"`
}

type ScanCounts struct {
	SchemaNameS string       `json:"schemaNameS"`
	TableNameS  string       `json:"tableNameS"`
	ChunkTotals int64        `json:"chunkTotals"`
	ScanDetail  []ScanDetail `json:"scanDetail"`
}

type ScanDetail struct {
	ChunkID       string `json:"chunkID"`
	SchemaNameS   string `json:"schemaNameS"`
	TableNameS    string `json:"tableNameS"`
	ChunkCounts   int64  `json:"chunkCounts"`
	ColumnDetailS string `json:"columnDetailS"`
	ChunkDetailS  string `json:"chunkDetailS"`
	ChunkArgsS    string `json:"chunkArgsS"`
	Samplerate    string `json:"samplerate"`
	TaskStatus    string `json:"taskStatus"`
}

func Counts(ctx context.Context, serverAddr, taskName, schema string, table string, chunk string, file *os.File) error {
	etcdClient, err := etcdutil.CreateClient(ctx, []string{stringutil.WithHostPort(serverAddr)}, nil)
	if err != nil {
		return err
	}
	keyResp, err := etcdutil.GetKey(etcdClient, constant.DefaultMasterDatabaseDBMSKey, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	switch {
	case len(keyResp.Kvs) > 1:
		return fmt.Errorf("get key [%v] values is over one record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	case len(keyResp.Kvs) == 1:
		// open database conn
		var dbCfg *model.Database
		err = json.Unmarshal(keyResp.Kvs[0].Value, &dbCfg)
		if err != nil {
			return fmt.Errorf("json unmarshal [%v] to struct database faild: [%v]", stringutil.BytesToString(keyResp.Kvs[0].Value), err)
		}
		err = model.CreateDatabaseReadWrite(dbCfg)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("get key [%v] values isn't exist record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	}

	taskinfo, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: taskName})
	if err != nil {
		return err
	}

	datasource, err := model.GetIDatasourceRW().GetDatasource(ctx, taskinfo.DatasourceNameS)
	if err != nil {
		return err
	}
	newDatabase, err := database.NewDatabase(ctx, datasource, schema, constant.ServiceDatabaseSqlQueryCallTimeout)
	if err != nil {
		return err
	}

	switch taskinfo.TaskMode {
	case constant.TaskModeDataCompare:
		compareTasks, err := model.GetIDataCompareTaskRW().QueryDataCompareTask(ctx, &task.DataCompareTask{
			TaskName:    taskName,
			SchemaNameS: schema,
			TableNameS:  table,
		})
		if err != nil {
			return err
		}

		var (
			clusterTable   CompareCounts
			compareDetails []CompareDetail
		)

		var totalCounts int64
		for _, v := range compareTasks {
			desChunkDetailS, err := stringutil.Decrypt(v.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return fmt.Errorf("error decrypt chunk_detail_s failed: %v", err)
			}
			decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
			if err != nil {
				return fmt.Errorf("error decode chunk_detail_s failed: %v", err)
			}

			desChunkDetailT, err := stringutil.Decrypt(v.ChunkDetailT, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return fmt.Errorf("error decrypt chunk_detail_s failed: %v", err)
			}
			decChunkDetailT, err := snappy.Decode(nil, []byte(desChunkDetailT))
			if err != nil {
				return fmt.Errorf("error decode chunk_detail_s failed: %v", err)
			}

			if !strings.EqualFold(chunk, "") {
				if strings.EqualFold(v.ChunkDetailS, chunk) {
					var args []interface{}
					if strings.EqualFold(v.ChunkDetailArgS, "") {
						args = nil
					} else {
						err = stringutil.UnmarshalJSON([]byte(v.ChunkDetailArgS), &args)
						if err != nil {
							return err
						}
					}
					_, res, err := newDatabase.GeneralQuery(fmt.Sprintf(`SELECT COUNT(1) AS "CT" FROM %s.%s WHERE %v`, v.SchemaNameS, v.TableNameS, stringutil.BytesToString(decChunkDetailS)), args...)
					if err != nil {
						return err
					}
					var counts string
					if strings.EqualFold(res[0]["CT"], "") {
						counts = "0"
					} else {
						counts = res[0]["CT"]
					}
					size, err := stringutil.StrconvIntBitSize(counts, 64)
					if err != nil {
						return err
					}
					compareDetails = append(compareDetails, CompareDetail{
						ChunkID:      v.ChunkID,
						SchemaNameS:  v.SchemaNameS,
						TableNameS:   v.TableNameS,
						SchemaNameT:  v.SchemaNameT,
						TableNameT:   v.TableNameT,
						ChunkCounts:  size,
						ChunkDetailS: stringutil.BytesToString(decChunkDetailS),
						ChunkArgsS:   v.ChunkDetailArgS,
						ChunkDetailT: stringutil.BytesToString(decChunkDetailT),
						ChunkArgsT:   v.ChunkDetailArgT,
						TaskStatus:   v.TaskStatus,
					})
					totalCounts += size
				}
			} else {
				var args []interface{}
				if strings.EqualFold(v.ChunkDetailArgS, "") {
					args = nil
				} else {
					err = stringutil.UnmarshalJSON([]byte(v.ChunkDetailArgS), &args)
					if err != nil {
						return err
					}
				}
				_, res, err := newDatabase.GeneralQuery(fmt.Sprintf(`SELECT COUNT(1) AS "CT" FROM %s.%s WHERE %v`, v.SchemaNameS, v.TableNameS, stringutil.BytesToString(decChunkDetailS)), args...)
				if err != nil {
					return err
				}
				var counts string
				if strings.EqualFold(res[0]["CT"], "") {
					counts = "0"
				} else {
					counts = res[0]["CT"]
				}
				size, err := stringutil.StrconvIntBitSize(counts, 64)
				if err != nil {
					return err
				}

				compareDetails = append(compareDetails, CompareDetail{
					ChunkID:      v.ChunkID,
					SchemaNameS:  v.SchemaNameS,
					TableNameS:   v.TableNameS,
					SchemaNameT:  v.SchemaNameT,
					TableNameT:   v.TableNameT,
					ChunkCounts:  size,
					ChunkDetailS: stringutil.BytesToString(decChunkDetailS),
					ChunkArgsS:   v.ChunkDetailArgS,
					ChunkDetailT: stringutil.BytesToString(decChunkDetailT),
					ChunkArgsT:   v.ChunkDetailArgT,
					TaskStatus:   v.TaskStatus,
				})

				totalCounts += size
			}
		}

		clusterTable.SchemaNameS = schema
		clusterTable.TableNameS = table
		clusterTable.ChunkTotals = totalCounts
		clusterTable.CompareDetails = compareDetails

		bf := bytes.NewBuffer([]byte{})
		jsonEncoder := json.NewEncoder(bf)
		jsonEncoder.SetEscapeHTML(false)
		jsonEncoder.SetIndent("", "    ")
		err = jsonEncoder.Encode(clusterTable)
		if err != nil {
			return err
		}
		_, err = file.WriteString(bf.String())
		if err != nil {
			return err
		}
		return nil
	case constant.TaskModeStmtMigrate, constant.TaskModeCSVMigrate:
		migrateTasks, err := model.GetIDataMigrateTaskRW().QueryDataMigrateTask(ctx, &task.DataMigrateTask{
			TaskName:    taskName,
			SchemaNameS: schema,
			TableNameS:  table,
		})
		if err != nil {
			return err
		}

		var (
			clusterTable   MigrateCounts
			migrateDetails []MigrateDetail
		)

		var totalCounts int64

		for _, v := range migrateTasks {
			desChunkDetailS, err := stringutil.Decrypt(v.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return fmt.Errorf("error decrypt chunk_detail_s failed: %v", err)
			}
			decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
			if err != nil {
				return fmt.Errorf("error decode chunk_detail_s failed: %v", err)
			}

			if !strings.EqualFold(chunk, "") {
				if strings.EqualFold(v.ChunkDetailS, chunk) {
					var args []interface{}
					if strings.EqualFold(v.ChunkDetailArgS, "") {
						args = nil
					} else {
						err = stringutil.UnmarshalJSON([]byte(v.ChunkDetailArgS), &args)
						if err != nil {
							return err
						}
					}
					_, res, err := newDatabase.GeneralQuery(fmt.Sprintf(`SELECT COUNT(1) AS "CT" FROM %s.%s WHERE %v`, v.SchemaNameS, v.TableNameS, stringutil.BytesToString(decChunkDetailS)), args...)
					if err != nil {
						return err
					}
					var counts string
					if strings.EqualFold(res[0]["CT"], "") {
						counts = "0"
					} else {
						counts = res[0]["CT"]
					}
					size, err := stringutil.StrconvIntBitSize(counts, 64)
					if err != nil {
						return err
					}
					migrateDetails = append(migrateDetails, MigrateDetail{
						ChunkID:       v.ChunkID,
						SchemaNameS:   v.SchemaNameS,
						TableNameS:    v.TableNameS,
						SchemaNameT:   v.SchemaNameT,
						TableNameT:    v.TableNameT,
						ChunkCounts:   size,
						ChunkDetailS:  stringutil.BytesToString(decChunkDetailS),
						ChunkArgsS:    v.ChunkDetailArgS,
						ColumnDetailS: v.ColumnDetailS,
						ColumnDetailT: v.ColumnDetailT,
						TaskStatus:    v.TaskStatus,
					})

					totalCounts += size
				}
			} else {
				var args []interface{}
				if strings.EqualFold(v.ChunkDetailArgS, "") {
					args = nil
				} else {
					err = stringutil.UnmarshalJSON([]byte(v.ChunkDetailArgS), &args)
					if err != nil {
						return err
					}
				}
				_, res, err := newDatabase.GeneralQuery(fmt.Sprintf(`SELECT COUNT(1) AS "CT" FROM %s.%s WHERE %v`, v.SchemaNameS, v.TableNameS, stringutil.BytesToString(decChunkDetailS)), args...)
				if err != nil {
					return err
				}
				var counts string
				if strings.EqualFold(res[0]["CT"], "") {
					counts = "0"
				} else {
					counts = res[0]["CT"]
				}
				size, err := stringutil.StrconvIntBitSize(counts, 64)
				if err != nil {
					return err
				}
				migrateDetails = append(migrateDetails, MigrateDetail{
					ChunkID:       v.ChunkID,
					SchemaNameS:   v.SchemaNameS,
					TableNameS:    v.TableNameS,
					SchemaNameT:   v.SchemaNameT,
					TableNameT:    v.TableNameT,
					ChunkCounts:   size,
					ChunkDetailS:  stringutil.BytesToString(decChunkDetailS),
					ChunkArgsS:    v.ChunkDetailArgS,
					ColumnDetailS: v.ColumnDetailS,
					ColumnDetailT: v.ColumnDetailT,
					TaskStatus:    v.TaskStatus,
				})

				totalCounts += size
			}
		}

		clusterTable.SchemaNameS = schema
		clusterTable.TableNameS = table
		clusterTable.ChunkTotals = totalCounts
		clusterTable.MigrateDetails = migrateDetails

		bf := bytes.NewBuffer([]byte{})
		jsonEncoder := json.NewEncoder(bf)
		jsonEncoder.SetEscapeHTML(false)
		jsonEncoder.SetIndent("", "    ")
		err = jsonEncoder.Encode(clusterTable)
		if err != nil {
			return err
		}
		_, err = file.WriteString(bf.String())
		if err != nil {
			return err
		}
		return nil
	case constant.TaskModeDataScan:
		compareTasks, err := model.GetIDataScanTaskRW().QueryDataScanTask(ctx, &task.DataScanTask{
			TaskName:    taskName,
			SchemaNameS: schema,
			TableNameS:  table,
		})
		if err != nil {
			return err
		}

		var (
			clusterTable ScanCounts
			scanDetails  []ScanDetail
		)

		var totalCounts int64

		for _, v := range compareTasks {
			desChunkDetailS, err := stringutil.Decrypt(v.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return fmt.Errorf("error decrypt chunk_detail_s failed: %v", err)
			}
			decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
			if err != nil {
				return fmt.Errorf("error decode chunk_detail_s failed: %v", err)
			}

			if !strings.EqualFold(chunk, "") {
				if strings.EqualFold(v.ChunkDetailS, chunk) {
					var args []interface{}
					if strings.EqualFold(v.ChunkDetailArgS, "") {
						args = nil
					} else {
						err = stringutil.UnmarshalJSON([]byte(v.ChunkDetailArgS), &args)
						if err != nil {
							return err
						}
					}
					_, res, err := newDatabase.GeneralQuery(fmt.Sprintf(`SELECT COUNT(1) AS "CT" FROM %s.%s WHERE %v`, v.SchemaNameS, v.TableNameS, stringutil.BytesToString(decChunkDetailS)), args...)
					if err != nil {
						return err
					}
					var counts string
					if strings.EqualFold(res[0]["CT"], "") {
						counts = "0"
					} else {
						counts = res[0]["CT"]
					}
					size, err := stringutil.StrconvIntBitSize(counts, 64)
					if err != nil {
						return err
					}
					scanDetails = append(scanDetails, ScanDetail{
						ChunkID:       v.ChunkID,
						SchemaNameS:   v.SchemaNameS,
						TableNameS:    v.TableNameS,
						ColumnDetailS: v.ColumnDetailS,
						ChunkDetailS:  stringutil.BytesToString(decChunkDetailS),
						ChunkArgsS:    v.ChunkDetailArgS,
						Samplerate:    v.Samplerate,
						TaskStatus:    v.TaskStatus,
					})
					totalCounts += size
				}
			} else {
				var args []interface{}
				if strings.EqualFold(v.ChunkDetailArgS, "") {
					args = nil
				} else {
					err = stringutil.UnmarshalJSON([]byte(v.ChunkDetailArgS), &args)
					if err != nil {
						return err
					}
				}
				_, res, err := newDatabase.GeneralQuery(fmt.Sprintf(`SELECT COUNT(1) AS "CT" FROM %s.%s WHERE %v`, v.SchemaNameS, v.TableNameS, stringutil.BytesToString(decChunkDetailS)), args...)
				if err != nil {
					return err
				}
				var counts string
				if strings.EqualFold(res[0]["CT"], "") {
					counts = "0"
				} else {
					counts = res[0]["CT"]
				}
				size, err := stringutil.StrconvIntBitSize(counts, 64)
				if err != nil {
					return err
				}
				scanDetails = append(scanDetails, ScanDetail{
					ChunkID:       v.ChunkID,
					SchemaNameS:   v.SchemaNameS,
					TableNameS:    v.TableNameS,
					ColumnDetailS: v.ColumnDetailS,
					ChunkDetailS:  stringutil.BytesToString(decChunkDetailS),
					ChunkArgsS:    v.ChunkDetailArgS,
					Samplerate:    v.Samplerate,
					TaskStatus:    v.TaskStatus,
				})
				totalCounts += size
			}
		}

		clusterTable.SchemaNameS = schema
		clusterTable.TableNameS = table
		clusterTable.ChunkTotals = totalCounts
		clusterTable.ScanDetail = scanDetails

		bf := bytes.NewBuffer([]byte{})
		jsonEncoder := json.NewEncoder(bf)
		jsonEncoder.SetEscapeHTML(false)
		jsonEncoder.SetIndent("", "    ")
		err = jsonEncoder.Encode(clusterTable)
		if err != nil {
			return err
		}
		_, err = file.WriteString(bf.String())
		if err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("the invalid task mode [%s], not support decrypt", taskName)
	}
}
