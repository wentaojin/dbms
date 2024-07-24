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
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang/snappy"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/etcdutil"
	"github.com/wentaojin/dbms/utils/stringutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"os"
	"strings"
)

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

		type CompareDecrypt struct {
			ID           int
			SchemaNameS  string
			TableNameS   string
			SchemaNameT  string
			TableNameT   string
			ChunkDetailS string
			ChunkDetailT string
			TaskStatus   string
		}
		var clusterTable []CompareDecrypt

		for i, v := range compareTasks {
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
						ID:           i,
						SchemaNameS:  v.SchemaNameS,
						TableNameS:   v.TableNameS,
						SchemaNameT:  v.SchemaNameT,
						TableNameT:   v.TableNameT,
						ChunkDetailS: stringutil.BytesToString(decChunkDetailS),
						ChunkDetailT: stringutil.BytesToString(decChunkDetailT),
						TaskStatus:   v.TaskStatus,
					})
				}
			} else {
				clusterTable = append(clusterTable, CompareDecrypt{
					ID:           i,
					SchemaNameS:  v.SchemaNameS,
					TableNameS:   v.TableNameS,
					SchemaNameT:  v.SchemaNameT,
					TableNameT:   v.TableNameT,
					ChunkDetailS: stringutil.BytesToString(decChunkDetailS),
					ChunkDetailT: stringutil.BytesToString(decChunkDetailT),
					TaskStatus:   v.TaskStatus,
				})
			}
		}
		indentJSON, err := stringutil.MarshalIndentJSON(clusterTable)
		if err != nil {
			return err
		}

		_, err = file.WriteString(indentJSON)
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

		type MigrateDecrypt struct {
			ID            int
			SchemaNameS   string
			TableNameS    string
			SchemaNameT   string
			TableNameT    string
			ChunkDetailS  string
			ColumnDetailS string
			ColumnDetailT string
			TaskStatus    string
		}

		var clusterTable []MigrateDecrypt

		for i, v := range migrateTasks {
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
						ID:            i,
						SchemaNameS:   v.SchemaNameS,
						TableNameS:    v.TableNameS,
						SchemaNameT:   v.SchemaNameT,
						TableNameT:    v.TableNameT,
						ChunkDetailS:  stringutil.BytesToString(decChunkDetailS),
						ColumnDetailS: v.ColumnDetailS,
						ColumnDetailT: v.ColumnDetailT,
						TaskStatus:    v.TaskStatus,
					})
				}
			} else {
				clusterTable = append(clusterTable, MigrateDecrypt{
					ID:            i,
					SchemaNameS:   v.SchemaNameS,
					TableNameS:    v.TableNameS,
					SchemaNameT:   v.SchemaNameT,
					TableNameT:    v.TableNameT,
					ChunkDetailS:  stringutil.BytesToString(decChunkDetailS),
					ColumnDetailS: v.ColumnDetailS,
					ColumnDetailT: v.ColumnDetailT,
					TaskStatus:    v.TaskStatus,
				})
			}
		}
		indentJSON, err := stringutil.MarshalIndentJSON(clusterTable)
		if err != nil {
			return err
		}

		_, err = file.WriteString(indentJSON)
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

		type ScanDecrypt struct {
			ID            int
			SchemaNameS   string
			TableNameS    string
			ColumnDetailS string
			ChunkDetailS  string
			Samplerate    string
			TaskStatus    string
		}
		var clusterTable []ScanDecrypt

		for i, v := range compareTasks {
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
						ID:            i,
						SchemaNameS:   v.SchemaNameS,
						TableNameS:    v.TableNameS,
						ColumnDetailS: v.ColumnDetailS,
						ChunkDetailS:  stringutil.BytesToString(decChunkDetailS),
						Samplerate:    v.Samplerate,
						TaskStatus:    v.TaskStatus,
					})
				}
			} else {
				clusterTable = append(clusterTable, ScanDecrypt{
					ID:            i,
					SchemaNameS:   v.SchemaNameS,
					TableNameS:    v.TableNameS,
					ColumnDetailS: v.ColumnDetailS,
					ChunkDetailS:  stringutil.BytesToString(decChunkDetailS),
					Samplerate:    v.Samplerate,
					TaskStatus:    v.TaskStatus,
				})
			}
		}
		indentJSON, err := stringutil.MarshalIndentJSON(clusterTable)
		if err != nil {
			return err
		}

		_, err = file.WriteString(indentJSON)
		if err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("the invalid task mode [%s], not support decrypt", taskName)
	}
}
