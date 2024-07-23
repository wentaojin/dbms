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
	"github.com/fatih/color"
	"github.com/golang/snappy"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/etcdutil"
	"github.com/wentaojin/dbms/utils/stringutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strconv"
	"strings"
)

func Decrypt(ctx context.Context, serverAddr, taskName, schema string, table string, chunk string) ([][]string, error) {
	etcdClient, err := etcdutil.CreateClient(ctx, []string{stringutil.WithHostPort(serverAddr)}, nil)
	if err != nil {
		return nil, err
	}
	keyResp, err := etcdutil.GetKey(etcdClient, constant.DefaultMasterDatabaseDBMSKey, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	switch {
	case len(keyResp.Kvs) > 1:
		return nil, fmt.Errorf("get key [%v] values is over one record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	case len(keyResp.Kvs) == 1:
		// open database conn
		var dbCfg *model.Database
		err = json.Unmarshal(keyResp.Kvs[0].Value, &dbCfg)
		if err != nil {
			return nil, fmt.Errorf("json unmarshal [%v] to struct database faild: [%v]", stringutil.BytesToString(keyResp.Kvs[0].Value), err)
		}
		err = model.CreateDatabaseReadWrite(dbCfg)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("get key [%v] values isn't exist record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	}

	taskinfo, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: taskName})
	if err != nil {
		return nil, err
	}

	switch taskinfo.TaskMode {
	case constant.TaskModeDataCompare:
		compareTasks, err := model.GetIDataCompareTaskRW().QueryDataCompareTask(ctx, &task.DataCompareTask{
			TaskName:    taskName,
			SchemaNameS: schema,
			TableNameS:  table,
		})
		if err != nil {
			return nil, err
		}

		var clusterTable [][]string
		rowHead := []string{"ID", "TableNameS", "ChunkDetailS", "TableNameT", "ChunkDetailT", "TaskStatus"}
		clusterTable = append(clusterTable, rowHead)

		for i, v := range compareTasks {
			desChunkDetailS, err := stringutil.Decrypt(v.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return nil, fmt.Errorf("error decrypt chunk_detail_s failed: %v", err)
			}
			decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
			if err != nil {
				return nil, fmt.Errorf("error decode chunk_detail_s failed: %v", err)
			}

			desChunkDetailT, err := stringutil.Decrypt(v.ChunkDetailT, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return nil, fmt.Errorf("error decrypt chunk_detail_s failed: %v", err)
			}
			decChunkDetailT, err := snappy.Decode(nil, []byte(desChunkDetailT))
			if err != nil {
				return nil, fmt.Errorf("error decode chunk_detail_s failed: %v", err)
			}

			if !strings.EqualFold(chunk, "") {
				if strings.EqualFold(v.ChunkDetailS, chunk) {
					row := []string{
						color.CyanString(strconv.Itoa(i)),
						fmt.Sprintf("%s.%s", v.SchemaNameS, v.TableNameS),
						stringutil.BytesToString(decChunkDetailS),
						fmt.Sprintf("%s.%s", v.SchemaNameT, v.TableNameT),
						stringutil.BytesToString(decChunkDetailT),
						v.TaskStatus,
					}
					clusterTable = append(clusterTable, row)
				}
			} else {
				row := []string{
					color.CyanString(strconv.Itoa(i)),
					fmt.Sprintf("%s.%s", v.SchemaNameS, v.TableNameS),
					stringutil.BytesToString(decChunkDetailS),
					fmt.Sprintf("%s.%s", v.SchemaNameT, v.TableNameT),
					stringutil.BytesToString(decChunkDetailT),
					v.TaskStatus,
				}
				clusterTable = append(clusterTable, row)
			}
		}
		return clusterTable, nil

	case constant.TaskModeStmtMigrate, constant.TaskModeCSVMigrate:
		migrateTasks, err := model.GetIDataMigrateTaskRW().QueryDataMigrateTask(ctx, &task.DataMigrateTask{
			TaskName:    taskName,
			SchemaNameS: schema,
			TableNameS:  table,
		})
		if err != nil {
			return nil, err
		}

		var clusterTable [][]string
		rowHead := []string{"ID", "TableNameS", "ColumnDetailS", "ChunkDetailS", "TableNameT", "ColumnDetailT", "TaskStatus"}
		clusterTable = append(clusterTable, rowHead)

		for i, v := range migrateTasks {
			desChunkDetailS, err := stringutil.Decrypt(v.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return nil, fmt.Errorf("error decrypt chunk_detail_s failed: %v", err)
			}
			decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
			if err != nil {
				return nil, fmt.Errorf("error decode chunk_detail_s failed: %v", err)
			}

			if !strings.EqualFold(chunk, "") {
				if strings.EqualFold(v.ChunkDetailS, chunk) {
					row := []string{
						color.CyanString(strconv.Itoa(i)),
						fmt.Sprintf("%s.%s", v.SchemaNameS, v.TableNameS),
						v.ColumnDetailS,
						stringutil.BytesToString(decChunkDetailS),
						fmt.Sprintf("%s.%s", v.SchemaNameT, v.TableNameT),
						v.ColumnDetailT,
						v.TaskStatus,
					}
					clusterTable = append(clusterTable, row)
				}
			} else {
				row := []string{
					color.CyanString(strconv.Itoa(i)),
					fmt.Sprintf("%s.%s", v.SchemaNameS, v.TableNameS),
					v.ColumnDetailS,
					stringutil.BytesToString(decChunkDetailS),
					fmt.Sprintf("%s.%s", v.SchemaNameT, v.TableNameT),
					v.ColumnDetailT,
					v.TaskStatus,
				}
				clusterTable = append(clusterTable, row)
			}
		}
		return clusterTable, nil

	case constant.TaskModeDataScan:
		compareTasks, err := model.GetIDataScanTaskRW().QueryDataScanTask(ctx, &task.DataScanTask{
			TaskName:    taskName,
			SchemaNameS: schema,
			TableNameS:  table,
		})
		if err != nil {
			return nil, err
		}

		var clusterTable [][]string
		rowHead := []string{"ID", "TableNameS", "ChunkDetailS", "Samplerate", "TaskStatus"}
		clusterTable = append(clusterTable, rowHead)

		for i, v := range compareTasks {
			desChunkDetailS, err := stringutil.Decrypt(v.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return nil, fmt.Errorf("error decrypt chunk_detail_s failed: %v", err)
			}
			decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
			if err != nil {
				return nil, fmt.Errorf("error decode chunk_detail_s failed: %v", err)
			}

			if !strings.EqualFold(chunk, "") {
				if strings.EqualFold(v.ChunkDetailS, chunk) {
					row := []string{
						color.CyanString(strconv.Itoa(i)),
						fmt.Sprintf("%s.%s", v.SchemaNameS, v.TableNameS),
						stringutil.BytesToString(decChunkDetailS),
						v.Samplerate,
						v.TaskStatus,
					}
					clusterTable = append(clusterTable, row)
				}
			} else {
				row := []string{
					color.CyanString(strconv.Itoa(i)),
					fmt.Sprintf("%s.%s", v.SchemaNameS, v.TableNameS),
					stringutil.BytesToString(decChunkDetailS),
					v.Samplerate,
					v.TaskStatus,
				}
				clusterTable = append(clusterTable, row)
			}
		}
		return clusterTable, nil
	default:
		return nil, fmt.Errorf("the invalid task mode [%s], not support decrypt", taskName)
	}
}
