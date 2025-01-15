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
package taskflow

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/database/msgsrv"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/message/oceanbase"
	"github.com/wentaojin/dbms/message/tidb"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/consume"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

type CdcConsumeTask struct {
	Ctx           context.Context
	Task          *task.Task
	DatasourceS   *datasource.Datasource
	DatasourceT   *datasource.Datasource
	MigrateParams *pb.CdcConsumeParam
}

func (cct *CdcConsumeTask) Start() error {
	schemaTaskTime := time.Now()

	if !cct.MigrateParams.EnableCheckpoint {
		logger.Warn("cdc consume task get schema route",
			zap.String("task_name", cct.Task.TaskName),
			zap.String("task_mode", cct.Task.TaskMode),
			zap.String("task_flow", cct.Task.TaskFlow),
			zap.Bool("enable_checkpoint", cct.MigrateParams.EnableCheckpoint))
		if err := model.Transaction(cct.Ctx, func(txnCtx context.Context) error {
			err := model.GetIMsgTopicPartitionRW().DeleteMsgTopicPartition(txnCtx, []string{cct.Task.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIMsgDdlRewriteRW().DeleteMsgDdlRewrite(txnCtx, []string{cct.Task.TaskName})
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}

	_, err := model.GetITaskLogRW().CreateLog(cct.Ctx, &task.Log{
		TaskName: cct.Task.TaskName,
		LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] cdc consumer get route rules",
			stringutil.CurrentTimeFormatString(),
			stringutil.StringLower(constant.TaskModeCdcConsume),
			cct.Task.WorkerAddr,
			cct.Task.TaskName,
		),
	})
	if err != nil {
		return err
	}

	logger.Info("cdc consume task get schema route",
		zap.String("task_name", cct.Task.TaskName),
		zap.String("task_mode", cct.Task.TaskMode),
		zap.String("task_flow", cct.Task.TaskFlow))
	schemaRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(cct.Ctx,
		&rule.SchemaRouteRule{TaskName: cct.Task.TaskName})
	if err != nil {
		return err
	}
	logger.Info("cdc consume task get table route",
		zap.String("task_name", cct.Task.TaskName),
		zap.String("task_mode", cct.Task.TaskMode),
		zap.String("task_flow", cct.Task.TaskFlow))
	tableRoutes, err := model.GetIMigrateTableRouteRW().FindTableRouteRule(cct.Ctx,
		&rule.TableRouteRule{
			TaskName:    cct.Task.TaskName,
			SchemaNameS: schemaRoute.SchemaNameS,
		})
	if err != nil {
		return err
	}
	logger.Info("cdc consume task get column route",
		zap.String("task_name", cct.Task.TaskName),
		zap.String("task_mode", cct.Task.TaskMode),
		zap.String("task_flow", cct.Task.TaskFlow))
	columnRoute, err := model.GetIMigrateColumnRouteRW().QueryColumnRouteRule(cct.Ctx,
		&rule.ColumnRouteRule{
			TaskName:    cct.Task.TaskName,
			SchemaNameS: schemaRoute.SchemaNameS,
		})
	if err != nil {
		return err
	}
	logger.Info("cdc consume task get consume table",
		zap.String("task_name", cct.Task.TaskName),
		zap.String("task_mode", cct.Task.TaskMode),
		zap.String("task_flow", cct.Task.TaskFlow))
	consumeTables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(cct.Ctx, &rule.MigrateTaskTable{
		TaskName:    cct.Task.TaskName,
		SchemaNameS: schemaRoute.SchemaNameS,
	})
	if err != nil {
		return err
	}
	var (
		includes, excludes []string
	)
	for _, t := range consumeTables {
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsExclude) {
			excludes = append(excludes, t.TableNameS)
		}
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsNotExclude) {
			includes = append(includes, t.TableNameS)
		}
	}

	_, err = model.GetITaskLogRW().CreateLog(cct.Ctx, &task.Log{
		TaskName: cct.Task.TaskName,
		LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] cdc consumer init connection",
			stringutil.CurrentTimeFormatString(),
			stringutil.StringLower(constant.TaskModeCdcConsume),
			cct.Task.WorkerAddr,
			cct.Task.TaskName,
		),
	})
	if err != nil {
		return err
	}

	logger.Info("cdc consume task init connection",
		zap.String("task_name", cct.Task.TaskName),
		zap.String("task_mode", cct.Task.TaskMode),
		zap.String("task_flow", cct.Task.TaskFlow))

	var (
		databaseS, databaseT database.IDatabase
		kafkaPartitions      []int
	)

	dbType := stringutil.StringSplit(cct.Task.TaskFlow, constant.StringSeparatorAite)
	dbTypeS := dbType[0]
	dbTypeT := dbType[1]

	switch dbTypeS {
	case constant.DatabaseTypeTiDB, constant.DatabaseTypeOceanbaseMYSQL:
		databaseS, err = database.NewDatabase(cct.Ctx, cct.DatasourceS, "", int64(cct.MigrateParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseS.Close()

		conTables, err := databaseS.FilterDatabaseTable(schemaRoute.SchemaNameS, includes, excludes)
		if err != nil {
			return err
		}

		switch cct.Task.TaskFlow {
		case constant.TaskFlowTiDBToOracle, constant.TaskFlowOBMySQLToOracle:
			databaseT, err = database.NewDatabase(cct.Ctx, cct.DatasourceT, schemaRoute.SchemaNameT, int64(cct.MigrateParams.CallTimeout))
			if err != nil {
				return err
			}
			defer databaseT.Close()

		case constant.TaskFlowTiDBToTiDB, constant.TaskFlowTiDBToMYSQL, constant.TaskFlowTiDBToPostgres,
			constant.TaskFlowOBMySQLToTiDB, constant.TaskFlowOBMySQLToMySQL, constant.TaskFlowOBMySQLToOBMySQL,
			constant.TaskFlowOBMySQLToPostgres:
			databaseT, err = database.NewDatabase(cct.Ctx, cct.DatasourceT, "", int64(cct.MigrateParams.CallTimeout))
			if err != nil {
				return err
			}
			defer databaseT.Close()
		default:
			return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] schema_name_s [%s] isn't support, please contact author or reselect", cct.Task.TaskName, cct.Task.TaskMode, cct.Task.TaskFlow, schemaRoute.SchemaNameS)
		}
		if strings.EqualFold(dbTypeS, constant.DatabaseTypeTiDB) {
			constraint := &tidb.Constraint{
				Task:        cct.Task,
				SchemaRoute: schemaRoute,
				DatabaseS:   databaseS,
				DatabaseT:   databaseT,
				TaskTables:  conTables.TaskTables,
				TableRoutes: tableRoutes,
				TableThread: int(cct.MigrateParams.TableThread),
			}
			if err := constraint.Inspect(cct.Ctx); err != nil {
				return err
			}

			d := &tidb.Metadata{
				TaskName:       cct.Task.TaskName,
				TaskFlow:       cct.Task.TaskFlow,
				TaskMode:       cct.Task.TaskMode,
				SchemaNameS:    schemaRoute.SchemaNameS,
				SchemaNameT:    schemaRoute.SchemaNameT,
				TaskTables:     conTables.TaskTables,
				TableThread:    int(cct.MigrateParams.TableThread),
				DatabaseT:      databaseT,
				TableRoutes:    tableRoutes,
				CaseFieldRuleS: cct.Task.CaseFieldRuleS,
				CaseFieldRuleT: cct.Task.CaseFieldRuleT,
			}
			err = d.GenDownstream(cct.Ctx)
			if err != nil {
				return err
			}
		}

		if strings.EqualFold(dbTypeS, constant.DatabaseTypeOceanbaseMYSQL) {
			constraint := &oceanbase.Constraint{
				Task:        cct.Task,
				SchemaRoute: schemaRoute,
				DbTypeT:     dbTypeT,
				DatabaseS:   databaseS,
				DatabaseT:   databaseT,
				TaskTables:  conTables.TaskTables,
				TableRoutes: tableRoutes,
			}
			if err := constraint.Inspect(cct.Ctx); err != nil {
				return err
			}
			d := &oceanbase.Metadata{
				TaskName:       cct.Task.TaskName,
				TaskFlow:       cct.Task.TaskFlow,
				TaskMode:       cct.Task.TaskMode,
				SchemaNameS:    schemaRoute.SchemaNameS,
				SchemaNameT:    schemaRoute.SchemaNameT,
				TaskTables:     conTables.TaskTables,
				TableThread:    int(cct.MigrateParams.TableThread),
				TableRoutes:    tableRoutes,
				ColumnRoutes:   columnRoute,
				CaseFieldRuleS: cct.Task.CaseFieldRuleS,
				CaseFieldRuleT: cct.Task.CaseFieldRuleT,
				DatabaseS:      databaseS,
				DatabaseT:      databaseT,
			}

			err = d.GenUpstream(cct.Ctx)
			if err != nil {
				return err
			}
			err = d.GenDownstream(cct.Ctx)
			if err != nil {
				return err
			}
		}

		logger.Info("get topic partitions",
			zap.String("task_name", cct.Task.TaskName),
			zap.String("task_flow", cct.Task.TaskFlow),
			zap.String("task_mode", cct.Task.TaskMode),
			zap.String("topic", cct.MigrateParams.SubscribeTopic))
		_, err = model.GetITaskLogRW().CreateLog(cct.Ctx, &task.Log{
			TaskName: cct.Task.TaskName,
			LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] cdc consumer init topic partition",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeCdcConsume),
				cct.Task.WorkerAddr,
				cct.Task.TaskName,
			),
		})
		if err != nil {
			return err
		}

		var kafkaConn *kafka.Conn

		kafkaConn, err = msgsrv.NewKafkaConn(cct.Ctx, cct.MigrateParams.ServerAddress)
		if err != nil {
			return err
		}
		defer kafkaConn.Close()

		partitions, err := kafkaConn.ReadPartitions(cct.MigrateParams.SubscribeTopic)
		if err != nil {
			return err
		}

		for _, p := range partitions {
			if err := model.Transaction(cct.Ctx, func(txnCtx context.Context) error {
				counts, err := model.GetIMsgTopicPartitionRW().CountMsgTopicPartition(txnCtx, &consume.MsgTopicPartition{
					TaskName:   cct.Task.TaskName,
					Topic:      cct.MigrateParams.SubscribeTopic,
					Partitions: p.ID,
				})
				if err != nil {
					return err
				}

				if counts == 0 {
					if _, err := model.GetIMsgTopicPartitionRW().CreateMsgTopicPartition(txnCtx, &consume.MsgTopicPartition{
						TaskName:   cct.Task.TaskName,
						Topic:      cct.MigrateParams.SubscribeTopic,
						Partitions: p.ID,
						Replicas:   len(p.Replicas),
						Checkpoint: 0,
						Offset:     0,
					}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
			kafkaPartitions = append(kafkaPartitions, p.ID)
		}

		logger.Info("start consumer group",
			zap.String("task_name", cct.Task.TaskName),
			zap.String("task_flow", cct.Task.TaskFlow),
			zap.String("task_mode", cct.Task.TaskMode),
			zap.String("topic", cct.MigrateParams.SubscribeTopic),
			zap.Ints("consumers", kafkaPartitions))

		_, err = model.GetITaskLogRW().CreateLog(cct.Ctx, &task.Log{
			TaskName: cct.Task.TaskName,
			LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] cdc consumer group start",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeCdcConsume),
				cct.Task.WorkerAddr,
				cct.Task.TaskName,
			),
		})
		if err != nil {
			return err
		}

		if strings.EqualFold(dbTypeS, constant.DatabaseTypeTiDB) {
			if err := tidb.NewConsumerGroup(
				cct.Ctx,
				cct.Task,
				schemaRoute,
				tableRoutes,
				columnRoute,
				conTables.TaskTables,
				cct.MigrateParams,
				kafkaPartitions,
				dbTypeT,
				databaseS,
				databaseT,
			).Run(); err != nil {
				return err
			}
		}
		if strings.EqualFold(dbTypeS, constant.DatabaseTypeOceanbaseMYSQL) {
			if err := oceanbase.NewConsumerGroup(
				cct.Ctx,
				cct.Task,
				schemaRoute,
				tableRoutes,
				columnRoute,
				conTables.TaskTables,
				cct.MigrateParams,
				kafkaPartitions,
				dbTypeS,
				dbTypeT,
				databaseS,
				databaseT,
			).Run(); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] database type [%s] isn't support, please contact author or reselect", cct.Task.TaskName, cct.Task.TaskMode, cct.Task.TaskFlow, dbTypeS)
	}

	logger.Info("cdc consume task",
		zap.String("task_name", cct.Task.TaskName),
		zap.String("task_mode", cct.Task.TaskMode),
		zap.String("task_flow", cct.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	return nil
}
