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
package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/model/buildin"

	"github.com/wentaojin/dbms/model/migrate"

	"github.com/wentaojin/dbms/model/params"

	"github.com/wentaojin/dbms/model/task"

	"github.com/wentaojin/dbms/model/rule"

	"go.uber.org/zap"

	"gorm.io/gorm/schema"

	"github.com/wentaojin/dbms/logger"

	_ "github.com/go-sql-driver/mysql"
	"github.com/wentaojin/dbms/model/common"
	"github.com/wentaojin/dbms/model/datasource"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var DefaultDB *database

type database struct {
	base                          *gorm.DB
	datasourceRW                  datasource.IDatasource
	migrateSchemaRouteRW          rule.ISchemaRouteRule
	migrateTableRouteRW           rule.ITableRouteRule
	migrateColumnRouteRW          rule.IColumnRouteRule
	migrateTableRuleRW            rule.IDataMigrateRule
	migrateSqlRuleRW              rule.ISqlMigrateRule
	migrateTaskTableRW            rule.IMigrateTaskTable
	taskRW                        task.ITask
	taskLogRW                     task.ILog
	paramsRW                      params.IParams
	structMigrateSummaryRW        task.IStructMigrateSummary
	structMigrateTaskRW           task.IStructMigrateTask
	structMigrateTaskRuleRW       migrate.IStructMigrateTaskRule
	structMigrateSchemaRuleRW     migrate.IStructMigrateSchemaRule
	structMigrateTableRuleRW      migrate.IStructMigrateTableRule
	structMigrateColumnRuleRW     migrate.IStructMigrateColumnRule
	structMigrateTableAttrsRuleRW migrate.IStructMigrateTableAttrsRule
	buildinDatatypeRuleRW         buildin.IBuildInDatatypeRule
	buildinRuleRecordRW           buildin.IBuildInRuleRecord
	buildinDefaultValueRW         buildin.IBuildInDefaultValueRule
	buildinCompatibleRW           buildin.IBuildInCompatibleRule
	dataMigrateTaskRW             task.IDataMigrateTask
	dataMigrateSummaryTask        task.IDataMigrateSummary
	sqlMigrateTaskRW              task.ISqlMigrateTask
	sqlMigrateSummaryTask         task.ISqlMigrateSummary
	dataCompareTaskRW             task.IDataCompareTask
	dataCompareRuleRW             rule.IDataCompareRule
	dataCompareSummaryTask        task.IDataCompareSummary
	assessMigrateTaskRW           task.IAssessMigrateTask
	structCompareSummaryRW        task.IStructCompareSummary
	structCompareTaskRW           task.IStructCompareTask
	dataScanSummaryRW             task.IDataScanSummary
	dataScanTaskRW                task.IDataScanTask
	dataScanRuleRW                rule.IDataScanRule
}

// Database is database configuration.
type Database struct {
	Host          string `toml:"host" json:"host"`
	Port          uint64 `toml:"port" json:"port"`
	Username      string `toml:"username" json:"username"`
	Password      string `toml:"password" json:"password"`
	Schema        string `toml:"schema" json:"schema"`
	SlowThreshold uint64 `toml:"slowThreshold" json:"slowThreshold"`
}

// CreateDatabaseConnection create database connection
func CreateDatabaseConnection(cfg *Database, addRole, logLevel string) error {
	dsn := buildMysqlDSN(cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Schema)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		PrepareStmt:                              true,
		DisableNestedTransaction:                 true,
		Logger:                                   logger.GetGormLogger(logLevel, cfg.SlowThreshold),
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
	})

	if err != nil || db.Error != nil {
		return fmt.Errorf("database open failed, database error: [%v]", err)
	}

	DefaultDB = &database{
		base: db,
	}

	DefaultDB.initReaderWriters()

	if strings.EqualFold(addRole, constant.DefaultInstanceRoleMaster) {
		startTime := time.Now()
		logger.Info("database table migrate starting", zap.String("database", cfg.Schema), zap.String("startTime", startTime.String()))
		err = DefaultDB.migrateTables()
		if err != nil {
			return fmt.Errorf("database [%s] migrate tables failed, database error: [%v]", cfg.Schema, err)
		}
		endTime := time.Now()
		logger.Info("database table migrate end", zap.String("database", cfg.Schema), zap.String("endTime", endTime.String()), zap.String("cost", endTime.Sub(startTime).String()))

		startTime = time.Now()
		logger.Info("database table init starting", zap.String("database", cfg.Schema), zap.String("startTime", startTime.String()))
		err = DefaultDB.migrateTables()
		ctx := context.Background()
		err = DefaultDB.initDatatypeRule(ctx)
		if err != nil {
			return err
		}
		err = DefaultDB.initDefaultValueRule(ctx)
		if err != nil {
			return err
		}
		err = DefaultDB.initCompatibleRule(ctx)
		if err != nil {
			return err
		}
		endTime = time.Now()
		logger.Info("database table init end", zap.String("database", cfg.Schema), zap.String("endTime", endTime.String()), zap.String("cost", endTime.Sub(startTime).String()))
	}
	logger.Info("database connection success", zap.String("database", cfg.Schema))
	return nil
}

func CreateDatabaseReadWrite(cfg *Database) error {
	dsn := buildMysqlDSN(cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Schema)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		PrepareStmt:                              true,
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
	})

	if err != nil || db.Error != nil {
		return fmt.Errorf("database open failed, database error: [%v]", err)
	}

	DefaultDB = &database{
		base: db,
	}

	DefaultDB.initReaderWriters()
	return nil
}

func CreateDatabaseSchema(cfg *Database) error {
	dsn := buildMysqlDSN(cfg.Username, cfg.Password, cfg.Host, cfg.Port, "")

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("error on open mysql database connection: %v", err)
	}

	err = db.Ping()
	if err != nil {
		return fmt.Errorf("database ping failed, database error: [%v]", err)
	}

	createSchema := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, cfg.Schema)
	_, err = db.Exec(createSchema)
	if err != nil {
		return fmt.Errorf("database sql [%v] exec failed, database error: [%v]", createSchema, err)
	}
	return nil
}

func buildMysqlDSN(user, password, host string, port uint64, schema string) string {
	if !strings.EqualFold(schema, "") {
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local", user, password, host, port, schema)
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=true&loc=Local", user, password, host, port)
}

func (d *database) initReaderWriters() {
	DefaultDB.datasourceRW = datasource.NewDatasourceRW(d.base)
	DefaultDB.migrateSchemaRouteRW = rule.NewSchemaRouteRuleRW(d.base)
	DefaultDB.migrateTableRouteRW = rule.NewTableRouteRuleRW(d.base)
	DefaultDB.migrateColumnRouteRW = rule.NewColumnRouteRuleRW(d.base)
	DefaultDB.migrateTaskTableRW = rule.NewMigrateTaskTableRW(d.base)
	DefaultDB.migrateTableRuleRW = rule.NewDataMigrateRuleRW(d.base)
	DefaultDB.migrateSqlRuleRW = rule.NewSqlMigrateRuleRW(d.base)
	DefaultDB.taskRW = task.NewTaskRW(d.base)
	DefaultDB.taskLogRW = task.NewLogRW(d.base)
	DefaultDB.paramsRW = params.NewTaskParamsRW(d.base)
	DefaultDB.structMigrateTaskRW = task.NewStructMigrateTaskRW(d.base)
	DefaultDB.structMigrateSummaryRW = task.NewStructMigrateSummaryRW(d.base)
	DefaultDB.structMigrateTaskRuleRW = migrate.NewStructMigrateTaskRuleRW(d.base)
	DefaultDB.structMigrateSchemaRuleRW = migrate.NewStructMigrateSchemaRuleRW(d.base)
	DefaultDB.structMigrateTableRuleRW = migrate.NewStructMigrateTableRuleRW(d.base)
	DefaultDB.structMigrateColumnRuleRW = migrate.NewStructMigrateColumnRuleRW(d.base)
	DefaultDB.structMigrateTableAttrsRuleRW = migrate.NewStructMigrateTableAttrsRuleRW(d.base)
	DefaultDB.buildinDatatypeRuleRW = buildin.NewBuildinDatatypeRuleRW(d.base)
	DefaultDB.buildinRuleRecordRW = buildin.NewBuildinRuleRecordRW(d.base)
	DefaultDB.buildinDefaultValueRW = buildin.NewBuildinDefaultValueRuleRW(d.base)
	DefaultDB.buildinCompatibleRW = buildin.NewBuildinCompatibleRuleRW(d.base)
	DefaultDB.dataMigrateTaskRW = task.NewDataMigrateTaskRW(d.base)
	DefaultDB.dataMigrateSummaryTask = task.NewDataMigrateSummaryRW(d.base)
	DefaultDB.sqlMigrateTaskRW = task.NewSqlMigrateTaskRW(d.base)
	DefaultDB.sqlMigrateSummaryTask = task.NewSqlMigrateSummaryRW(d.base)
	DefaultDB.dataCompareTaskRW = task.NewDataCompareTaskRW(d.base)
	DefaultDB.dataCompareRuleRW = rule.NewDataCompareRuleRW(d.base)
	DefaultDB.dataCompareSummaryTask = task.NewDataCompareSummaryRW(d.base)
	DefaultDB.assessMigrateTaskRW = task.NewAssessMigrateTaskRW(d.base)
	DefaultDB.structCompareTaskRW = task.NewStructCompareTaskRW(d.base)
	DefaultDB.structCompareSummaryRW = task.NewStructCompareSummaryRW(d.base)
	DefaultDB.dataScanTaskRW = task.NewDataScanTaskRW(d.base)
	DefaultDB.dataScanSummaryRW = task.NewDataScanSummaryRW(d.base)
	DefaultDB.dataScanRuleRW = rule.NewDataScanRuleRW(d.base)
}

func (d *database) migrateStream(models ...interface{}) (err error) {
	for _, m := range models {
		if err = d.base.Set("gorm:table_options", " ENGINE=InnoDB DEFAULT CHARACTER SET UTF8MB4 COLLATE UTF8MB4_GENERAL_CI").AutoMigrate(m); err != nil {
			return err
		}
	}
	return nil
}

func (d *database) migrateTables() (err error) {
	return d.migrateStream(
		new(datasource.Datasource),
		new(rule.MigrateTaskTable),
		new(rule.SchemaRouteRule),
		new(rule.TableRouteRule),
		new(rule.ColumnRouteRule),
		new(rule.DataMigrateRule),
		new(rule.SqlMigrateRule),
		new(task.Task),
		new(task.Log),
		new(task.StructMigrateSummary),
		new(task.StructMigrateTask),
		new(params.TaskDefaultParam),
		new(params.TaskCustomParam),
		new(migrate.TaskStructRule),
		new(migrate.SchemaStructRule),
		new(migrate.TableStructRule),
		new(migrate.ColumnStructRule),
		new(migrate.TableAttrsRule),
		new(buildin.BuildinDatatypeRule),
		new(buildin.BuildinRuleRecord),
		new(buildin.BuildinDefaultvalRule),
		new(buildin.BuildinCompatibleRule),
		new(task.DataMigrateTask),
		new(task.DataMigrateSummary),
		new(task.SqlMigrateTask),
		new(task.SqlMigrateSummary),
		new(task.DataCompareTask),
		new(task.DataCompareSummary),
		new(rule.DataCompareRule),
		new(task.AssessMigrateTask),
		new(task.StructCompareTask),
		new(task.StructCompareSummary),
		new(task.DataScanSummary),
		new(task.DataScanTask),
		new(rule.DataScanRule),
	)
}

func (d *database) initDatatypeRule(ctx context.Context) error {
	err := Transaction(ctx, func(txnCtx context.Context) error {
		record, err := GetBuildInRuleRecordRW().GetBuildInRuleRecord(ctx, constant.BuildInRuleNameColumnDatatype)
		if err != nil {
			return err
		}
		if !strings.EqualFold(record.RuleInit, constant.BuildInRuleInitSuccess) {
			_, err = GetIBuildInDatatypeRuleRW().CreateBuildInDatatypeRule(ctx, buildin.InitO2MBuildinDatatypeRule())
			if err != nil {
				return err
			}
			_, err = GetIBuildInDatatypeRuleRW().CreateBuildInDatatypeRule(ctx, buildin.InitO2TBuildinDatatypeRule())
			if err != nil {
				return err
			}
			_, err = GetIBuildInDatatypeRuleRW().CreateBuildInDatatypeRule(ctx, buildin.InitM2OBuildinDatatypeRule())
			if err != nil {
				return err
			}
			_, err = GetIBuildInDatatypeRuleRW().CreateBuildInDatatypeRule(ctx, buildin.InitT2OBuildinDatatypeRule())
			if err != nil {
				return err
			}
			_, err = GetBuildInRuleRecordRW().CreateBuildInRuleRecord(ctx, &buildin.BuildinRuleRecord{
				RuleName: constant.BuildInRuleNameColumnDatatype,
				RuleInit: constant.BuildInRuleInitSuccess,
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (d *database) initDefaultValueRule(ctx context.Context) error {
	err := Transaction(ctx, func(txnCtx context.Context) error {
		record, err := GetBuildInRuleRecordRW().GetBuildInRuleRecord(ctx, constant.BuildInRuleNameColumnDefaultValue)
		if err != nil {
			return err
		}
		if !strings.EqualFold(record.RuleInit, constant.BuildInRuleInitSuccess) {
			_, err = GetBuildInDefaultValueRuleRW().CreateBuildInDefaultValueRule(ctx, buildin.InitO2MTBuildinDefaultValue())
			if err != nil {
				return err
			}
			_, err = GetBuildInDefaultValueRuleRW().CreateBuildInDefaultValueRule(ctx, buildin.InitMT2OBuildinDefaultValue())
			if err != nil {
				return err
			}
			_, err = GetBuildInRuleRecordRW().CreateBuildInRuleRecord(ctx, &buildin.BuildinRuleRecord{
				RuleName: constant.BuildInRuleNameColumnDefaultValue,
				RuleInit: constant.BuildInRuleInitSuccess,
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (d *database) initCompatibleRule(ctx context.Context) error {
	err := Transaction(ctx, func(txnCtx context.Context) error {
		record, err := GetBuildInRuleRecordRW().GetBuildInRuleRecord(ctx, constant.BuildInRuleNameObjectCompatible)
		if err != nil {
			return err
		}
		if !strings.EqualFold(record.RuleInit, constant.BuildInRuleInitSuccess) {
			_, err = GetBuildInCompatibleRuleRW().CreateBuildInCompatibleRule(ctx, buildin.InitO2MBuildinCompatibleRule())
			if err != nil {
				return err
			}
			_, err = GetBuildInCompatibleRuleRW().CreateBuildInCompatibleRule(ctx, buildin.InitO2TBuildinCompatibleRule())
			if err != nil {
				return err
			}
			_, err = GetBuildInRuleRecordRW().CreateBuildInRuleRecord(ctx, &buildin.BuildinRuleRecord{
				RuleName: constant.BuildInRuleNameObjectCompatible,
				RuleInit: constant.BuildInRuleInitSuccess,
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (d *Database) String() string {
	jsonByte, _ := json.Marshal(d)
	return stringutil.BytesToString(jsonByte)
}

// Transaction
// @Description: Transaction for service
// @Parameter ctx
// @Parameter fc
// @return error
func Transaction(ctx context.Context, fc func(txnCtx context.Context) error) (err error) {
	if DefaultDB.base == nil {
		return fc(ctx)
	}
	db := DefaultDB.base.WithContext(ctx)

	return db.Transaction(func(tx *gorm.DB) error {
		return fc(common.CtxWithTransaction(ctx, tx))
	})
}

func GetIDatasourceRW() datasource.IDatasource {
	return DefaultDB.datasourceRW
}

func GetIMigrateSchemaRouteRW() rule.ISchemaRouteRule {
	return DefaultDB.migrateSchemaRouteRW
}

func GetIMigrateTaskTableRW() rule.IMigrateTaskTable {
	return DefaultDB.migrateTaskTableRW
}

func GetIMigrateTableRouteRW() rule.ITableRouteRule {
	return DefaultDB.migrateTableRouteRW
}

func GetIMigrateColumnRouteRW() rule.IColumnRouteRule {
	return DefaultDB.migrateColumnRouteRW
}

func GetIDataMigrateRuleRW() rule.IDataMigrateRule {
	return DefaultDB.migrateTableRuleRW
}

func GetISqlMigrateRuleRW() rule.ISqlMigrateRule {
	return DefaultDB.migrateSqlRuleRW
}

func GetITaskRW() task.ITask {
	return DefaultDB.taskRW
}

func GetITaskLogRW() task.ILog {
	return DefaultDB.taskLogRW
}

func GetIStructMigrateTaskRW() task.IStructMigrateTask {
	return DefaultDB.structMigrateTaskRW
}

func GetIStructMigrateSummaryRW() task.IStructMigrateSummary {
	return DefaultDB.structMigrateSummaryRW
}

func GetIStructCompareTaskRW() task.IStructCompareTask {
	return DefaultDB.structCompareTaskRW
}

func GetIStructCompareSummaryRW() task.IStructCompareSummary {
	return DefaultDB.structCompareSummaryRW
}

func GetIParamsRW() params.IParams {
	return DefaultDB.paramsRW
}

func GetIStructMigrateTaskRuleRW() migrate.IStructMigrateTaskRule {
	return DefaultDB.structMigrateTaskRuleRW
}

func GetIStructMigrateSchemaRuleRW() migrate.IStructMigrateSchemaRule {
	return DefaultDB.structMigrateSchemaRuleRW
}

func GetIStructMigrateTableRuleRW() migrate.IStructMigrateTableRule {
	return DefaultDB.structMigrateTableRuleRW
}

func GetIStructMigrateColumnRuleRW() migrate.IStructMigrateColumnRule {
	return DefaultDB.structMigrateColumnRuleRW
}

func GetIStructMigrateTableAttrsRuleRW() migrate.IStructMigrateTableAttrsRule {
	return DefaultDB.structMigrateTableAttrsRuleRW
}

func GetBuildInRuleRecordRW() buildin.IBuildInRuleRecord {
	return DefaultDB.buildinRuleRecordRW
}

func GetIBuildInDatatypeRuleRW() buildin.IBuildInDatatypeRule {
	return DefaultDB.buildinDatatypeRuleRW
}

func GetBuildInDefaultValueRuleRW() buildin.IBuildInDefaultValueRule {
	return DefaultDB.buildinDefaultValueRW
}

func GetBuildInCompatibleRuleRW() buildin.IBuildInCompatibleRule {
	return DefaultDB.buildinCompatibleRW
}

func GetIDataMigrateTaskRW() task.IDataMigrateTask {
	return DefaultDB.dataMigrateTaskRW
}

func GetIDataMigrateSummaryRW() task.IDataMigrateSummary {
	return DefaultDB.dataMigrateSummaryTask
}

func GetISqlMigrateTaskRW() task.ISqlMigrateTask {
	return DefaultDB.sqlMigrateTaskRW
}

func GetISqlMigrateSummaryRW() task.ISqlMigrateSummary {
	return DefaultDB.sqlMigrateSummaryTask
}

func GetIDataCompareTaskRW() task.IDataCompareTask {
	return DefaultDB.dataCompareTaskRW
}

func GetIDataCompareRuleRW() rule.IDataCompareRule {
	return DefaultDB.dataCompareRuleRW
}

func GetIDataCompareSummaryRW() task.IDataCompareSummary {
	return DefaultDB.dataCompareSummaryTask
}

func GetIAssessMigrateTaskRW() task.IAssessMigrateTask {
	return DefaultDB.assessMigrateTaskRW
}

func GetIDataScanSummaryRW() task.IDataScanSummary {
	return DefaultDB.dataScanSummaryRW
}

func GetIDataScanTaskRW() task.IDataScanTask {
	return DefaultDB.dataScanTaskRW
}

func GetIDataScanRuleRW() rule.IDataScanRule {
	return DefaultDB.dataScanRuleRW
}
