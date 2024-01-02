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
	migrateTaskRuleRW             rule.IMigrateTaskRule
	migrateSchemaRouteRW          rule.ISchemaRouteRule
	migrateTableRouteRW           rule.ITableRouteRule
	migrateColumnRouteRW          rule.IColumnRouteRule
	migrateTaskTableRW            rule.IMigrateTaskTable
	taskRW                        task.ITask
	taskLogRW                     task.ILog
	structMigrateTaskRW           task.IStructMigrateTask
	dataMigrateTaskRW             task.IDataMigrateTask
	paramsRW                      params.IParams
	structMigrateTaskRuleRW       migrate.IStructMigrateTaskRule
	structMigrateSchemaRuleRW     migrate.IStructMigrateSchemaRule
	structMigrateTableRuleRW      migrate.IStructMigrateTableRule
	structMigrateColumnRuleRW     migrate.IStructMigrateColumnRule
	structMigrateTableAttrsRuleRW migrate.IStructMigrateTableAttrsRule
	buildinDatatypeRuleRW         buildin.IBuildInDatatypeRule
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
func CreateDatabaseConnection(cfg *Database, logLevel string) error {
	dsn := buildMysqlDSN(cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Schema)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		PrepareStmt:                              true,
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

	startTime := time.Now()
	logger.Info("database table migrate starting", zap.String("database", cfg.Schema), zap.String("startTime", startTime.String()))
	err = DefaultDB.migrateTables()
	if err != nil {
		return fmt.Errorf("database [%s] migrate tables failed, database error: [%v]", cfg.Schema, err)
	}

	endTime := time.Now()
	logger.Info("database table migrate end", zap.String("database", cfg.Schema), zap.String("endTime", endTime.String()), zap.String("cost", time.Now().Sub(startTime).String()))
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

func (p *database) initReaderWriters() {
	DefaultDB.datasourceRW = datasource.NewDatasourceRW(p.base)
	DefaultDB.migrateTaskRuleRW = rule.NewMigrateTaskRuleRW(p.base)
	DefaultDB.migrateSchemaRouteRW = rule.NewSchemaRouteRuleRW(p.base)
	DefaultDB.migrateTableRouteRW = rule.NewTableRouteRuleRW(p.base)
	DefaultDB.migrateColumnRouteRW = rule.NewColumnRouteRuleRW(p.base)
	DefaultDB.migrateTaskTableRW = rule.NewMigrateTaskTableRW(p.base)
	DefaultDB.taskRW = task.NewTaskRW(p.base)
	DefaultDB.taskLogRW = task.NewLogRW(p.base)
	DefaultDB.structMigrateTaskRW = task.NewStructMigrateTaskRW(p.base)
	DefaultDB.dataMigrateTaskRW = task.NewDataMigrateTaskRW(p.base)
	DefaultDB.paramsRW = params.NewTaskParamsRW(p.base)
	DefaultDB.structMigrateTaskRuleRW = migrate.NewStructMigrateTaskRuleRW(p.base)
	DefaultDB.structMigrateSchemaRuleRW = migrate.NewStructMigrateSchemaRuleRW(p.base)
	DefaultDB.structMigrateTableRuleRW = migrate.NewStructMigrateTableRuleRW(p.base)
	DefaultDB.structMigrateColumnRuleRW = migrate.NewStructMigrateColumnRuleRW(p.base)
	DefaultDB.structMigrateTableAttrsRuleRW = migrate.NewStructMigrateTableAttrsRuleRW(p.base)
	DefaultDB.buildinDatatypeRuleRW = buildin.NewBuildinDatatypeRuleRW(p.base)
}

func (p *database) migrateStream(models ...interface{}) (err error) {
	for _, m := range models {
		if err = p.base.Set("gorm:table_options", " ENGINE=InnoDB DEFAULT CHARACTER SET UTF8MB4 COLLATE UTF8MB4_GENERAL_CI").AutoMigrate(m); err != nil {
			return err
		}
	}
	return nil
}

func (p *database) migrateTables() (err error) {
	return p.migrateStream(
		new(datasource.Datasource),
		new(rule.MigrateTaskRule),
		new(rule.MigrateTaskTable),
		new(rule.SchemaRouteRule),
		new(rule.TableRouteRule),
		new(rule.ColumnRouteRule),
		new(task.Task),
		new(task.Log),
		new(task.StructMigrateTask),
		new(task.DataMigrateTask),
		new(params.TaskDefaultParam),
		new(params.TaskCustomParam),
		new(migrate.TaskStructRule),
		new(migrate.SchemaStructRule),
		new(migrate.TableStructRule),
		new(migrate.ColumnStructRule),
		new(migrate.TableAttrsRule),
		new(buildin.BuildinDatatypeRule),
	)
}

func (d *Database) String() string {
	jsonByte, _ := json.Marshal(d)
	return string(jsonByte)
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

func GetIMigrateTaskRuleRW() rule.IMigrateTaskRule {
	return DefaultDB.migrateTaskRuleRW
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

func GetITaskRW() task.ITask {
	return DefaultDB.taskRW
}

func GetITaskLogRW() task.ILog {
	return DefaultDB.taskLogRW
}

func GetIStructMigrateTaskRW() task.IStructMigrateTask {
	return DefaultDB.structMigrateTaskRW
}

func GetIDataMigrateTaskRW() task.IDataMigrateTask {
	return DefaultDB.dataMigrateTaskRW
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

func GetIBuildInDatatypeRuleRW() buildin.IBuildInDatatypeRule {
	return DefaultDB.buildinDatatypeRuleRW
}
