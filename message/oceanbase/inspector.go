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
package oceanbase

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Constraint struct {
	Task                 *task.Task
	SchemaRoute          *rule.SchemaRouteRule
	DbTypeT              string
	DatabaseS, DatabaseT database.IDatabase
	TaskTables           []string
	TableThread          int
	TableRoutes          []*rule.TableRouteRule
	validUpstream        validUpstream
	validDownstream      validDownstream
}

type validUpstream struct {
	mu          sync.Mutex
	validTables []string
}

func (v *validUpstream) append(t ...string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.validTables = append(v.validTables, t...)
}

func (v *validUpstream) get() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.validTables
}

type validDownstream struct {
	mu          sync.Mutex
	validTables []string
}

func (v *validDownstream) append(t ...string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.validTables = append(v.validTables, t...)
}

func (v *validDownstream) get() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.validTables
}

func (c *Constraint) Inspect(ctx context.Context) error {
	timeS := time.Now()
	logger.Info("inspect database constraint",
		zap.String("task_name", c.Task.TaskName),
		zap.String("task_mode", c.Task.TaskMode),
		zap.String("task_flow", c.Task.TaskFlow),
		zap.String("schema_name_s", c.SchemaRoute.SchemaNameS),
		zap.String("schema_name_t", c.SchemaRoute.SchemaNameT),
		zap.Time("start_time", timeS))
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := c.InspectUpstream(gCtx); err != nil {
			return err
		}
		return nil
	})
	g.Go(func() error {
		if err := c.InspectDownstream(gCtx); err != nil {
			return err
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	logger.Info("inspect database constraint completed",
		zap.String("task_name", c.Task.TaskName),
		zap.String("task_mode", c.Task.TaskMode),
		zap.String("task_flow", c.Task.TaskFlow),
		zap.String("schema_name_s", c.SchemaRoute.SchemaNameS),
		zap.String("schema_name_t", c.SchemaRoute.SchemaNameT),
		zap.Duration("cost", time.Now().Sub(timeS)))
	return nil
}

// TiDB TiCDC index-value dispatcher update event compatible
// https://docs.pingcap.com/zh/tidb/dev/ticdc-split-update-behavior
// v6.5 [>=v6.5.5] tidb database version greater than v6.5.5 and less than v7.0.0 All versions are supported normally
// v7 version and above [>=v7.1.2] all versions of the tidb database version greater than v7.1.2 can be supported normally
func (c *Constraint) InspectUpstream(ctx context.Context) error {
	version, err := c.DatabaseS.GetDatabaseVersion()
	if err != nil {
		return err
	}

	logger.Info("inspect upstream database",
		zap.String("task_name", c.Task.TaskName),
		zap.String("task_mode", c.Task.TaskMode),
		zap.String("task_flow", c.Task.TaskFlow),
		zap.String("schema_name_s", c.SchemaRoute.SchemaNameS),
		zap.String("version", version),
		zap.String("inspect", "with valid index column"))

	if stringutil.VersionOrdinal(version) < stringutil.VersionOrdinal("2.2.73.0") {
		return fmt.Errorf("the current database version [%s] does not support real-time synchronization based on the message queue + hash distribution mode. The order of DML row changes within a transaction is not guaranteed. There may be cause data correctness issues. please choose other methods for data synchronization, details: https://www.oceanbase.com/docs/enterprise-oms-doc-cn-1000000001781598", version)
	}

	pkTables, err := c.DatabaseS.GetDatabaseSchemaPrimaryTables(c.SchemaRoute.SchemaNameS)
	if err != nil {
		return err
	}

	c.validUpstream.append(pkTables...)

	noValidTables := stringutil.StringItemsFilterDifference(c.TaskTables, c.validUpstream.get())

	if len(noValidTables) == 0 {
		return nil
	}

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(c.TableThread)
	for _, tab := range noValidTables {
		t := tab
		g.Go(func() error {
			isValid, err := c.DatabaseS.GetDatabaseSchemaTableValidIndex(c.SchemaRoute.SchemaNameS, t)
			if err != nil {
				return err
			}
			if isValid {
				c.validUpstream.append(t)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	doubleCValidTables := stringutil.StringItemsFilterDifference(c.TaskTables, c.validUpstream.get())
	if len(doubleCValidTables) > 0 {
		return fmt.Errorf("the upstream database tables [%v] does not meet the data synchronization requirements. Data synchronization requirements must have a primary key or a non-null unique key", stringutil.StringJoin(doubleCValidTables, constant.StringSeparatorComma))
	}
	return nil
}

func (c *Constraint) InspectDownstream(ctx context.Context) error {
	logger.Info("inspect downstream database",
		zap.String("task_name", c.Task.TaskName),
		zap.String("task_mode", c.Task.TaskMode),
		zap.String("task_flow", c.Task.TaskFlow),
		zap.String("schema_name_t", c.SchemaRoute.SchemaNameT),
		zap.String("inspect", "with valid index column"))
	var (
		taskTables []string
	)
	for _, t := range c.TaskTables {
		var tableName string
		switch c.Task.CaseFieldRuleS {
		case constant.ParamValueRuleCaseFieldNameUpper:
			tableName = strings.ToUpper(t)
		case constant.ParamValueRuleCaseFieldNameLower:
			tableName = strings.ToLower(t)
		default:
			tableName = t
		}
		taskTables = append(taskTables, tableName)
	}

	oriTableRoutes := make(map[string]string)
	revTableRoutes := make(map[string]string)

	for _, s := range c.TableRoutes {
		revTableRoutes[s.TableNameT] = s.TableNameS
		oriTableRoutes[s.TableNameS] = s.TableNameT
	}

	pkTables, err := c.DatabaseT.GetDatabaseSchemaPrimaryTables(c.SchemaRoute.SchemaNameT)
	if err != nil {
		return err
	}

	for _, t := range pkTables {
		var tableName string
		switch c.Task.CaseFieldRuleT {
		case constant.ParamValueRuleCaseFieldNameUpper:
			tableName = strings.ToUpper(t)
		case constant.ParamValueRuleCaseFieldNameLower:
			tableName = strings.ToLower(t)
		default:
			tableName = t
		}

		if tableNameS, exits := revTableRoutes[tableName]; exits {
			c.validDownstream.append(tableNameS)
		}
	}

	noValidTables := stringutil.StringItemsFilterDifference(taskTables, c.validDownstream.get())

	if len(noValidTables) == 0 {
		return nil
	}

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(c.TableThread)
	for _, tab := range noValidTables {
		t := tab
		g.Go(func() error {
			var tableName string
			if val, exist := oriTableRoutes[t]; exist {
				tableName = val
			} else {
				tableName = t
			}
			isValid, err := c.DatabaseT.GetDatabaseSchemaTableValidIndex(c.SchemaRoute.SchemaNameT, tableName)
			if err != nil {
				return err
			}
			if isValid {
				c.validDownstream.append(t)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	doubleCValidTables := stringutil.StringItemsFilterDifference(taskTables, c.validDownstream.get())
	if len(doubleCValidTables) > 0 {
		var revTables []string
		for _, d := range doubleCValidTables {
			if val, ok := oriTableRoutes[d]; ok {
				revTables = append(revTables, val)
			} else {
				revTables = append(revTables, d)
			}
		}
		return fmt.Errorf("the dowstream database tables [%v] does not meet the data synchronization requirements. data synchronization requirements must have a primary key or a non-null unique key", stringutil.StringJoin(revTables, constant.StringSeparatorComma))
	}
	return nil
}

/*
the database table metadata
*/
type Metadata struct {
	TaskName       string
	TaskFlow       string
	TaskMode       string
	SchemaNameS    string
	SchemaNameT    string
	TaskTables     []string
	TableThread    int
	TableRoutes    []*rule.TableRouteRule
	CaseFieldRuleS string
	CaseFieldRuleT string
	DatabaseS      database.IDatabase
	DatabaseT      database.IDatabase
}

func (m *Metadata) GenUpstream(ctx context.Context) error {
	logger.Info("get upstream metadata",
		zap.String("task_name", m.TaskName),
		zap.String("task_mode", m.TaskMode),
		zap.String("task_flow", m.TaskFlow),
		zap.String("schema_name_s", m.SchemaNameS))

	startTime := time.Now()

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(m.TableThread)

	for _, tab := range m.TaskTables {
		t := tab
		g.Go(func() error {
			timeS := time.Now()

			md := &metadata{
				tableColumns: make(map[string]*column),
			}

			var tableName string
			switch m.CaseFieldRuleS {
			case constant.ParamValueRuleCaseFieldNameUpper:
				tableName = strings.ToUpper(t)
			case constant.ParamValueRuleCaseFieldNameLower:
				tableName = strings.ToLower(t)
			default:
				tableName = t
			}

			logger.Info("get upstream metadata",
				zap.String("task_name", m.TaskName),
				zap.String("task_mode", m.TaskMode),
				zap.String("task_flow", m.TaskFlow),
				zap.String("schema_name_s", m.SchemaNameS),
				zap.String("table_name_s", tableName))

			res, err := m.DatabaseS.GetDatabaseTableColumnMetadata(m.SchemaNameS, tableName)
			if err != nil {
				return err
			}

			for _, r := range res {
				var (
					columnName  string
					isGenerated bool
				)
				switch m.CaseFieldRuleT {
				case constant.ParamValueRuleCaseFieldNameUpper:
					columnName = strings.ToUpper(r["COLUMN_NAME"])
				case constant.ParamValueRuleCaseFieldNameLower:
					columnName = strings.ToLower(r["COLUMN_NAME"])
				default:
					columnName = r["COLUMN_NAME"]
				}
				dataL, err := stringutil.StrconvIntBitSize(r["DATA_LENGTH"], 64)
				if err != nil {
					return fmt.Errorf("strconv data_length [%s] failed: [%v]", r["DATA_LENGTH"], err)
				}

				if strings.EqualFold(r["IS_GENERATED"], "YES") {
					isGenerated = true
				}

				md.setColumn(columnName, &column{
					columnName: columnName,
					columnType: r["DATA_TYPE"],
					dataLength: int(dataL),
					isGeneraed: isGenerated,
				})
			}

			md.setTable(m.SchemaNameS, tableName)

			upMetaCache.Set(m.SchemaNameS, tableName, md)

			logger.Info("get upstream metadata",
				zap.String("task_name", m.TaskName),
				zap.String("task_mode", m.TaskMode),
				zap.String("task_flow", m.TaskFlow),
				zap.String("schema_name_s", m.SchemaNameS),
				zap.String("table_name_s", tableName),
				zap.Duration("cost", time.Now().Sub(timeS)))
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	logger.Info("get upstream metadata completed",
		zap.String("task_name", m.TaskName),
		zap.String("task_mode", m.TaskMode),
		zap.String("task_flow", m.TaskFlow),
		zap.String("schema_name_s", m.SchemaNameS),
		zap.Duration("cost", time.Now().Sub(startTime)))
	return nil
}

func (m *Metadata) GenDownstream(ctx context.Context) error {
	logger.Info("get downstream metadata",
		zap.String("task_name", m.TaskName),
		zap.String("task_mode", m.TaskMode),
		zap.String("task_flow", m.TaskFlow),
		zap.String("schema_name_t", m.SchemaNameT))

	startTime := time.Now()

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(m.TableThread)

	for _, tab := range m.TaskTables {
		t := tab
		g.Go(func() error {
			timeS := time.Now()

			md := &metadata{
				tableColumns: make(map[string]*column),
			}

			var tableName string
			for _, r := range m.TableRoutes {
				if m.SchemaNameS == r.SchemaNameS && t == r.TableNameS && m.SchemaNameT == r.SchemaNameT && !strings.EqualFold(r.TableNameT, "") {
					tableName = r.TableNameT
					break
				}
			}
			if strings.EqualFold(tableName, "") {
				switch m.CaseFieldRuleT {
				case constant.ParamValueRuleCaseFieldNameUpper:
					tableName = strings.ToUpper(t)
				case constant.ParamValueRuleCaseFieldNameLower:
					tableName = strings.ToLower(t)
				default:
					tableName = t
				}
			}

			logger.Info("get downstream metadata",
				zap.String("task_name", m.TaskName),
				zap.String("task_mode", m.TaskMode),
				zap.String("task_flow", m.TaskFlow),
				zap.String("schema_name_t", m.SchemaNameT),
				zap.String("table_name_t", tableName))

			res, err := m.DatabaseT.GetDatabaseTableColumnInfo(m.SchemaNameT, tableName)
			if err != nil {
				return err
			}

			for _, r := range res {
				var (
					columnName  string
					isGenerated bool
				)
				switch m.CaseFieldRuleT {
				case constant.ParamValueRuleCaseFieldNameUpper:
					columnName = strings.ToUpper(r["COLUMN_NAME"])
				case constant.ParamValueRuleCaseFieldNameLower:
					columnName = strings.ToLower(r["COLUMN_NAME"])
				default:
					columnName = r["COLUMN_NAME"]
				}
				dataL, err := stringutil.StrconvIntBitSize(r["DATA_LENGTH"], 64)
				if err != nil {
					return fmt.Errorf("strconv data_length [%s] failed: [%v]", r["DATA_LENGTH"], err)
				}

				if strings.EqualFold(r["IS_GENERATED"], "YES") {
					isGenerated = true
				}

				md.setColumn(columnName, &column{
					columnName: columnName,
					columnType: r["DATA_TYPE"],
					dataLength: int(dataL),
					isGeneraed: isGenerated,
				})
			}

			md.setTable(m.SchemaNameT, tableName)

			downMetaCache.Set(m.SchemaNameT, tableName, md)

			logger.Info("get downstream metadata",
				zap.String("task_name", m.TaskName),
				zap.String("task_mode", m.TaskMode),
				zap.String("task_flow", m.TaskFlow),
				zap.String("schema_name_t", m.SchemaNameT),
				zap.String("table_name_t", tableName),
				zap.Duration("cost", time.Now().Sub(timeS)))
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	logger.Info("get downstream metadata completed",
		zap.String("task_name", m.TaskName),
		zap.String("task_mode", m.TaskMode),
		zap.String("task_flow", m.TaskFlow),
		zap.String("schema_name_t", m.SchemaNameT),
		zap.Duration("cost", time.Now().Sub(startTime)))
	return nil
}
