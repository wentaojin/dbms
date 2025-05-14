/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://wwc.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package tidb

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/message"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/consume"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Consumer represents a specific consumer
type Consumer struct {
	task        *task.Task
	cancelCtx   context.Context
	cancelFn    context.CancelFunc
	serverAddrs []string
	topic       string
	partition   int
	checkpoint  uint64
	kafkaOffset int64

	consumer     *kafka.Reader
	needContinue bool

	// Messages are stored in groups according to database tables
	// schemaName.tableName -> EventGroup
	eventGroups map[string]*EventGroup
	wg          *sync.WaitGroup

	decoder RowEventDecoder

	// Avoid frequent metadata refreshes due to no data before resolvedTs. Check whether there is data before <= resolvedTs.
	lastCommitTime time.Time

	schemaRoute   *rule.SchemaRouteRule
	tableRoute    []*rule.TableRouteRule
	columnRoute   []*rule.ColumnRouteRule
	consumeParam  *pb.CdcConsumeParam
	databaseS     database.IDatabase
	databaseT     database.IDatabase
	dbTypeT       string
	consumeTables []string
}

func (cg *ConsumerGroup) ConsumeMessage(c *Consumer) (bool, error) {
	msg, err := c.consumer.ReadMessage(c.cancelCtx)
	if err != nil {
		return false, fmt.Errorf("read message failed: [%v]", err)
	}

	needCommit, err := cg.WriteMessage(c, msg)
	if err != nil {
		return false, fmt.Errorf("write message failed: [%v]", err)
	}

	// update message count, byte count, and accumulate total latency of all message consumption
	cg.progress.UpdateMsgConsumeCounts(c.partition, 1)
	cg.progress.UpdateMsgConsumeBytes(c.partition, uint64(len(msg.Value)))
	cg.progress.UpdateMsgConsumeDelay(c.partition, time.Since(msg.Time))

	if needCommit {
		if err := cg.CommitMessage(c, msg); err != nil {
			return false, fmt.Errorf("commit message failed: [%v]", err)
		}
		return true, nil
	}
	return false, nil
}

// WriteMessage is to decode kafka message to event.
func (cg *ConsumerGroup) WriteMessage(c *Consumer, msg kafka.Message) (bool, error) {
	var (
		err      error
		key      = msg.Key
		value    = msg.Value
		partCode = msg.Partition
	)

	if c.partition != partCode {
		logger.Warn("message dispatched to wrong partition",
			zap.String("task_name", c.task.TaskName),
			zap.String("task_flow", c.task.TaskFlow),
			zap.String("task_mode", c.task.TaskMode),
			zap.String("topic", c.topic),
			zap.Int("partition", c.partition),
			zap.Int("expected", c.partition),
			zap.Int64("offset", msg.Offset),
			zap.String("key", string(msg.Key)),
			zap.String("value", string(msg.Value)))
		return false, fmt.Errorf("the message dispatched to wrong partition [%d], the consumer assign partitions [%v]", partCode, c.partition)
	}

	if err = c.decoder.AddKeyValue(key, value); err != nil {
		return false, fmt.Errorf("the topic [%s] partiton [%d] key [%s] value [%s] offset [%d] message decoder failed: %v", msg.Topic, msg.Partition, string(msg.Key), string(msg.Value), msg.Offset, err)
	}

	/*
		ticdc open protocol:
		1. Resolved events will be broadcasted to each MQ Partition periodically. Resolved events mean that any dmls events with TS less than the resolved event TS have been sent to the downstream
		2. DDL events will be broadcasted to each MQ Partition, and it is ensured that all dmls events with TS less than the ddl event TS have been sent to the corresponding downstream

		based on ticdc open protocol:
		1. any partition that receives a DDL Event will suspend consumption and enter the DDL coordination phase, waiting for all partitions to receive the corresponding DDL Event, and then flush all DMLs <= DDL Event CommitTs, and the DDL coordinator executes DDL
		2. any partition that receives a ResolvedTs Event will flush DMLs and continue consumption if it is not in the DDL coordination phase. If it is in the DDL coordination phase and is less than min ddl commitTs, it will not flush DMLs and continue consumption and wait for DDL coordination. If it is in the DDL coordination phase but is greater than min ddl commitTs, it will exit with an error. This phenomenon should not occur. Normally, consumption will be suspended before the DDL Event
	*/
	needFlush := false

	// batch single key-value message
	for {
		msgEventType, hasNext, err := c.decoder.HasNext()
		if err != nil {
			return false, fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] partiton [%d] key [%s] value [%s] offset [%d] message next failed: %v",
				c.task.TaskName, c.task.TaskFlow, c.task.TaskMode, msg.Topic, msg.Partition, string(msg.Key), string(msg.Value), msg.Offset, err)
		}

		if !hasNext {
			break
		}

		switch msgEventType {
		case MsgEventTypeDDL:
			ddl, err := c.decoder.NextDDLEvent()
			if err != nil {
				return false, fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] partiton [%d] key [%s] value [%s] offset [%d] decode ddl event message failed: %v",
					c.task.TaskName, c.task.TaskFlow, c.task.TaskMode, msg.Topic, msg.Partition, string(msg.Key), string(msg.Value), msg.Offset, err)
			}

			if cg.ObsoleteMessages(ddl.CommitTs, c.checkpoint) {
				logger.Warn("ddl message received",
					zap.String("task_name", c.task.TaskName),
					zap.String("task_flow", c.task.TaskFlow),
					zap.String("task_mode", c.task.TaskMode),
					zap.String("topic", c.topic),
					zap.Int("partition", c.partition),
					zap.Int64("offset", msg.Offset),
					zap.Uint64("commitTs", ddl.CommitTs),
					zap.String("ddl_type", ddl.DdlType.String()),
					zap.String("ddl_query", ddl.DdlQuery),
					zap.Uint64("checkpoint", c.checkpoint),
					zap.String("message_action", "less than or equal checkpoint, msg experied"))
				continue
			}

			if ddl.SchemaName == c.schemaRoute.SchemaNameS && stringutil.IsContainedString(c.consumeTables, ddl.TableName) {
				// DDL commitTs fallback, just crash it to indicate the bug.
				if cg.ddlWithMaxCommitTs != nil && ddl.CommitTs < cg.ddlWithMaxCommitTs.CommitTs {
					logger.Error("ddl event consume fallback",
						zap.String("task_name", c.task.TaskName),
						zap.String("task_flow", c.task.TaskFlow),
						zap.String("task_mode", c.task.TaskMode),
						zap.String("topic", c.topic),
						zap.Int("partition", c.partition),
						zap.Int64("offset", msg.Offset),
						zap.Uint64("commitTs", ddl.CommitTs),
						zap.String("ddl_type", ddl.DdlType.String()),
						zap.String("ddl_query", ddl.DdlQuery),
						zap.Uint64("checkpoint", c.checkpoint),
						zap.Int("coordinator", cg.Coordinators(ddl)),
						zap.String("message_action", "panic"))
					return false, fmt.Errorf("ddl event consume fallback, events [%v], indicate the bug, please contact author", ddl.String())
				}

				identified := QuoteSchemaTable(ddl.SchemaName, ddl.TableName)
				appendEle := &RowChangedEvent{
					SchemaName: ddl.SchemaName,
					TableName:  ddl.TableName,
					QueryType:  ddl.DdlType.String(),
					CommitTs:   ddl.CommitTs,
					IsDDL:      true,
					DdlQuery:   ddl.DdlQuery,
				}

				g, ok := c.eventGroups[identified]
				if !ok {
					group := NewEventGroup()
					group.Append(appendEle)
					c.eventGroups[identified] = group
				} else {
					g.Append(appendEle)
				}

				cg.AppendDDL(ddl, partCode)

				cg.Pause(c.partition)

				logger.Info("ddl message received",
					zap.String("task_name", c.task.TaskName),
					zap.String("task_flow", c.task.TaskFlow),
					zap.String("task_mode", c.task.TaskMode),
					zap.String("topic", c.topic),
					zap.Int("partition", c.partition),
					zap.Int64("offset", msg.Offset),
					zap.Uint64("commitTs", ddl.CommitTs),
					zap.String("ddl_type", ddl.DdlType.String()),
					zap.String("ddl_query", ddl.DdlQuery),
					zap.Uint64("checkpoint", c.checkpoint),
					zap.Int("coordinator", cg.Coordinators(ddl)),
					zap.String("message_action", "paused"))

				c.checkpoint = ddl.CommitTs
				c.kafkaOffset = msg.Offset

				// The partition that receives the DDL Event last is responsible for executing the DDL Event.
				if cg.IsEventDDLFlush(ddl) {
					logger.Info("ddl message received",
						zap.String("task_name", c.task.TaskName),
						zap.String("task_flow", c.task.TaskFlow),
						zap.String("task_mode", c.task.TaskMode),
						zap.String("topic", c.topic),
						zap.Int("partition", c.partition),
						zap.Int64("offset", msg.Offset),
						zap.Uint64("commitTs", ddl.CommitTs),
						zap.String("ddl_type", ddl.DdlType.String()),
						zap.String("ddl_query", ddl.DdlQuery),
						zap.Uint64("checkpoint", c.checkpoint),
						zap.Int("coordinator", cg.Coordinators(ddl)),
						zap.String("message_action", "flush"))
					needFlush = true
				} else {
					logger.Info("ddl message received",
						zap.String("task_name", c.task.TaskName),
						zap.String("task_flow", c.task.TaskFlow),
						zap.String("task_mode", c.task.TaskMode),
						zap.String("topic", c.topic),
						zap.Int("partition", c.partition),
						zap.Int64("offset", msg.Offset),
						zap.Uint64("commitTs", ddl.CommitTs),
						zap.String("ddl_type", ddl.DdlType.String()),
						zap.String("ddl_query", ddl.DdlQuery),
						zap.Uint64("checkpoint", c.checkpoint),
						zap.Int("coordinator", cg.Coordinators(ddl)),
						zap.String("message_action", "waiting flush"))
				}
			} else {
				logger.Warn("ddl message received",
					zap.String("task_name", c.task.TaskName),
					zap.String("task_flow", c.task.TaskFlow),
					zap.String("task_mode", c.task.TaskMode),
					zap.String("schema_name", ddl.SchemaName),
					zap.String("table_name", ddl.TableName),
					zap.String("topic", c.topic),
					zap.Int("partition", c.partition),
					zap.Int64("offset", msg.Offset),
					zap.Uint64("commitTs", ddl.CommitTs),
					zap.String("ddl_type", ddl.DdlType.String()),
					zap.String("ddl_query", ddl.DdlQuery),
					zap.Uint64("checkpoint", c.checkpoint),
					zap.Int("coordinator", cg.Coordinators(ddl)),
					zap.String("message_action", "skipped"))
			}
		case MsgEventTypeRow:
			dml, err := c.decoder.NextRowChangedEvent()
			if err != nil {
				return false, fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] partiton [%d] key [%s] value [%s] offset [%d] decode dml event message failed: %v",
					c.task.TaskName, c.task.TaskFlow, c.task.TaskMode, msg.Topic, msg.Partition, string(msg.Key), string(msg.Value), msg.Offset, err)
			}

			if cg.ObsoleteMessages(dml.CommitTs, c.checkpoint) {
				logger.Warn("dml event received",
					zap.String("task_name", c.task.TaskName),
					zap.String("task_flow", c.task.TaskFlow),
					zap.String("task_mode", c.task.TaskMode),
					zap.String("topic", c.topic),
					zap.Int("partition", c.partition),
					zap.Int64("offset", msg.Offset),
					zap.String("dml_events", dml.String()),
					zap.Uint64("checkpoint", c.checkpoint),
					zap.String("message_action", "less than or equal checkpoint, msg experied"))
				continue
			}

			if dml.SchemaName == c.schemaRoute.SchemaNameS && stringutil.IsContainedString(c.consumeTables, dml.TableName) {
				logger.Debug("dml event received",
					zap.String("task_name", c.task.TaskName),
					zap.String("task_flow", c.task.TaskFlow),
					zap.String("task_mode", c.task.TaskMode),
					zap.String("schema_name", dml.SchemaName),
					zap.String("table_name", dml.TableName),
					zap.String("topic", c.topic),
					zap.Int("partition", c.partition),
					zap.Int64("offset", msg.Offset),
					zap.Uint64("checkpoint", c.checkpoint),
					zap.String("dml_events", dml.String()),
					zap.String("message_action", "consumering"))

				identified := QuoteSchemaTable(dml.SchemaName, dml.TableName)

				g, ok := c.eventGroups[identified]
				if !ok {
					group := NewEventGroup()
					group.Append(dml)
					c.eventGroups[identified] = group
				} else {
					g.Append(dml)
				}
			} else {
				logger.Debug("dml event received",
					zap.String("task_name", c.task.TaskName),
					zap.String("task_flow", c.task.TaskFlow),
					zap.String("task_mode", c.task.TaskMode),
					zap.String("schema_name", dml.SchemaName),
					zap.String("table_name", dml.TableName),
					zap.String("topic", c.topic),
					zap.Int("partition", c.partition),
					zap.Int64("offset", msg.Offset),
					zap.Uint64("checkpoint", c.checkpoint),
					zap.String("dml_events", dml.String()),
					zap.String("message_action", "skipped"))
			}
		case MsgEventTypeResolved:
			resolvedTs, err := c.decoder.NextResolvedEvent()
			if err != nil {
				return false, fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] partiton [%d] key [%s] value [%s] offset [%d] decode resolved_ts event message failed: %v",
					c.task.TaskName, c.task.TaskFlow, c.task.TaskMode, msg.Topic, msg.Partition, string(msg.Key), string(msg.Value), msg.Offset, err)
			}

			if cg.ObsoleteMessages(resolvedTs, c.checkpoint) {
				logger.Warn("resolved event received",
					zap.String("task_name", c.task.TaskName),
					zap.String("task_flow", c.task.TaskFlow),
					zap.String("task_mode", c.task.TaskMode),
					zap.String("topic", c.topic),
					zap.Int("partition", c.partition),
					zap.Int64("offset", msg.Offset),
					zap.Uint64("checkpoint", c.checkpoint),
					zap.String("message_action", "less than or equal checkpoint, msg experied"))
				continue
			}

			logger.Debug("resolved event received",
				zap.String("task_name", c.task.TaskName),
				zap.String("task_flow", c.task.TaskFlow),
				zap.String("task_mode", c.task.TaskMode),
				zap.String("topic", c.topic),
				zap.Int("partition", c.partition),
				zap.Int64("offset", msg.Offset),
				zap.Uint64("checkpoint", c.checkpoint),
				zap.Uint64("resolvedTs", resolvedTs),
				zap.String("message_action", "consumering"))

			/*
				If there is no coordination, continue to flush according to partition consumption and retain the partition-level consumption site information
				If there is coordination, the judgment is classified according to the following situations
				1. If reoslvedTs < ddl taskqueue min commits, do not flush, continue to consume according to the partition, wait for all partition DDL Event message events to be received, and then automatically pause or consume flush according to the ddl flush requirement
				2. If reoslvedTs >= ddl taskqueue min commits, it means an exception. Normally, the corresponding partition should have consumed the DDL Event and triggered the suspension of consumption, and resolvedTs >= ddl taskqueue min commits should not appear

				如果没有协调，继续按照分区消费 flush 并保留分区级别消费站点信息
				如果有协调，则根据如下情况分类判断
				1，reoslvedTs < ddl taskqueue min commits，则不 flush，继续按照分区消费，等待收到所有分区 DDL Event 消息事件后再根据 ddl flush 要求自动暂停或者消费 flush
				2. reoslvedTs >= ddl taskqueue min commits 则说明异常，正常是对应分区应该已经消费 DDL Event 且触发暂停消费，不应该出现 resolvedTs >= ddl taskqueue min commits
			*/
			c.checkpoint = resolvedTs
			c.kafkaOffset = msg.Offset

			if cg.IsEventResolvedFlush(resolvedTs) || cg.GetDDL() == nil {
				rowChangedEventCounts := 0
				sortCommitEvs := make(map[string][]uint64)
				for t, group := range c.eventGroups {
					rows := len(group.events)
					if rows == 0 {
						continue
					}
					rowChangedEventCounts = rowChangedEventCounts + rows
					sortCommitEvs[t] = group.OrderSortedCommitTs()
				}

				if (rowChangedEventCounts == 0 && time.Since(c.lastCommitTime) >= time.Duration(c.consumeParam.IdleResolvedThreshold)*time.Second) || (rowChangedEventCounts > 0) {
					logger.Info("resolved event received",
						zap.String("task_name", c.task.TaskName),
						zap.String("task_flow", c.task.TaskFlow),
						zap.String("task_mode", c.task.TaskMode),
						zap.String("topic", c.topic),
						zap.Int("partition", c.partition),
						zap.Int64("offset", msg.Offset),
						zap.Uint64("checkpoint", c.checkpoint),
						zap.Uint64("resolved_ts", resolvedTs),
						zap.Int("dml_event_counts", rowChangedEventCounts),
						zap.Any("dml_event_ascs", sortCommitEvs),
						zap.Duration("since_seconds", time.Since(c.lastCommitTime)),
						zap.String("message_action", "flush"))
					needFlush = true
				}
			} else {
				switch {
				case resolvedTs < cg.GetDDL().CommitTs:
					needFlush = false
				default:
					logger.Error("resolved event received",
						zap.String("task_name", c.task.TaskName),
						zap.String("task_flow", c.task.TaskFlow),
						zap.String("task_mode", c.task.TaskMode),
						zap.String("topic", c.topic),
						zap.Int("partition", c.partition),
						zap.Int64("offset", msg.Offset),
						zap.Uint64("checkpoint", c.checkpoint),
						zap.String("min_ddl_commit_ts_event", cg.GetDDL().String()),
						zap.String("message_action", fmt.Sprintf("The corresponding partition [%v] should have consumed the DDL event and triggered the consumption suspension. The current situation where resolvedTs >= ddl taskqueue min commits should not appear", partCode)))

					return false, fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] corresponding partition [%v] should have consumed the ddl event and triggered the consumption suspension. the current situation where resolvedTs >= ddl taskqueue min commits should not appear", c.task.TaskName, c.task.TaskFlow, c.task.TaskMode, msg.Topic, partCode)
				}
			}
		default:
			return false, fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] partiton [%d] key [%s] value [%s] offset [%d] unknown message type [%v]",
				c.task.TaskName, c.task.TaskFlow, c.task.TaskMode, msg.Topic, msg.Partition, string(msg.Key), string(msg.Value), msg.Offset, msgEventType)
		}
	}

	return needFlush, nil
}

// ObsoleteMessages
// 1. Initially start filtering and filtration of synchronously consumed events
// 2. During the operation, the corresponding partition DDL/ResolvedTs Event is refreshed. The reason is that CDC guarantees that all events before the DDL/ResolvedTs Event have been sent, and there should be no more events smaller than the DDL/ResolvedTs Event Ts
func (cg *ConsumerGroup) ObsoleteMessages(currentTs, nowTs uint64) bool {
	return currentTs <= nowTs
}

func (cg *ConsumerGroup) CommitMessage(c *Consumer, msg kafka.Message) error {
	// The ticdc open protocol sends ddl events to all MQ partitions and ensures that all DMLs events <= ddl event Ts have been sent.
	// Any partition that receives a DDL event will suspend consumption.
	// Therefore, it is normal that multiple different DDLEvents will not enter coordination at the same time. Encountering multiple different DDL Events entering coordination is an unexpected phenomenon.
	var todoDDL *DDLChangedEvent

	if cg.ddls.Len() > 1 {
		return fmt.Errorf(`the ticdc open protocol sends ddl events to all MQ partitions and ensures that all DMLs events <= ddl event Ts have been sent. any partition that receives a DDL event will suspend consumption. therefore, it is normal that multiple different DDLEvents will not enter coordination at the same time. encountering multiple different DDL Events entering coordination is an unexpected phenomenon`)
	} else if cg.ddls.Len() == 1 {
		for {
			todoDDL = cg.GetDDL()

			if todoDDL == nil {
				break
			}

			// flush less than DDL CommitTs's DMLs
			if err := cg.flushRowChangedEventsBeforeDdl(c.cancelCtx); err != nil {
				return err
			}

			hash := md5.Sum([]byte(todoDDL.DdlQuery))

			md5String := hex.EncodeToString(hash[:])

			var (
				rewrite *consume.MsgDdlRewrite
				err     error
			)

			rewrite, err = model.GetIMsgDdlRewriteRW().GetMsgDdlRewrite(c.cancelCtx, &consume.MsgDdlRewrite{
				TaskName: c.task.TaskName,
				Topic:    msg.Topic,
				Digest:   md5String,
			})
			if err != nil {
				return err
			}

			// execute DDL Event
			if rewrite != nil {
				hs := md5.Sum([]byte(rewrite.RewriteDdlText))
				switch {
				case strings.EqualFold(rewrite.RewriteDdlText, ""):
					logger.Warn("ddl event coordinator skip",
						zap.String("task_name", c.task.TaskName),
						zap.String("task_flow", c.task.TaskFlow),
						zap.String("task_mode", c.task.TaskMode),
						zap.String("topic", msg.Topic),
						zap.String("ddl_digest", md5String),
						zap.String("ddl_events", todoDDL.String()),
						zap.String("message_action", "skip"))
				case rewrite.Digest == hex.EncodeToString(hs[:]):
					return fmt.Errorf("the dowstream database origin sql [%v] digest [%s] commit_ts [%d] exec failed, please decide whether to rewrite based on the error message and downstream database type [%s]. If you need to rewrite, please use digest to rewrite, error detail [%v]", todoDDL.DdlQuery, md5String, todoDDL.CommitTs, c.dbTypeT, err)
				default:
					if _, err := c.databaseT.ExecContext(c.cancelCtx, rewrite.RewriteDdlText); err != nil {
						return fmt.Errorf("the dowstream database rewrite sql [%v] digest [%s] commit_ts [%d] exec failed, please check whether the statement is rewritten correctly , error detail [%v]", todoDDL.DdlQuery, md5String, todoDDL.CommitTs, err)
					}
				}
			} else {
				if _, err := c.databaseT.ExecContext(c.cancelCtx, todoDDL.DdlQuery); err != nil {
					// rewrite default record origin ddl
					if _, err := model.GetIMsgDdlRewriteRW().CreateMsgDdlRewrite(c.cancelCtx, &consume.MsgDdlRewrite{
						TaskName:       c.task.TaskName,
						Topic:          c.topic,
						Digest:         md5String,
						OriginDdlText:  todoDDL.DdlQuery,
						RewriteDdlText: todoDDL.DdlQuery,
					}); err != nil {
						return fmt.Errorf("the dowstream database task [%s] topic [%s] origin sql [%v] digest [%s] commit_ts [%d] record failed, please retry or manual write msg_ddl_rewrite metadata, error detail [%v]", c.task.TaskName, c.topic, todoDDL.DdlQuery, md5String, todoDDL.CommitTs, err)
					}
					return fmt.Errorf("the dowstream database origin sql [%v] digest [%s] commit_ts [%d] exec failed, please decide whether to rewrite based on the error message and downstream database type [%s]. If you need to rewrite, please use digest to rewrite, error detail [%v]", todoDDL.DdlQuery, md5String, todoDDL.CommitTs, c.dbTypeT, err)
				}
			}
			if err := cg.flushDdlCheckpoint(c.cancelCtx); err != nil {
				return err
			}
			// metadata flush
			if err := c.updateDowstreamTableColumnMetadataCache(todoDDL.TableName, todoDDL.DdlType); err != nil {
				return err
			}

			cg.PopDDL()
			for _, p := range cg.Partitions(todoDDL) {
				logger.Warn("ddl event coordinator finished",
					zap.String("task_name", c.task.TaskName),
					zap.String("task_flow", c.task.TaskFlow),
					zap.String("task_mode", c.task.TaskMode),
					zap.String("topic", msg.Topic),
					zap.Int("partition", p),
					zap.String("ddl_digest", md5String),
					zap.String("ddl_events", todoDDL.String()),
					zap.String("message_action", "resume"))
				cg.Resume(c.partition)
			}
			cg.RemoveDDL(todoDDL)
		}
	}

	// flush <= resolvedTs DMLs
	var ddlCommitTs uint64
	if todoDDL != nil {
		ddlCommitTs = todoDDL.CommitTs
	}
	if err := c.flushRowChangedEventsBeforeResolvedTs(ddlCommitTs); err != nil {
		return err
	}
	return nil
}

// flushRowChangedEventsBeforeDdl flushes all row changed events before the DDL and and control it by the last receive ddl event consumer context
func (cg *ConsumerGroup) flushRowChangedEventsBeforeDdl(ctx context.Context) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(len(cg.Consumers()))

	for _, consumer := range cg.Consumers() {
		c := consumer
		g.Go(func() error {
			// partition wait sink events
			// store all the events that are less than or equal a certain ts in order according to the table latitude
			sinkEvents := make(map[string][]*RowChangedEvent)

			for tableName, group := range c.eventGroups {
				var events []*RowChangedEvent
				events = group.DDLCommitTs(c.checkpoint)
				if len(events) == 0 {
					continue
				}
				sinkEvents[tableName] = events
			}

			if len(sinkEvents) > 0 {
				if err := c.flushRowChangedEvents(gCtx, sinkEvents); err != nil {
					return err
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// flushDdlCheckpoint refresh ddl Checkpoint and control it by the last receive ddl event consumer context
func (cg *ConsumerGroup) flushDdlCheckpoint(ctx context.Context) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(len(cg.Consumers()))

	for _, consumer := range cg.Consumers() {
		c := consumer
		g.Go(func() error {
			if err := c.updateMetadataCheckpoint(gCtx); err != nil {
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func (c *Consumer) flushRowChangedEventsBeforeResolvedTs(ddlCommitTs uint64) error {
	// partition wait sink events
	// store all the events that are less than or equal a certain ts in order according to the table latitude
	sinkEvents := make(map[string][]*RowChangedEvent)

	for tableName, group := range c.eventGroups {
		var events []*RowChangedEvent
		if ddlCommitTs != 0 {
			group.RemoveDDLCommitTs(ddlCommitTs)
		}
		events = group.ResolvedTs(c.checkpoint)
		if len(events) == 0 {
			continue
		}
		sinkEvents[tableName] = events
	}

	counts := len(sinkEvents)
	if counts > 0 {
		if err := c.flushRowChangedEvents(c.cancelCtx, sinkEvents); err != nil {
			return err
		}
	} else {
		// solve the problem that data does not exist before resolvedTs and refresh checkpoints at intervals
		if err := c.updateMetadataCheckpoint(c.cancelCtx); err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) updateMetadataCheckpoint(ctx context.Context) error {
	if err := model.GetIMsgTopicPartitionRW().UpdateMsgTopicPartition(ctx, &consume.MsgTopicPartition{
		TaskName:   c.task.TaskName,
		Topic:      c.topic,
		Partitions: c.partition,
	}, map[string]interface{}{
		"Checkpoint": c.checkpoint,
		"Offset":     c.kafkaOffset,
	}); err != nil {
		return err
	}
	logger.Debug("commit message success",
		zap.String("task_name", c.task.TaskName),
		zap.String("task_flow", c.task.TaskFlow),
		zap.String("task_mode", c.task.TaskMode),
		zap.String("topic", c.topic),
		zap.Int("partition", c.partition),
		zap.Int64("offset", c.kafkaOffset),
		zap.Uint64("checkpoint", c.checkpoint))
	return nil
}

func (c *Consumer) flushRowChangedEvents(ctx context.Context, sinkEvents map[string][]*RowChangedEvent) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(int(c.consumeParam.TableThread))

	for _, es := range sinkEvents {
		events := es
		g.Go(func() error {
			for _, e := range events {
				if e.IsDDL {
					return fmt.Errorf("the DDL events need to be executed separately. After the execution is completed, the message should not appear here. Please contact the author")
				} else {
					logger.Info("message event sink",
						zap.String("task_name", c.task.TaskName),
						zap.String("task_flow", c.task.TaskFlow),
						zap.String("task_mode", c.task.TaskMode),
						zap.String("topic", c.topic),
						zap.Int("partition", c.partition),
						zap.Int64("offset", c.kafkaOffset),
						zap.String("schema_name", e.SchemaName),
						zap.String("table_name", e.TableName),
						zap.String("query_type", e.QueryType),
						zap.String("msg_events", e.String()))
					switch e.QueryType {
					case message.DMLUpdateQueryType:
						/*
							for specific compatibility instructions: https://docs.pingcap.com/tidb/dev/ticdc-split-update-behavior#release-65-compatibility,
							where the `Split UK/PK UPDATE events` column marked as ✅ does not have this problem

							1，UK/PK UPDATE events will not have UPDATE logic, and have been split by TiCDC to send DELETE/INSERT events
							2，NonUK/NonPK UPDATE events will display the UPDATE logic. TiCDC distributes the corresponding partitions according to PK/ UK NOT NULL
						*/
						if err := c.databaseT.Transaction(gCtx, nil, []func(ctx context.Context, tx *sql.Tx) error{
							func(ctx context.Context, tx *sql.Tx) error {
								sqlStr, sqlParas, err := e.Delete(
									c.dbTypeT,
									c.schemaRoute.SchemaNameT,
									c.tableRoute,
									c.columnRoute,
									c.task.CaseFieldRuleT)
								if err != nil {
									return err
								}
								if _, err := tx.ExecContext(ctx, sqlStr, sqlParas...); err != nil {
									return fmt.Errorf("the dowstream database topic [%s] partition [%d] offset [%d] exec sql [%v] parmas [%v] commit_ts [%d] failed: [%v]",
										c.topic, c.partition, c.kafkaOffset, sqlStr, sqlParas, e.CommitTs, err)
								}
								return nil
							},
							func(ctx context.Context, tx *sql.Tx) error {
								sqlStr, sqlParas, err := e.Insert(
									c.dbTypeT,
									c.schemaRoute.SchemaNameT,
									c.tableRoute,
									c.columnRoute,
									c.task.CaseFieldRuleT,
								)
								if err != nil {
									return err
								}
								if _, err := tx.ExecContext(ctx, sqlStr, sqlParas...); err != nil {
									return fmt.Errorf("the dowstream database topic [%s] partition [%d] offset [%d] exec sql [%v] parmas [%v] commit_ts [%d] failed: [%v]",
										c.topic, c.partition, c.kafkaOffset, sqlStr, sqlParas, e.CommitTs, err)
								}
								return nil
							},
						}); err != nil {
							return err
						}
					case message.DMLInsertQueryType:
						if err := c.databaseT.Transaction(gCtx, nil, []func(ctx context.Context, tx *sql.Tx) error{
							func(ctx context.Context, tx *sql.Tx) error {
								sqlStr, sqlParas, err := e.Delete(
									c.dbTypeT,
									c.schemaRoute.SchemaNameT,
									c.tableRoute,
									c.columnRoute,
									c.task.CaseFieldRuleT)
								if err != nil {
									return err
								}
								if _, err := tx.ExecContext(ctx, sqlStr, sqlParas...); err != nil {
									return fmt.Errorf("the dowstream database topic [%s] partition [%d] offset [%d] exec sql [%v] parmas [%v] commit_ts [%d] failed: [%v]",
										c.topic, c.partition, c.kafkaOffset, sqlStr, sqlParas, e.CommitTs, err)
								}
								return nil
							},
							func(ctx context.Context, tx *sql.Tx) error {
								sqlStr, sqlParas, err := e.Insert(
									c.dbTypeT,
									c.schemaRoute.SchemaNameT,
									c.tableRoute,
									c.columnRoute,
									c.task.CaseFieldRuleT,
								)
								if err != nil {
									return err
								}
								if _, err := tx.ExecContext(ctx, sqlStr, sqlParas...); err != nil {
									return fmt.Errorf("the dowstream database topic [%s] partition [%d] offset [%d] exec sql [%v] parmas [%v] commit_ts [%d] failed: [%v]",
										c.topic, c.partition, c.kafkaOffset, sqlStr, sqlParas, e.CommitTs, err)
								}
								return nil
							},
						}); err != nil {
							return err
						}
					case message.DMLDeleteQueryType:
						sqlStr, sqlParas, err := e.Delete(
							c.dbTypeT,
							c.schemaRoute.SchemaNameT,
							c.tableRoute,
							c.columnRoute,
							c.task.CaseFieldRuleT)
						if err != nil {
							return err
						}
						if _, err := c.databaseT.ExecContext(gCtx, sqlStr, sqlParas...); err != nil {
							return fmt.Errorf("the dowstream database topic [%s] partition [%d] offset [%d] exec sql [%v] parmas [%v] commit_ts [%d] failed: [%v]",
								c.topic, c.partition, c.kafkaOffset, sqlStr, sqlParas, e.CommitTs, err)
						}
					default:
						return fmt.Errorf("currently, the receive message event query type [%s] isnot support, the message event information [%v]", e.QueryType, e.String())
					}
					if err := c.updateMetadataCheckpoint(gCtx); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (c *Consumer) updateDowstreamTableColumnMetadataCache(tableName string, ddlType DDLType) error {
	timeS := time.Now()
	md := &metadata{
		tableColumns: make(map[string]*column),
	}

	var tableNameS, tableNameT string
	switch c.task.CaseFieldRuleS {
	case constant.ParamValueRuleCaseFieldNameUpper:
		tableNameS = strings.ToUpper(tableName)
	case constant.ParamValueRuleCaseFieldNameLower:
		tableNameS = strings.ToLower(tableName)
	default:
		tableNameS = tableName
	}

	for _, r := range c.tableRoute {
		if c.schemaRoute.SchemaNameS == r.SchemaNameS && tableNameS == r.TableNameS && c.schemaRoute.SchemaNameT == r.SchemaNameT && !strings.EqualFold(r.TableNameT, "") {
			tableNameT = r.TableNameT
			break
		}
	}
	if strings.EqualFold(tableNameT, "") {
		switch c.task.CaseFieldRuleT {
		case constant.ParamValueRuleCaseFieldNameUpper:
			tableNameT = strings.ToUpper(tableNameS)
		case constant.ParamValueRuleCaseFieldNameLower:
			tableNameT = strings.ToLower(tableNameS)
		default:
			tableNameT = tableNameS
		}
	}

	logger.Info("update downstream metadata",
		zap.String("task_name", c.task.TaskName),
		zap.String("task_mode", c.task.TaskMode),
		zap.String("task_flow", c.task.TaskFlow),
		zap.String("topic", c.topic),
		zap.Int("partition", c.partition),
		zap.Int64("offset", c.kafkaOffset),
		zap.String("schema_name_t", c.schemaRoute.SchemaNameT),
		zap.String("table_name_t", tableNameT),
		zap.String("ddl_type", ddlType.String()))

	switch ddlType {
	case DDLDropSchema:
		metaCache.DeleteSchema(c.schemaRoute.SchemaNameT)
	case DDLDropTable:
		metaCache.Delete(c.schemaRoute.SchemaNameT, tableNameT)
	case DDLRenameTable:
		// todo: not support
		return fmt.Errorf("the ddl type [%s] update metadata not support, please contact author or ensume the rename table can sync and restart the task", ddlType.String())
	case DDLCreateTable, DDLAddColumn, DDLDropColumn, DDLModifyColumn, DDLAddPrimaryKey, DDLDropPrimaryKey:
		res, err := c.databaseT.GetDatabaseTableColumnInfo(c.schemaRoute.SchemaNameT, tableNameT)
		if err != nil {
			return err
		}

		for _, r := range res {
			var (
				columnName string
			)
			switch c.task.CaseFieldRuleT {
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

			md.setColumn(columnName, &column{
				columnName: columnName,
				columnType: r["DATA_TYPE"],
				dataLength: int(dataL),
			})
		}

		md.setTable(c.schemaRoute.SchemaNameT, tableNameT)
		metaCache.Set(c.schemaRoute.SchemaNameT, tableNameT, md)
	}

	logger.Info("update downstream metadata completed",
		zap.String("task_name", c.task.TaskName),
		zap.String("task_mode", c.task.TaskMode),
		zap.String("task_flow", c.task.TaskFlow),
		zap.String("topic", c.topic),
		zap.Int("partition", c.partition),
		zap.Int64("offset", c.kafkaOffset),
		zap.String("schema_name_t", c.schemaRoute.SchemaNameT),
		zap.String("table_name_t", tableNameT),
		zap.String("ddl_type", ddlType.String()),
		zap.Duration("cost", time.Now().Sub(timeS)))
	return nil
}

// QuoteSchemaTable quotes a table name
func QuoteSchemaTable(schema string, table string) string {
	return fmt.Sprintf("%s.%s", schema, table)
}
