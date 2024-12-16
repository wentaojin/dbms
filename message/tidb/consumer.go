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
	"database/sql"
	"fmt"
	"sync"

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

type ConsumerGroup struct {
	task      *task.Task
	cancelCtx context.Context
	cancelFn  context.CancelFunc
	mu        sync.Mutex

	partitions []int

	consumers map[int]*Consumer // 使用 kafka paritionID 作为 key

	ddlCoordinator *DdlCoordinator

	schemaRoute   *rule.SchemaRouteRule
	tableRoute    []*rule.TableRouteRule
	columnRoute   []*rule.ColumnRouteRule
	consumeParam  *pb.CdcConsumeParam
	consumeTables []string
	databaseT     database.IDatabase
	dbTypeT       string
}

type Consumer struct {
	cancelCtx   context.Context
	serverAddrs []string
	topic       string
	partition   int
	checkpoint  uint64
	kafkaOffset int64

	consumer     *kafka.Reader
	stopChan     chan struct{}
	needContinue bool

	// Messages are stored in groups according to database tables
	// schemaName.tableName -> EventGroup
	eventGroups map[string]*EventGroup
	wg          *sync.WaitGroup

	// Avoid frequent metadata refreshes due to no data before resolvedTs. Check whether there is data before <= resolvedTs.
	// If there is data, set needFlush to refresh immediately. If there are X consecutive resolvedTs with no data, needFlush is set when X resolvedTs are reached.
	// ResolvedTs according to a single partition ticdc interval Generates 1 resolvedTs approximately every 1 second, with a setting value of 60
	resolvedTs []uint64

	decoder RowEventDecoder
}

func NewConsumerGroup(
	ctx context.Context,
	task *task.Task,
	schemaRoute *rule.SchemaRouteRule,
	tableRoute []*rule.TableRouteRule,
	columnRoute []*rule.ColumnRouteRule,
	consumeTables []string,
	param *pb.CdcConsumeParam,
	partitions []int,
	dbTypeT string,
	database database.IDatabase) *ConsumerGroup {
	cancelCtx, cancelFn := context.WithCancel(ctx)
	return &ConsumerGroup{
		task:      task,
		cancelCtx: cancelCtx,
		cancelFn:  cancelFn,

		partitions:     partitions,
		ddlCoordinator: NewDdlCoordinator(len(partitions)),
		dbTypeT:        dbTypeT,
		databaseT:      database,
		consumers:      make(map[int]*Consumer),
		consumeParam:   param,
		consumeTables:  consumeTables,
		schemaRoute:    schemaRoute,
		tableRoute:     tableRoute,
		columnRoute:    columnRoute,
	}
}

func (cg *ConsumerGroup) Run() error {
	g, gCtx := errgroup.WithContext(cg.cancelCtx)
	g.SetLimit(len(cg.partitions))

	for _, ps := range cg.partitions {
		p := ps
		g.Go(func() error {
			if err := cg.Start(gCtx, p); err != nil {
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

func (cg *ConsumerGroup) Close() error {
	if err := cg.StopAll(); err != nil {
		return err
	}
	return nil
}

func (cg *ConsumerGroup) Start(ctx context.Context, partition int) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if _, exists := cg.consumers[partition]; exists {
		return fmt.Errorf("the consumer [%d] has already exists, please setting new partition", partition)
	}

	msg, err := model.GetIMsgTopicPartitionRW().GetMsgTopicPartition(ctx, &consume.MsgTopicPartition{
		TaskName:   cg.task.TaskName,
		Topic:      cg.consumeParam.SubscribeTopic,
		Partitions: partition,
	})
	if err != nil {
		return err
	}

	logger.Info("load consumer checkpoint",
		zap.String("task_name", cg.task.TaskName),
		zap.String("task_flow", cg.task.TaskFlow),
		zap.String("task_mode", cg.task.TaskMode),
		zap.String("topic", msg.Topic),
		zap.Int("partition", partition),
		zap.Uint64("checkpoint", msg.Checkpoint))

	c := &Consumer{
		cancelCtx:    cg.cancelCtx,
		serverAddrs:  cg.consumeParam.ServerAddress,
		topic:        cg.consumeParam.SubscribeTopic,
		partition:    partition,
		stopChan:     make(chan struct{}),
		needContinue: true,
		decoder:      NewBatchDecoder(cg.consumeParam.MessageCompression),
		checkpoint:   msg.Checkpoint,
		wg:           &sync.WaitGroup{},
		eventGroups:  make(map[string]*EventGroup),
		resolvedTs:   make([]uint64, 0, cg.consumeParam.IdleResolvedThreshold),
	}
	cg.consumers[partition] = c

	c.consumer = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     c.serverAddrs,
		Topic:       c.topic,
		Partition:   c.partition,
		Logger:      kafka.LoggerFunc(logger.GetRootLogger().Sugar().Infof),
		ErrorLogger: kafka.LoggerFunc(logger.GetRootLogger().Sugar().Errorf),
	})

	// setting consumer start offset（direct consume kafka partition, non-consumer-group）
	var kafkaOffset int64
	if msg.Offset == 0 {
		kafkaOffset = kafka.FirstOffset
	} else {
		kafkaOffset = msg.Offset
	}
	c.kafkaOffset = kafkaOffset

	if err := c.consumer.SetOffset(kafkaOffset); err != nil {
		return err
	}

	logger.Info("start consumer coroutines",
		zap.String("task_name", cg.task.TaskName),
		zap.String("task_flow", cg.task.TaskFlow),
		zap.String("task_mode", cg.task.TaskMode),
		zap.String("topic", msg.Topic),
		zap.Int("partition", partition),
		zap.Uint64("checkpoint", msg.Checkpoint))

	c.wg.Add(1)

	go cg.Consume(c)

	return nil
}

func (cg *ConsumerGroup) Stop(partition int) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	ps, exists := cg.consumers[partition]
	if !exists {
		return fmt.Errorf("the consumer [%d] does not exists", partition)
	}

	// Send a stop signal
	close(ps.stopChan)
	// Waiting for goroutine to complete
	ps.wg.Wait()
	// Remove the consumer from the consumerGroup
	delete(cg.consumers, partition)

	if err := ps.consumer.Close(); err != nil {
		return fmt.Errorf("the consumer [%d] closed failed: %v", partition, err)
	}
	return nil
}

func (cg *ConsumerGroup) Pause(partition int) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	ps, exists := cg.consumers[partition]
	if !exists {
		return fmt.Errorf("the consumer [%d] does not exists", partition)
	}

	// Set to false to pause the job.
	ps.needContinue = false
	return nil
}

func (cg *ConsumerGroup) Resume(partition int) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	ps, exists := cg.consumers[partition]
	if !exists {
		return fmt.Errorf("the consumer [%d] does not exists", partition)
	}
	// Set to true to resume the job.
	ps.needContinue = true
	return nil
}

func (cg *ConsumerGroup) StopAll() error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	cg.cancelFn()

	for _, ps := range cg.consumers {
		if err := ps.consumer.Close(); err != nil {
			return fmt.Errorf("the consumer [%d] closed failed: %v", ps.partition, err)
		}
	}
	return nil
}

// Consume will read message from Kafka.
func (cg *ConsumerGroup) Consume(c *Consumer) {
	defer c.wg.Done()

	for {
		select {
		case <-c.cancelCtx.Done():
			logger.Warn("consumer stopped by global context",
				zap.String("task_name", cg.task.TaskName),
				zap.String("task_flow", cg.task.TaskFlow),
				zap.String("task_mode", cg.task.TaskMode),
				zap.String("topic", c.topic),
				zap.Int("partition", c.partition))
			return
		case <-c.stopChan:
			logger.Warn("consumer stopped by stop signal",
				zap.String("task_name", cg.task.TaskName),
				zap.String("task_flow", cg.task.TaskFlow),
				zap.String("task_mode", cg.task.TaskMode),
				zap.String("topic", c.topic),
				zap.Int("partition", c.partition))
			return
		default:
			if c.needContinue {
				msg, err := c.consumer.ReadMessage(c.cancelCtx)
				if err != nil {
					logger.Error("read message failed",
						zap.String("task_name", cg.task.TaskName),
						zap.String("task_flow", cg.task.TaskFlow),
						zap.String("task_mode", cg.task.TaskMode),
						zap.String("topic", c.topic),
						zap.Int("partition", c.partition),
						zap.Error(err))
					panic(fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] partition [%d] read message failed: [%v]",
						cg.task.TaskName, cg.task.TaskFlow, cg.task.TaskMode, c.topic, c.partition, err))
				}

				needCommit, err := cg.WriteMessage(c, msg)
				if err != nil {
					logger.Error("write message failed",
						zap.String("task_name", cg.task.TaskName),
						zap.String("task_flow", cg.task.TaskFlow),
						zap.String("task_mode", cg.task.TaskMode),
						zap.String("topic", c.topic),
						zap.Int("partition", c.partition),
						zap.Error(err))
					panic(fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] partition [%d] write message failed: [%v]",
						cg.task.TaskName, cg.task.TaskFlow, cg.task.TaskMode, c.topic, c.partition, err))
				}
				if !needCommit {
					continue
				}
				if err := cg.CommitMessage(c.cancelCtx, msg); err != nil {
					logger.Error("commit message failed",
						zap.String("task_name", cg.task.TaskName),
						zap.String("task_flow", cg.task.TaskFlow),
						zap.String("task_mode", cg.task.TaskMode),
						zap.String("topic", c.topic),
						zap.Int("partition", c.partition),
						zap.Error(err))
					panic(fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] partition [%d] commit message failed: [%v]",
						cg.task.TaskName, cg.task.TaskFlow, cg.task.TaskMode, c.topic, c.partition, err))
				}
			}
		}
	}
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
			zap.String("task_name", cg.task.TaskName),
			zap.String("task_flow", cg.task.TaskFlow),
			zap.String("task_mode", cg.task.TaskMode),
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
			return false, fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] partiton [%d] key [%s] value [%s] offset [%d] message next failed: %v", cg.task.TaskName, cg.task.TaskFlow, cg.task.TaskMode, msg.Topic, msg.Partition, string(msg.Key), string(msg.Value), msg.Offset, err)
		}

		if !hasNext {
			break
		}

		switch msgEventType {
		case MsgEventTypeDDL:
			ddl, err := c.decoder.NextDDLEvent()
			if err != nil {
				return false, fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] partiton [%d] key [%s] value [%s] offset [%d] decode ddl event message failed: %v", cg.task.TaskName, cg.task.TaskFlow, cg.task.TaskMode, msg.Topic, msg.Partition, string(msg.Key), string(msg.Value), msg.Offset, err)
			}

			if cg.CheckObsoleteMessages(ddl.CommitTs, c.checkpoint) {
				logger.Warn("ddl message received",
					zap.String("task_name", cg.task.TaskName),
					zap.String("task_flow", cg.task.TaskFlow),
					zap.String("task_mode", cg.task.TaskMode),
					zap.String("topic", c.topic),
					zap.Int("partition", c.partition),
					zap.Int64("offset", msg.Offset),
					zap.Uint64("commitTs", ddl.CommitTs),
					zap.String("ddl_type", ddl.DdlType.String()),
					zap.String("ddl_query", ddl.DdlQuery),
					zap.Uint64("checkpoint", c.checkpoint),
					zap.String("message_action", "less than checkpoint, msg experied"))
				continue
			}

			if ddl.SchemaName == cg.schemaRoute.SchemaNameS && stringutil.IsContainedString(cg.consumeTables, ddl.TableName) {
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

				cg.ddlCoordinator.Append(ddl, partCode)

				if err := cg.Pause(msg.Partition); err != nil {
					return false, err
				}

				logger.Info("ddl message received",
					zap.String("task_name", cg.task.TaskName),
					zap.String("task_flow", cg.task.TaskFlow),
					zap.String("task_mode", cg.task.TaskMode),
					zap.String("topic", c.topic),
					zap.Int("partition", c.partition),
					zap.Int64("offset", msg.Offset),
					zap.Uint64("commitTs", ddl.CommitTs),
					zap.String("ddl_type", ddl.DdlType.String()),
					zap.String("ddl_query", ddl.DdlQuery),
					zap.Uint64("checkpoint", c.checkpoint),
					zap.Int("coordinator", cg.ddlCoordinator.Len(ddl)),
					zap.String("message_action", "paused"))

				c.checkpoint = ddl.CommitTs
				c.kafkaOffset = msg.Offset

				// The partition that receives the DDL Event last is responsible for executing the DDL Event.
				if cg.ddlCoordinator.IsDDLFlush(ddl) {
					logger.Info("ddl message received",
						zap.String("task_name", cg.task.TaskName),
						zap.String("task_flow", cg.task.TaskFlow),
						zap.String("task_mode", cg.task.TaskMode),
						zap.String("topic", c.topic),
						zap.Int("partition", c.partition),
						zap.Int64("offset", msg.Offset),
						zap.Uint64("commitTs", ddl.CommitTs),
						zap.String("ddl_type", ddl.DdlType.String()),
						zap.String("ddl_query", ddl.DdlQuery),
						zap.Uint64("checkpoint", c.checkpoint),
						zap.Int("coordinator", cg.ddlCoordinator.Len(ddl)),
						zap.String("message_action", "flush"))
					needFlush = true
				}
			} else {
				logger.Warn("ddl message received",
					zap.String("task_name", cg.task.TaskName),
					zap.String("task_flow", cg.task.TaskFlow),
					zap.String("task_mode", cg.task.TaskMode),
					zap.String("schema_name", ddl.SchemaName),
					zap.String("table_name", ddl.TableName),
					zap.String("topic", c.topic),
					zap.Int("partition", c.partition),
					zap.Int64("offset", msg.Offset),
					zap.Uint64("commitTs", ddl.CommitTs),
					zap.String("ddl_type", ddl.DdlType.String()),
					zap.String("ddl_query", ddl.DdlQuery),
					zap.Uint64("checkpoint", c.checkpoint),
					zap.Int("coordinator", cg.ddlCoordinator.Len(ddl)),
					zap.String("message_action", "skip"))
			}
		case MsgEventTypeRow:
			dml, err := c.decoder.NextRowChangedEvent()
			if err != nil {
				return false, fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] partiton [%d] key [%s] value [%s] offset [%d] decode dml event message failed: %v", cg.task.TaskName, cg.task.TaskFlow, cg.task.TaskMode, msg.Topic, msg.Partition, string(msg.Key), string(msg.Value), msg.Offset, err)
			}

			if cg.CheckObsoleteMessages(dml.CommitTs, c.checkpoint) {
				logger.Warn("dml event received",
					zap.String("task_name", cg.task.TaskName),
					zap.String("task_flow", cg.task.TaskFlow),
					zap.String("task_mode", cg.task.TaskMode),
					zap.String("topic", c.topic),
					zap.Int("partition", c.partition),
					zap.Int64("offset", msg.Offset),
					zap.String("dml_events", dml.String()),
					zap.Uint64("checkpoint", c.checkpoint),
					zap.String("message_action", "less than checkpoint, msg experied"))
				continue
			}

			if dml.SchemaName == cg.schemaRoute.SchemaNameS && stringutil.IsContainedString(cg.consumeTables, dml.TableName) {
				logger.Info("dml event received",
					zap.String("task_name", cg.task.TaskName),
					zap.String("task_flow", cg.task.TaskFlow),
					zap.String("task_mode", cg.task.TaskMode),
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
				logger.Warn("dml event received",
					zap.String("task_name", cg.task.TaskName),
					zap.String("task_flow", cg.task.TaskFlow),
					zap.String("task_mode", cg.task.TaskMode),
					zap.String("schema_name", dml.SchemaName),
					zap.String("table_name", dml.TableName),
					zap.String("topic", c.topic),
					zap.Int("partition", c.partition),
					zap.Int64("offset", msg.Offset),
					zap.Uint64("checkpoint", c.checkpoint),
					zap.String("dml_events", dml.String()),
					zap.String("message_action", "consumering"))
			}
		case MsgEventTypeResolved:
			resolvedTs, err := c.decoder.NextResolvedEvent()
			if err != nil {
				return false, fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] partiton [%d] key [%s] value [%s] offset [%d] decode resolved_ts event message failed: %v", cg.task.TaskName, cg.task.TaskFlow, cg.task.TaskMode, msg.Topic, msg.Partition, string(msg.Key), string(msg.Value), msg.Offset, err)
			}

			if cg.CheckObsoleteMessages(resolvedTs, c.checkpoint) {
				logger.Warn("resolved event received",
					zap.String("task_name", cg.task.TaskName),
					zap.String("task_flow", cg.task.TaskFlow),
					zap.String("task_mode", cg.task.TaskMode),
					zap.String("topic", c.topic),
					zap.Int("partition", c.partition),
					zap.Int64("offset", msg.Offset),
					zap.Uint64("checkpoint", c.checkpoint),
					zap.String("message_action", "less than checkpoint, msg experied"))
				continue
			}

			logger.Debug("resolved event received",
				zap.String("task_name", cg.task.TaskName),
				zap.String("task_flow", cg.task.TaskFlow),
				zap.String("task_mode", cg.task.TaskMode),
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

			if cg.ddlCoordinator.IsResolvedFlush(resolvedTs) || cg.ddlCoordinator.GetFrontDDL() == nil {
				rowChangedEventCounts := 0
				for _, group := range c.eventGroups {
					rows := len(group.events)
					if rows == 0 {
						continue
					}
					rowChangedEventCounts = rowChangedEventCounts + rows
				}

				if rowChangedEventCounts == 0 {
					c.resolvedTs = append(c.resolvedTs, resolvedTs)
				}

				if rowChangedEventCounts > 0 || len(c.resolvedTs) == int(cg.consumeParam.IdleResolvedThreshold) {
					logger.Info("resolved event received",
						zap.String("task_name", cg.task.TaskName),
						zap.String("task_flow", cg.task.TaskFlow),
						zap.String("task_mode", cg.task.TaskMode),
						zap.String("topic", c.topic),
						zap.Int("partition", c.partition),
						zap.Int64("offset", msg.Offset),
						zap.Uint64("checkpoint", c.checkpoint),
						zap.Uint64("resolved_ts", resolvedTs),
						zap.Int("dml_events", rowChangedEventCounts),
						zap.Int("resolved_queues", len(c.resolvedTs)),
						zap.String("message_action", "flush"))
					// reset
					c.resolvedTs = c.resolvedTs[:0]
					needFlush = true
				}
			} else {
				switch {
				case resolvedTs < cg.ddlCoordinator.GetFrontDDL().CommitTs:
					needFlush = false
				default:
					logger.Error("resolved event received",
						zap.String("task_name", cg.task.TaskName),
						zap.String("task_flow", cg.task.TaskFlow),
						zap.String("task_mode", cg.task.TaskMode),
						zap.String("topic", c.topic),
						zap.Int("partition", c.partition),
						zap.Int64("offset", msg.Offset),
						zap.Uint64("checkpoint", c.checkpoint),
						zap.String("min_ddl_commit_ts_event", cg.ddlCoordinator.GetFrontDDL().String()),
						zap.String("message_action", fmt.Sprintf("The corresponding partition [%v] should have consumed the DDL event and triggered the consumption suspension. The current situation where resolvedTs >= ddl taskqueue min commits should not appear", partCode)))

					return false, fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] corresponding partition [%v] should have consumed the ddl event and triggered the consumption suspension. the current situation where resolvedTs >= ddl taskqueue min commits should not appear", cg.task.TaskName, cg.task.TaskFlow, cg.task.TaskMode, msg.Topic, partCode)
				}
			}
		default:
			return false, fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] topic [%s] partiton [%d] key [%s] value [%s] offset [%d] unknown message type [%v]", cg.task.TaskName, cg.task.TaskFlow, cg.task.TaskMode, msg.Topic, msg.Partition, string(msg.Key), string(msg.Value), msg.Offset, msgEventType)
		}
	}

	return needFlush, nil
}

// CheckObsoleteMessages
// 1. Initially start filtering and filtration of synchronously consumed events
// 2. During the operation, the corresponding partition DDL/ResolvedTs Event is refreshed. The reason is that CDC guarantees that all events before the DDL/ResolvedTs Event have been sent, and there should be no more events smaller than the DDL/ResolvedTs Event Ts
func (cg *ConsumerGroup) CheckObsoleteMessages(currentTs, nowTs uint64) bool {
	return currentTs < nowTs
}

func (cg *ConsumerGroup) CommitMessage(ctx context.Context, msg kafka.Message) error {
	// The ticdc open protocol sends ddl events to all MQ partitions and ensures that all DMLs events <= ddl event Ts have been sent.
	// Any partition that receives a DDL event will suspend consumption.
	// Therefore, it is normal that multiple different DDLEvents will not enter coordination at the same time. Encountering multiple different DDL Events entering coordination is an unexpected phenomenon.
	if cg.ddlCoordinator.Ddls.Len() > 1 {
		return fmt.Errorf(`the ticdc open protocol sends ddl events to all MQ partitions and ensures that all DMLs events <= ddl event Ts have been sent. any partition that receives a DDL event will suspend consumption. therefore, it is normal that multiple different DDLEvents will not enter coordination at the same time. encountering multiple different DDL Events entering coordination is an unexpected phenomenon`)
	} else if cg.ddlCoordinator.Ddls.Len() == 1 {
		var todoDDL *DDLChangedEvent

		for {
			todoDDL = cg.ddlCoordinator.GetFrontDDL()

			if todoDDL == nil {
				break
			}

			// flush less than DDL CommitTs's DMLs
			if err := cg.flushCheckpointBeforeRowChangedEvents(ctx, true); err != nil {
				return err
			}

			encrStr, err := stringutil.Encrypt(todoDDL.DdlQuery, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return err
			}
			rewrite, err := model.GetIMsgDdlRewriteRW().GetMsgDdlRewrite(ctx, &consume.MsgDdlRewrite{
				TaskName:  cg.task.TaskName,
				Topic:     msg.Topic,
				DdlDigest: encrStr,
			})
			if err != nil {
				return err
			}
			// execute DDL Event
			if rewrite != nil {
				if _, err := cg.databaseT.ExecContext(ctx, rewrite.RewriteDdlText); err != nil {
					return fmt.Errorf("the dowstream database rewrite sql [%v] digest [%s] commit_ts [%d] exec failed, please check whether the statement is rewritten correctly , error detail [%v]", todoDDL.DdlQuery, encrStr, todoDDL.CommitTs, err)
				}
			} else {
				if _, err := cg.databaseT.ExecContext(ctx, todoDDL.DdlQuery); err != nil {
					return fmt.Errorf("the dowstream database origin sql [%v] digest [%s] commit_ts [%d] exec failed, please decide whether to rewrite based on the error message and downstream database type [%s]. If you need to rewrite, please use digest to rewrite, error detail [%v]", todoDDL.DdlQuery, encrStr, todoDDL.CommitTs, cg.dbTypeT, err)
				}
			}
			if err := cg.flushCheckpoint(ctx); err != nil {
				return err
			}

			cg.ddlCoordinator.PopDDL()
			for _, p := range cg.ddlCoordinator.Get(todoDDL) {
				logger.Warn("ddl event coordinator finished",
					zap.String("task_name", cg.task.TaskName),
					zap.String("task_flow", cg.task.TaskFlow),
					zap.String("task_mode", cg.task.TaskMode),
					zap.String("topic", msg.Topic),
					zap.Int("partition", p),
					zap.String("ddl_events", todoDDL.String()),
					zap.String("message_action", "resume"))
				if err := cg.Resume(p); err != nil {
					return err
				}
			}
			cg.ddlCoordinator.Remove(todoDDL)
		}
	}

	// flush <= resolvedTs DMLs
	if err := cg.flushCheckpointBeforeRowChangedEvents(ctx, false); err != nil {
		return err
	}
	if err := cg.flushCheckpoint(ctx); err != nil {
		return err
	}

	return nil
}

func (cg *ConsumerGroup) flushCheckpointBeforeRowChangedEvents(ctx context.Context, isDDLCommit bool) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(len(cg.consumers))

	for _, consumer := range cg.consumers {
		c := consumer
		g.Go(func() error {
			// partition wait sink events
			// store all the events that are less than or equal a certain ts in order according to the table latitude
			sinkEvents := make(map[string][]*RowChangedEvent)

			for tableName, group := range c.eventGroups {
				var events []*RowChangedEvent
				if isDDLCommit {
					events = group.DDLCommitTs(c.checkpoint)
				} else {
					events = group.ResolvedTs(c.checkpoint)
				}
				if len(events) == 0 {
					continue
				}
				sinkEvents[tableName] = events
			}

			if len(sinkEvents) > 0 {
				if err := cg.flushRowChangedEvents(gCtx, sinkEvents); err != nil {
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

func (cg *ConsumerGroup) flushRowChangedEvents(ctx context.Context, sinkEvents map[string][]*RowChangedEvent) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(int(cg.consumeParam.TableThread))

	for _, es := range sinkEvents {
		events := es
		g.Go(func() error {
			for _, e := range events {
				if e.IsDDL {
					return fmt.Errorf("the DDL events need to be executed separately. After the execution is completed, the message should not appear here. Please contact the author")
				} else {
					logger.Info("message event flush",
						zap.String("task_name", cg.task.TaskName),
						zap.String("task_flow", cg.task.TaskFlow),
						zap.String("task_mode", cg.task.TaskMode),
						zap.String("topic", cg.consumeParam.SubscribeTopic),
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
						if err := cg.databaseT.Transaction(gCtx, nil, []func(ctx context.Context, tx *sql.Tx) error{
							func(ctx context.Context, tx *sql.Tx) error {
								sqlStr, sqlParas := e.Delete(
									cg.dbTypeT,
									cg.tableRoute,
									cg.columnRoute,
									cg.task.CaseFieldRuleT)
								if _, err := tx.ExecContext(ctx, sqlStr, sqlParas...); err != nil {
									return fmt.Errorf("the dowstream database exec sql [%v] parmas [%v] commit_ts [%d] failed: [%v]", sqlStr, sqlParas, e.CommitTs, err)
								}
								return nil
							},
							func(ctx context.Context, tx *sql.Tx) error {
								sqlStr, sqlParas := e.Replace(
									cg.dbTypeT,
									cg.tableRoute,
									cg.columnRoute,
									cg.task.CaseFieldRuleT)
								if _, err := tx.ExecContext(ctx, sqlStr, sqlParas...); err != nil {
									return fmt.Errorf("the dowstream database exec sql [%v] parmas [%v] commit_ts [%d] failed: [%v]", sqlStr, sqlParas, e.CommitTs, err)
								}
								return nil
							},
						}); err != nil {
							return err
						}
					case message.DMLInsertQueryType:
						sqlStr, sqlParas := e.Replace(
							cg.dbTypeT,
							cg.tableRoute,
							cg.columnRoute,
							cg.task.CaseFieldRuleT)
						if _, err := cg.databaseT.ExecContext(gCtx, sqlStr, sqlParas...); err != nil {
							return fmt.Errorf("the dowstream database exec sql [%v] parmas [%v] commit_ts [%d] failed: [%v]", sqlStr, sqlParas, e.CommitTs, err)
						}
					case message.DMLDeleteQueryType:
						sqlStr, sqlParas := e.Delete(
							cg.dbTypeT,
							cg.tableRoute,
							cg.columnRoute,
							cg.task.CaseFieldRuleT)
						if _, err := cg.databaseT.ExecContext(gCtx, sqlStr, sqlParas...); err != nil {
							return fmt.Errorf("the dowstream database exec sql [%v] parmas [%v] commit_ts [%d] failed: [%v]", sqlStr, sqlParas, e.CommitTs, err)
						}
					default:
						return fmt.Errorf("currently, the receive message event query type [%s] isnot support, the message event information [%v]", e.QueryType, e.String())
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

func (cg *ConsumerGroup) flushCheckpoint(ctx context.Context) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(len(cg.consumers))

	for _, consumer := range cg.consumers {
		c := consumer
		g.Go(func() error {
			if err := cg.updateMetadataCheckpoint(gCtx, c); err != nil {
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

func (cg *ConsumerGroup) updateMetadataCheckpoint(ctx context.Context, c *Consumer) error {
	if err := model.GetIMsgTopicPartitionRW().UpdateMsgTopicPartition(ctx, &consume.MsgTopicPartition{
		TaskName:   c.topic,
		Topic:      c.topic,
		Partitions: c.partition,
	}, map[string]interface{}{
		"Checkpoint": c.checkpoint,
		"Offset":     c.kafkaOffset,
	}); err != nil {
		return err
	}
	logger.Debug("commit message success",
		zap.String("task_name", cg.task.TaskName),
		zap.String("task_flow", cg.task.TaskFlow),
		zap.String("task_mode", cg.task.TaskMode),
		zap.String("topic", c.topic),
		zap.Int("partition", c.partition),
		zap.Int64("offset", c.kafkaOffset),
		zap.Uint64("checkpoint", c.checkpoint))
	return nil
}

// QuoteSchemaTable quotes a table name
func QuoteSchemaTable(schema string, table string) string {
	return fmt.Sprintf("%s.%s", schema, table)
}
