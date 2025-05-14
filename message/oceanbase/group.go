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
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/database/processor"
	"github.com/wentaojin/dbms/logger"
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

// ConsumerGroup represents the consumer manager
type ConsumerGroup struct {
	mu        sync.Mutex
	cancelCtx context.Context
	cancelFn  context.CancelFunc

	task *task.Task
	// ticdc open protocol, DDL message events will be distributed to all partitions
	// That is, if any partition receives the DDL Event, all other partitions need to receive it, indicating that the DDL event is complete
	partitions []int

	consumers map[int]*Consumer // kafka paritionID key

	// ddl coordinator
	// ddl -> partitions
	// 1. Determine whether the ddl event is received completely, len([]int32) == coordNums
	// 2. Receive the complete ddl event and append the ddl task queue
	coordinators map[string][]int

	// store ddl max commitTs information
	ddlWithMaxCommitTs *DDLChangedEvent

	// ddl task queue, stores ddl events in order of commitTs
	ddls DDLChangedEvents

	dbTypeS       string
	dbTypeT       string
	databaseS     database.IDatabase
	databaseT     database.IDatabase
	schemaRoute   *rule.SchemaRouteRule
	tableRoute    []*rule.TableRouteRule
	columnRoute   []*rule.ColumnRouteRule
	consumeParam  *pb.CdcConsumeParam
	consumeTables []string
	progress      *processor.Progress
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
	dbTypeS string,
	dbTypeT string,
	databaseS, databaseT database.IDatabase, progress *processor.Progress) *ConsumerGroup {
	cancelCtx, cancelFn := context.WithCancel(ctx)

	return &ConsumerGroup{
		task:      task,
		cancelCtx: cancelCtx,
		cancelFn:  cancelFn,

		partitions:    partitions,
		dbTypeS:       dbTypeS,
		dbTypeT:       dbTypeT,
		databaseS:     databaseS,
		databaseT:     databaseT,
		consumeParam:  param,
		consumeTables: consumeTables,
		schemaRoute:   schemaRoute,
		tableRoute:    tableRoute,
		columnRoute:   columnRoute,
		consumers:     make(map[int]*Consumer),
		progress:      progress,
	}
}

func (cg *ConsumerGroup) Run() error {
	g := &errgroup.Group{}
	g.SetLimit(len(cg.partitions))

	for _, partition := range cg.partitions {
		p := partition
		g.Go(func() error {
			if err := cg.Start(p); err != nil {
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
	cg.mu.Lock()
	defer cg.mu.Unlock()

	cg.cancelFn()

	for _, ps := range cg.consumers {
		if err := ps.consumer.Close(); err != nil {
			return fmt.Errorf("the consumer [%d] closed failed: %v", ps.partition, err)
		}
		delete(cg.consumers, ps.partition)
	}
	return nil
}

func (cg *ConsumerGroup) Consumers() map[int]*Consumer {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	return cg.consumers
}

func (cg *ConsumerGroup) Set(partition int, c *Consumer) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.consumers[partition] = c
}

func (cg *ConsumerGroup) Get(partition int) (*Consumer, error) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	ps, exists := cg.consumers[partition]
	if !exists {
		return nil, fmt.Errorf("the consumer [%d] does not exists", partition)
	}
	return ps, nil
}

func (cg *ConsumerGroup) Start(partition int) error {
	if _, err := cg.Get(partition); err == nil {
		return fmt.Errorf("the consumer [%d] has already exists, please setting new partition", partition)
	}

	msg, err := model.GetIMsgTopicPartitionRW().GetMsgTopicPartition(cg.cancelCtx, &consume.MsgTopicPartition{
		TaskName:   cg.task.TaskName,
		Topic:      cg.consumeParam.SubscribeTopic,
		Partitions: partition,
	})
	if err != nil {
		return err
	}

	_, err = model.GetITaskLogRW().CreateLog(cg.cancelCtx, &task.Log{
		TaskName: cg.task.TaskName,
		LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] cdc consumer topic [%s] partition [%d] started",
			stringutil.CurrentTimeFormatString(),
			stringutil.StringLower(constant.TaskModeCdcConsume),
			cg.task.WorkerAddr,
			cg.task.TaskName,
			cg.consumeParam.SubscribeTopic,
			partition,
		),
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

	cancelCtx, cancelFn := context.WithCancel(cg.cancelCtx)
	c := &Consumer{
		cancelCtx:      cancelCtx,
		cancelFn:       cancelFn,
		task:           cg.task,
		serverAddrs:    cg.consumeParam.ServerAddress,
		topic:          cg.consumeParam.SubscribeTopic,
		partition:      partition,
		needContinue:   true,
		decoder:        NewDecoder(cg.dbTypeS, cg.task.CaseFieldRuleS),
		checkpoint:     msg.Checkpoint,
		wg:             &sync.WaitGroup{},
		eventGroups:    make(map[string]*EventGroup),
		lastCommitTime: time.Now(),
		schemaRoute:    cg.schemaRoute,
		tableRoute:     cg.tableRoute,
		columnRoute:    cg.columnRoute,
		dbTypeS:        cg.dbTypeS,
		dbTypeT:        cg.dbTypeT,
		consumeTables:  cg.consumeTables,
		databaseS:      cg.databaseS,
		databaseT:      cg.databaseT,
		consumeParam:   cg.consumeParam,
	}

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

	cg.Set(partition, c)

	logger.Info("start consumer coroutines",
		zap.String("task_name", cg.task.TaskName),
		zap.String("task_flow", cg.task.TaskFlow),
		zap.String("task_mode", cg.task.TaskMode),
		zap.String("topic", msg.Topic),
		zap.Int("partition", partition),
		zap.Uint64("checkpoint", msg.Checkpoint))

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		cg.Coroutines(c)
	}()

	go func() {
		logger.Info("consumer serve for ready",
			zap.String("task_name", cg.task.TaskName),
			zap.String("task_flow", cg.task.TaskFlow),
			zap.String("task_mode", cg.task.TaskMode),
			zap.String("topic", c.topic),
			zap.Int("partition", c.partition))
		c.wg.Wait()
	}()

	select {
	case <-cg.cancelCtx.Done():
		logger.Error("consumer stopped by task stopped",
			zap.String("task_name", cg.task.TaskName),
			zap.String("task_flow", cg.task.TaskFlow),
			zap.String("task_mode", cg.task.TaskMode),
			zap.String("topic", c.topic),
			zap.Int("partition", c.partition),
			zap.Error(cg.cancelCtx.Err()))
		if err := cg.Cancel(partition); err != nil {
			return err
		}
		return cg.cancelCtx.Err()
	case <-c.cancelCtx.Done():
		logger.Warn("consumer stopped by stop signal",
			zap.String("task_name", cg.task.TaskName),
			zap.String("task_flow", cg.task.TaskFlow),
			zap.String("task_mode", cg.task.TaskMode),
			zap.String("topic", c.topic),
			zap.Int("partition", c.partition),
			zap.Error(c.cancelCtx.Err()))
		return nil
	}
}

func (cg *ConsumerGroup) Stop(partition int) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	ps, exists := cg.consumers[partition]
	if !exists {
		return fmt.Errorf("the consumer [%d] does not exists", partition)
	}

	// Send a stop signal
	ps.cancelFn()

	if err := ps.consumer.Close(); err != nil {
		return fmt.Errorf("the consumer [%d] closed failed: [%v]", partition, err)
	}
	delete(cg.consumers, partition)
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

func (cg *ConsumerGroup) Cancel(partition int) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	if c, exits := cg.consumers[partition]; exits {
		if err := c.consumer.Close(); err != nil {
			return fmt.Errorf("the consumer [%d] closed failed: [%v]", partition, err)
		}
		delete(cg.consumers, partition)
	}
	return nil
}

func (cg *ConsumerGroup) Coroutines(c *Consumer) {
	for {
		if !c.needContinue {
			continue
		}
		needCommit, err := cg.ConsumeMessage(c)
		if err != nil {
			logger.Error("consume message failed",
				zap.String("task_name", cg.task.TaskName),
				zap.String("task_flow", cg.task.TaskFlow),
				zap.String("task_mode", cg.task.TaskMode),
				zap.String("topic", c.topic),
				zap.Int("partition", c.partition),
				zap.Error(err))
			_, errC := model.GetITaskLogRW().CreateLog(c.cancelCtx, &task.Log{
				TaskName: cg.task.TaskName,
				LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] topic [%s] partition [%d] offset [%d] checkpoint [%d] cdc consume failed: [%v]",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeCdcConsume),
					cg.task.WorkerAddr,
					cg.task.TaskName,
					c.topic,
					c.partition,
					c.kafkaOffset,
					c.checkpoint,
					err.Error(),
				),
			})
			if errC != nil {
				logger.Error("record message failed",
					zap.String("task_name", cg.task.TaskName),
					zap.String("task_flow", cg.task.TaskFlow),
					zap.String("task_mode", cg.task.TaskMode),
					zap.String("topic", c.topic),
					zap.Int("partition", c.partition),
					zap.Error(err))
			}
			// if any consumer fails to execute, the task process service will be stopped directly.
			cg.cancelFn()
			return
		}
		if !needCommit {
			continue
		}
		c.lastCommitTime = time.Now()
	}
}

func (cg *ConsumerGroup) IsEventDDLFlush(key *DDLChangedEvent) bool {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	// DDL Event is received completely, only one DDL Event is retained
	if len(cg.coordinators[key.String()]) == len(cg.partitions) {
		cg.append(key)
		return true
	}
	return false
}

// append DDL wait to be handled, only consider the constraint among DDLs.
// for DDL a / b received in the order, a.CommitTs < b.CommitTs should be true.
func (cg *ConsumerGroup) append(ddl *DDLChangedEvent) {
	// A rename tables DDL job contains multiple DDL events with same CommitTs.
	// So to tell if a DDL is redundant or not, we must check the equivalence of
	// the current DDL and the DDL with max CommitTs.
	if ddl == cg.ddlWithMaxCommitTs {
		logger.Warn("ignore redundant ddl, the ddl is equal to ddlWithMaxCommitTs",
			zap.Uint64("commitTs", ddl.CommitTs),
			zap.String("events", ddl.String()))
		return
	}

	cg.ddls.Add(ddl)
	cg.ddlWithMaxCommitTs = ddl
}

func (cg *ConsumerGroup) RemoveDDL(key *DDLChangedEvent) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	delete(cg.coordinators, key.String())
	return nil
}

func (cg *ConsumerGroup) Coordinators(key *DDLChangedEvent) int {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	return len(cg.coordinators[key.String()])
}

func (cg *ConsumerGroup) Partitions(key *DDLChangedEvent) []int {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	return cg.coordinators[key.String()]
}

func (cg *ConsumerGroup) GetDDL() *DDLChangedEvent {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if cg.ddls.Len() > 0 {
		return cg.ddls[0]
	}
	return nil
}

func (cg *ConsumerGroup) PopDDL() {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if cg.ddls.Len() > 0 {
		cg.ddls = cg.ddls[1:]
	}
}

func (cg *ConsumerGroup) AppendDDL(key *DDLChangedEvent, value int) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	// In special cases (node ​​failure, network partition, etc.), the same version of Event may be sent multiple times
	// The DDL coordinator only guarantees to receive 1 DDL Event for the same version (to avoid duplicate reception)
	event := key.String()
	if vals, ok := cg.coordinators[event]; ok {
		if isContain(value, vals) {
			logger.Warn("ddl event duplicate reception",
				zap.Int("partition", value),
				zap.String("events", key.String()),
				zap.String("message_action", "ignore"))
			return
		}
	}
	cg.coordinators[event] = append(cg.coordinators[event], value)
}

func isContain(val int, values []int) bool {
	for _, v := range values {
		if v == val {
			return true
		}
	}
	return false
}
