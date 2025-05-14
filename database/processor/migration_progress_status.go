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
package processor

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/constant"
	"go.uber.org/zap"
)

const DefaultProgressPrintIntervalSec = 10

type Progress struct {
	Ctx              context.Context
	Ticker           *time.Ticker
	TaskName         string
	TaskMode         string
	TaskFlow         string
	TableCounts      uint64
	TableRowCounts   uint64
	TableSizeCounts  uint64
	TableChunkCounts uint64

	TableNameProcessed   uint64
	TableRowsProcessed   uint64
	TableChunksProcessed uint64
	TableBytesProcessed  uint64
	TableRowsImported    uint64

	TableRowsReaded uint64

	LastTableNameProcessed   uint64
	LastTableRowsProcessed   uint64
	LastTableChunksProcessed uint64
	LastTableBytesProcessed  uint64
	LastTableRowsReaded      uint64
	LastTableRowsImported    uint64

	MsgConsumeCounts sync.Map
	MsgConsumeBytes  sync.Map
	MsgConsumeDelay  sync.Map

	Done        chan struct{} // channel used to notify exit
	StartedTime time.Time
}

func NewProgresser(ctx context.Context) *Progress {
	return &Progress{
		Ctx:    ctx,
		Ticker: time.NewTicker(time.Duration(DefaultProgressPrintIntervalSec) * time.Second), // print every 10 seconds
		Done:   make(chan struct{}),
	}
}

type Options func(opts *Progress)

func (p *Progress) Init(opts ...Options) {
	for _, opt := range opts {
		opt(p)
	}
}

func WithTaskName(str string) Options {
	return func(opts *Progress) {
		opts.TaskName = str
	}
}

func WithTaskMode(str string) Options {
	return func(opts *Progress) {
		opts.TaskMode = str
	}
}

func WithTaskFlow(str string) Options {
	return func(opts *Progress) {
		opts.TaskFlow = str
	}
}

func (p *Progress) SetTableCounts(tables uint64) {
	atomic.StoreUint64(&p.TableCounts, tables)
}

func (p *Progress) GetTableCounts() uint64 {
	return atomic.LoadUint64(&p.TableCounts)
}

func (p *Progress) SetTableSizeCounts(sizes uint64) {
	atomic.AddUint64(&p.TableSizeCounts, sizes)
}

func (p *Progress) SetTableRowCounts(rows uint64) {
	atomic.AddUint64(&p.TableRowCounts, rows)
}

func (p *Progress) SetTableChunkCounts(chunks uint64) {
	atomic.AddUint64(&p.TableChunkCounts, chunks)
}

func (p *Progress) UpdateTableNameProcessed(tables uint64) {
	atomic.AddUint64(&p.TableNameProcessed, tables)
}

func (p *Progress) UpdateTableRowsProcessed(rows uint64) {
	atomic.AddUint64(&p.TableRowsProcessed, rows)
}

func (p *Progress) UpdateTableChunksProcessed(chunks uint64) {
	atomic.AddUint64(&p.TableChunksProcessed, chunks)
}

func (p *Progress) UpdateTableBytesProcessed(sizeMB uint64) {
	atomic.AddUint64(&p.TableBytesProcessed, sizeMB)
}

func (p *Progress) UpdateTableRowsReaded(rows uint64) {
	atomic.AddUint64(&p.TableRowsReaded, rows)
}

func (p *Progress) UpdateTableRowsImported(rows uint64) {
	atomic.AddUint64(&p.TableRowsImported, rows)
}

func (p *Progress) UpdateMsgConsumeCounts(paritionID int, rows uint64) {
	mcs, loaded := p.MsgConsumeCounts.LoadOrStore(paritionID, rows)
	if loaded {
		newMcs := mcs.(uint64) + rows
		p.MsgConsumeCounts.Store(paritionID, newMcs)
	}
}

func (p *Progress) UpdateMsgConsumeBytes(paritionID int, bytes uint64) {
	mcs, loaded := p.MsgConsumeBytes.LoadOrStore(paritionID, bytes)
	if loaded {
		newMcs := mcs.(uint64) + bytes
		p.MsgConsumeBytes.Store(paritionID, newMcs)
	}
}

func (p *Progress) UpdateMsgConsumeDelay(paritionID int, delay time.Duration) {
	mcs, loaded := p.MsgConsumeDelay.LoadOrStore(paritionID, delay)
	if loaded {
		newMcs := mcs.(time.Duration) + delay
		p.MsgConsumeDelay.Store(paritionID, newMcs)
	}
}

func (p *Progress) SetLastTableNameProcessed(tables uint64) {
	atomic.StoreUint64(&p.LastTableNameProcessed, tables)
}

func (p *Progress) SetLastTableRowsProcessed(rows uint64) {
	atomic.StoreUint64(&p.LastTableRowsProcessed, rows)
}

func (p *Progress) SetLastTableChunksProcessed(chunks uint64) {
	atomic.StoreUint64(&p.LastTableChunksProcessed, chunks)
}

func (p *Progress) SetLastTableBytesProcessed(sizeMB uint64) {
	atomic.StoreUint64(&p.LastTableBytesProcessed, sizeMB)
}

func (p *Progress) SetLastTableRowsReaded(rows uint64) {
	atomic.StoreUint64(&p.LastTableRowsReaded, rows)
}

func (p *Progress) SetLastTableRowsImported(rows uint64) {
	atomic.StoreUint64(&p.LastTableRowsImported, rows)
}

func (p *Progress) ResetMsgConsumeCounts(paritionID int) {
	p.MsgConsumeCounts.Store(paritionID, uint64(0))
}

func (p *Progress) ResetMsgConsumeBytes(paritionID int) {
	p.MsgConsumeBytes.Store(paritionID, uint64(0))
}

func (p *Progress) ResetMsgConsumeDelay(paritionID int) {
	p.MsgConsumeDelay.Store(paritionID, time.Duration(0))
}

func (p *Progress) LoadProgress() *Progress {
	return &Progress{
		TableCounts:              atomic.LoadUint64(&p.TableCounts),
		TableRowCounts:           atomic.LoadUint64(&p.TableRowCounts),
		TableSizeCounts:          atomic.LoadUint64(&p.TableSizeCounts),
		TableChunkCounts:         atomic.LoadUint64(&p.TableChunkCounts),
		TableNameProcessed:       atomic.LoadUint64(&p.TableNameProcessed),
		TableChunksProcessed:     atomic.LoadUint64(&p.TableChunksProcessed),
		TableRowsProcessed:       atomic.LoadUint64(&p.TableRowsProcessed),
		TableBytesProcessed:      atomic.LoadUint64(&p.TableBytesProcessed),
		TableRowsReaded:          atomic.LoadUint64(&p.TableRowsReaded),
		TableRowsImported:        atomic.LoadUint64(&p.TableRowsImported),
		LastTableNameProcessed:   atomic.LoadUint64(&p.LastTableNameProcessed),
		LastTableChunksProcessed: atomic.LoadUint64(&p.LastTableChunksProcessed),
		LastTableRowsProcessed:   atomic.LoadUint64(&p.LastTableRowsProcessed),
		LastTableBytesProcessed:  atomic.LoadUint64(&p.LastTableBytesProcessed),
		LastTableRowsReaded:      atomic.LoadUint64(&p.LastTableRowsReaded),
		LastTableRowsImported:    atomic.LoadUint64(&p.LastTableRowsImported),
	}
}

func (p *Progress) PrintProgress() {
	var cp *Progress
	p.StartedTime = time.Now()

	for {
		select {
		case <-p.Ctx.Done():
			p.Stop()
			return
		case <-p.Done:
			return
		case <-p.Ticker.C:
			currentTime := time.Now()
			elapsed := currentTime.Sub(p.StartedTime).Seconds()

			cp = p.LoadProgress()

			var (
				rowsPerSec float64
				// bytesPerSec float64
				chunkPerSec float64
				tablePerSec float64
			)

			fields := []zap.Field{
				zap.String("task_name", p.TaskName),
				zap.String("task_mode", p.TaskMode),
				zap.String("task_flow", p.TaskFlow),
			}

			switch p.TaskMode {
			case constant.TaskModeStructMigrate, constant.TaskModeStructCompare:
				fields = append(fields, zap.Uint64("table_counts", cp.TableCounts))

				tablePerSec = float64(cp.TableNameProcessed-cp.LastTableNameProcessed) / elapsed

				var eta time.Duration
				if tablePerSec > 0 && cp.TableCounts > 0 && cp.TableNameProcessed < cp.TableCounts {
					remainTables := float64(cp.TableCounts - cp.TableNameProcessed)
					eta = time.Duration(remainTables/tablePerSec) * time.Second
				}

				fields = append(fields, zap.String("tables/sec", fmt.Sprintf("%2.f", math.Round(tablePerSec))))
				fields = append(fields, zap.Uint64("tables_completed", cp.TableNameProcessed))
				if cp.TableCounts == 0 {
					fields = append(fields, zap.String("tables_completion_ratio", "0%"))
				} else {
					fields = append(fields, zap.String("tables_completion_ratio", fmt.Sprintf("%2.f%%", float64(cp.TableNameProcessed)/float64(cp.TableCounts)*100)))
				}
				if eta > 0 {
					fields = append(fields, zap.String("remaining_time", eta.Truncate(time.Second).String()))
				}

				logger.Info("task progress monitoring started", fields...)
			case constant.TaskModeStmtMigrate, constant.TaskModeCSVMigrate, constant.TaskModeDataCompare, constant.TaskModeDataScan:
				fields = append(fields, zap.Uint64("table_counts", cp.TableCounts))

				rowsPerSec = float64(cp.TableRowsProcessed-cp.LastTableRowsProcessed) / elapsed
				//bytesPerSec = float64(cp.TableBytesProcessed-cp.LastTableBytesProcessed) / elapsed
				chunkPerSec = float64(cp.TableChunksProcessed-cp.LastTableChunksProcessed) / elapsed
				tablePerSec = float64(cp.TableNameProcessed-cp.LastTableNameProcessed) / elapsed

				readPerSec := float64(cp.TableRowsReaded-cp.LastTableRowsReaded) / elapsed

				var eta time.Duration
				if rowsPerSec > 0 && cp.TableRowCounts > 0 && cp.TableRowsProcessed < cp.TableRowCounts {
					remainRows := float64(cp.TableRowCounts - cp.TableRowsProcessed)
					eta = time.Duration(remainRows/rowsPerSec) * time.Second
				}

				fields = append(fields, zap.Uint64("tables_completed", cp.TableNameProcessed))
				fields = append(fields, zap.String("processed_tables/sec", fmt.Sprintf("%2.f", math.Round(tablePerSec))))
				if cp.TableCounts == 0 {
					fields = append(fields, zap.String("tables_completion_ratio", "0%"))
				} else {
					fields = append(fields, zap.String("tables_completion_ratio", fmt.Sprintf("%2.f%%", float64(cp.TableNameProcessed)/float64(cp.TableCounts)*100)))
				}
				fields = append(fields, zap.Uint64("chunks_counts", cp.TableChunkCounts))
				fields = append(fields, zap.Uint64("chunks_completed", cp.TableChunksProcessed))
				fields = append(fields, zap.String("processed_chunks/sec", fmt.Sprintf("%2.f", math.Round(chunkPerSec))))
				fields = append(fields, zap.String("chunks_completion_ratio", fmt.Sprintf("%2.f%%", float64(cp.TableChunksProcessed)/float64(cp.TableChunkCounts)*100)))

				// The original number of rows is based on statistical data, and the actual number of rows processed may exceed the original number of rows. Breakpoint resume is based on the number of completed chunks. The completed rows are calculated by evenly dividing the statistical information. It may happen that the completed chunks are less than the average calculated rows, and the unfinished parts are more than the average calculated rows, resulting in the number of rows exceeding the original number of rows.
				// to avoid confusion, avoid row_completed and rows_completion_ration output
				fields = append(fields, zap.Uint64("rows_counts", cp.TableRowCounts))
				// fields = append(fields, zap.Uint64("rows_completed", cp.TableRowsProcessed))
				if p.TaskMode == constant.TaskModeStmtMigrate || p.TaskMode == constant.TaskModeCSVMigrate {
					fields = append(fields, zap.String("readed_rows/sec", fmt.Sprintf("%2.f", math.Round(readPerSec))))
				}
				fields = append(fields, zap.String("processed_rows/sec", fmt.Sprintf("%2.f", math.Round(rowsPerSec))))
				// if cp.TableRowsProcessed >= cp.TableRowCounts && cp.TableRowCounts > 0 {
				// 	fields = append(fields, zap.String("rows_completion_ratio", "100%"))
				// } else {
				// 	if cp.TableRowCounts == 0 {
				// 		fields = append(fields, zap.String("rows_completion_ratio", "0%"))
				// 	} else {
				// 		fields = append(fields, zap.String("rows_completion_ratio", fmt.Sprintf("%2.f%%", float64(cp.TableRowsProcessed)/float64(cp.TableRowCounts)*100)))
				// 	}
				// }
				if p.TaskMode == constant.TaskModeCSVMigrate {
					importPerSec := float64(cp.TableRowsImported-cp.LastTableRowsImported) / elapsed
					fields = append(fields, zap.String("imported_rows/sec", fmt.Sprintf("%2.f", math.Round(importPerSec))))
				}
				// fields = append(fields, zap.String("bytes/sec(MB)", fmt.Sprintf("%2.f", math.Round(bytesPerSec))))

				if eta > 0 {
					fields = append(fields, zap.String("remaining_time", eta.Truncate(time.Second).String()))
				}
				logger.Info("task progress monitoring started", fields...)
			case constant.TaskModeCdcConsume:
				var partitionIds []int
				cp.MsgConsumeCounts.Range(func(key, value any) bool {
					msgCounts := value.(uint64)
					rowsPerSec = float64(msgCounts) / elapsed
					byteVal, _ := cp.MsgConsumeBytes.Load(key)
					bytesPerSec := float64(byteVal.(uint64)) / elapsed
					delayVal, _ := cp.MsgConsumeBytes.Load(key)
					avgDelay := delayVal.(time.Duration).Seconds() / float64(msgCounts)

					fields = append(fields, zap.Any("processed_partition", key))
					fields = append(fields, zap.String("processed_msgs/sec", fmt.Sprintf("%2.f", math.Round(rowsPerSec))))
					fields = append(fields, zap.String("processed_bytes/sec(KB)", fmt.Sprintf("%4.f", math.Round(bytesPerSec/1024))))
					fields = append(fields, zap.String("processed_latency(sec)", fmt.Sprintf("%2.f", math.Round(avgDelay))))
					logger.Info("task progress monitoring started", fields...)

					partitionIds = append(partitionIds, key.(int))
					return true
				})

				// reset
				for _, id := range partitionIds {
					p.ResetMsgConsumeCounts(id)
					p.ResetMsgConsumeBytes(id)
					p.ResetMsgConsumeDelay(id)
				}
				p.StartedTime = currentTime
			}
		}
	}
}

func (p *Progress) Close() {
	switch p.TaskMode {
	case constant.TaskModeStructMigrate:
		logger.Info("task progress monitoring closed",
			zap.String("task_name", p.TaskName),
			zap.String("task_mode", p.TaskMode),
			zap.String("task_flow", p.TaskFlow),
			zap.Uint64("table_counts", p.TableCounts),
			zap.Uint64("tables_completed", p.TableNameProcessed),
			zap.String("tables_completion_ratio", fmt.Sprintf("%2.f%%", float64(p.TableNameProcessed)/float64(p.TableCounts)*100)),
			zap.String("cost", time.Since(p.StartedTime).String()))
	case constant.TaskModeStmtMigrate, constant.TaskModeCSVMigrate, constant.TaskModeDataCompare, constant.TaskModeDataScan:
		var fileds []zap.Field

		fileds = append(fileds,
			zap.String("task_name", p.TaskName),
			zap.String("task_mode", p.TaskMode),
			zap.String("task_flow", p.TaskFlow),
			zap.Uint64("table_counts", p.TableCounts),
			zap.Uint64("rows_counts", p.TableRowCounts),
			zap.Uint64("tables_completed", p.TableNameProcessed),
			zap.String("tables_completion_ratio", fmt.Sprintf("%2.f%%", float64(p.TableNameProcessed)/float64(p.TableCounts)*100)),
			zap.Uint64("chunks_completed", p.TableChunksProcessed),
			zap.String("chunks_completion_ratio", fmt.Sprintf("%2.f%%", float64(p.TableChunksProcessed)/float64(p.TableChunkCounts)*100)),
		)
		fileds = append(fileds, zap.String("cost", time.Since(p.StartedTime).String()))
		logger.Info("task progress monitoring closed", fileds...)
	case constant.TaskModeCdcConsume:
		p.MsgConsumeCounts.Range(func(key, value any) bool {
			logger.Info("task progress monitoring closed",
				zap.String("task_name", p.TaskName),
				zap.String("task_mode", p.TaskMode),
				zap.String("task_flow", p.TaskFlow),
				zap.Any("closed_partition", key),
				zap.Time("last_progress_time", p.StartedTime))
			return true
		})
	}
	p.Stop()
	close(p.Done)
}

func (p *Progress) Stop() {
	p.Ticker.Stop()
}

func calcSuccRatioResult(successChunkNums, totalChunks int, multiplier uint64) uint64 {
	if totalChunks == 0 {
		return 0
	}
	ratio := float64(successChunkNums) / float64(totalChunks)
	roundedRatio := math.Ceil(ratio*100) / 100
	// After multiplying with the integer, round up
	result := math.Ceil(roundedRatio * float64(multiplier))
	return uint64(result)
}
