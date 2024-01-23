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
package logger

import (
	"fmt"

	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type CronLogger struct {
	L *zap.Logger
}

func NewCronLogger(l *zap.Logger) cron.Logger {
	return &CronLogger{L: l.WithOptions(zap.AddCallerSkip(2))} // FIXME: caller skip depth
}

func (cl *CronLogger) log(lvl zapcore.Level, msg string, keysAndValues ...interface{}) {
	if keysAndValues == nil || len(keysAndValues) == 0 {
		cl.L.Log(lvl, msg)
	} else if len(keysAndValues)%2 != 0 {
		cl.L.Log(lvl, fmt.Sprintf("%s; %v", msg, keysAndValues))
	} else {
		fields := make([]zap.Field, 0, len(keysAndValues)/2)
		for ix := 0; ix < len(keysAndValues); ix += 2 {
			key, ok := keysAndValues[ix].(string)
			if !ok {
				key = fmt.Sprintf("%v", keysAndValues[ix])
			}
			fields = append(fields, zap.Any(key, keysAndValues[ix+1]))
		}
		cl.L.Log(lvl, msg, fields...)
	}
}

func (cl *CronLogger) Info(msg string, keysAndValues ...interface{}) {
	cl.log(zapcore.InfoLevel, msg, keysAndValues...)
}

func (cl *CronLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	cl.log(zapcore.ErrorLevel, msg, keysAndValues...)
}

func (cl *CronLogger) Printf(string, ...interface{}) {

}
