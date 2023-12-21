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

import "go.uber.org/zap"

func Info(msg string, fields ...zap.Field) {
	zap.L().Info(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	zap.L().Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	zap.L().Error(msg, fields...)
}

func Debug(msg string, fields ...zap.Field) {
	zap.L().Debug(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	zap.L().Fatal(msg, fields...)
}

func Panic(msg string, fields ...zap.Field) {
	zap.L().Panic(msg, fields...)
}

func DPanic(msg string, fields ...zap.Field) {
	zap.L().DPanic(msg, fields...)
}

func WithField(key string, value string) {
	zap.L().With(zap.String(key, value))
}

func Sync() error {
	return zap.L().Sync()
}
