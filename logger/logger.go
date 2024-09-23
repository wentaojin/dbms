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
	"strings"
	"time"

	logger2 "gorm.io/gorm/logger"

	"moul.io/zapgorm2"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	LogTimeFmt = "2006-01-02 15:04:05.000"
)

var logger *zap.Logger

type Config struct {
	LogLevel   string `toml:"log-level" json:"log-level"`
	LogFile    string `toml:"log-file" json:"log-file"`
	MaxSize    int    `toml:"max-size" json:"max-size"`
	MaxDays    int    `toml:"max-days" json:"max-days"`
	MaxBackups int    `toml:"max-backups" json:"max-backups"`
}

func NewRootLogger(cfg *Config) {
	encoder := getEncoder()
	writeSyncer := getWriteSyncer(cfg)
	levelEnabler := getLevelEnabler(cfg.LogLevel)
	// consoleEncoder := GetConsoleEncoder()
	newCore := zapcore.NewTee(
		zapcore.NewCore(encoder, writeSyncer, levelEnabler), // write file
		//zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stdout), zapcore.DebugLevel), // console
	)
	logger = zap.New(newCore, zap.AddCaller())
	zap.ReplaceGlobals(logger)
}

func GetRootLogger() *zap.Logger {
	return logger
}

func GetGormLogger(logLevel string, slowThreshold uint64) zapgorm2.Logger {
	gormLogger := zapgorm2.New(GetRootLogger())
	gormLogger.SlowThreshold = time.Duration(slowThreshold) * time.Millisecond

	spaceLogLevel := strings.TrimSpace(logLevel)
	// reduce log
	if strings.EqualFold(spaceLogLevel, "info") || strings.EqualFold(spaceLogLevel, "warn") {
		gormLogger.LogLevel = logger2.Warn
	} else if strings.EqualFold(spaceLogLevel, "silent") {
		gormLogger.LogLevel = logger2.Silent
	} else if strings.EqualFold(spaceLogLevel, "error") {
		gormLogger.LogLevel = logger2.Error
	}
	// avoid First Method logger output "record not found" error
	gormLogger.IgnoreRecordNotFoundError = true
	gormLogger.LogMode(gormLogger.LogLevel)
	gormLogger.SetAsDefault()
	return gormLogger
}

// getEncoder custom logger encoder
func getEncoder() zapcore.Encoder {
	return zapcore.NewConsoleEncoder(
		zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller_line",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "message",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    cEncodeLevel,
			EncodeTime:     cEncodeTime,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   cEncodeCaller,
		})
}

// getConsoleEncoder log output console
func getConsoleEncoder() zapcore.Encoder {
	return zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
}

// GetWriteSyncer custom write syncer
func getWriteSyncer(cfg *Config) zapcore.WriteSyncer {
	lumberJackLogger := &lumberjack.Logger{
		Filename:   cfg.LogFile,
		MaxSize:    cfg.MaxSize,
		MaxAge:     cfg.MaxDays,
		MaxBackups: cfg.MaxBackups,
	}
	return zapcore.AddSync(lumberJackLogger)
}

// getLevelEnabler used for get custom log level
func getLevelEnabler(logLevel string) zapcore.Level {
	switch strings.ToUpper(logLevel) {
	case "INFO":
		return zapcore.InfoLevel
	case "WARN":
		return zapcore.WarnLevel
	case "FATAL":
		return zapcore.FatalLevel
	case "DEBUG":
		return zapcore.DebugLevel
	case "ERROR":
		return zapcore.ErrorLevel
	case "PANIC":
		return zapcore.PanicLevel
	case "DPANIC":
		return zapcore.DPanicLevel
	default:
		return zapcore.InfoLevel
	}
}

// cEncodeLevel custom log level display
func cEncodeLevel(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + level.CapitalString() + "]")
}

// cEncodeTime custom time format display
func cEncodeTime(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + t.Format(LogTimeFmt) + "]")
}

// cEncodeCaller custom line number display
func cEncodeCaller(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + caller.TrimmedPath() + "]")
}
