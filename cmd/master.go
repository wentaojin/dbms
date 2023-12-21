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
package main

import (
	"context"
	"log"
	"os"

	"github.com/wentaojin/dbms/version"

	"go.uber.org/zap"

	"github.com/wentaojin/dbms/signal"

	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/master"
)

func main() {
	cfg := master.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatalf("start meta failed. error is [%s], Use '--help' for help.", err)
	}

	logger.NewRootLogger(cfg.LogConfig)

	version.RecordAppVersion("dbms", cfg.String())

	ctx, cancel := context.WithCancel(context.Background())

	srv := master.NewServer(cfg)
	err := srv.Start(ctx)
	if err != nil {
		logger.Fatal("server start failed", zap.Error(err))
		os.Exit(1)
	}

	signal.SetupSignalHandler(func() {
		cancel()
	})

	<-ctx.Done()

	srv.Close()

	err = logger.Sync()
	if err != nil {
		logger.Fatal("sync log", zap.Error(err))
		os.Exit(1)
	}
}
