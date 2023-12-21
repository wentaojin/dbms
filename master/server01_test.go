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
package master

import (
	"context"
	"os"
	"testing"

	"github.com/wentaojin/dbms/utils/configutil"

	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/signal"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestMasterServer01(t *testing.T) {
	cfg := &Config{
		MasterOptions: &configutil.MasterOptions{
			Name:                "dbms-master00",
			DataDir:             "/Users/marvin/gostore/dbms/test",
			ClientAddr:          "127.0.0.1:2379",
			PeerAddr:            "127.0.0.1:2380",
			InitialClusterState: embed.ClusterStateFlagNew,
		},
		LogConfig: &logger.Config{
			LogLevel: "info",
			LogFile:  "/Users/marvin/gostore/dbms/test/dbms-master-2379.log",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	srv := NewServer(cfg)
	err := srv.Start(ctx)
	if err != nil {
		panic(err)
	}

	signal.SetupSignalHandler(func() {
		cancel()
	})

	<-ctx.Done()

	srv.Close()

	err = logger.Sync()
	if err != nil {
		os.Exit(1)
	}
}
