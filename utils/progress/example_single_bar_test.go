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
package progress_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/wentaojin/dbms/utils/progress"
)

func ExampleSingleBar() {
	b := progress.NewSingleBar("Prefix")

	b.UpdateDisplay(&progress.DisplayProps{
		Prefix: "Prefix",
		Suffix: "Suffix",
	})

	n := 3

	go func() {
		time.Sleep(time.Second)
		for i := 0; i < n; i++ {
			b.UpdateDisplay(&progress.DisplayProps{
				Prefix: "Prefix" + strconv.Itoa(i),
				Suffix: "Suffix" + strconv.Itoa(i),
			})
			time.Sleep(time.Second)
		}
	}()

	b.StartRenderLoop()

	time.Sleep(time.Second * time.Duration(n+1))

	b.UpdateDisplay(&progress.DisplayProps{
		Mode: progress.ModeDone,
		// Mode:   progress.ModeError,
		Prefix: "Prefix",
	})

	b.StopRenderLoop()
}

func ExampleSingleBar_err() {
	b := progress.NewSingleBar("Prefix")

	b.UpdateDisplay(&progress.DisplayProps{
		Prefix: "Prefix",
		Suffix: "Suffix",
	})

	n := 3

	go func() {
		time.Sleep(time.Second)
		for i := 0; i < n; i++ {
			b.UpdateDisplay(&progress.DisplayProps{
				Prefix: "Prefix" + strconv.Itoa(i),
				Suffix: "Suffix" + strconv.Itoa(i),
			})
			time.Sleep(time.Second)
		}
	}()

	b.StartRenderLoop()

	time.Sleep(time.Second * time.Duration(n+1))

	b.UpdateDisplay(&progress.DisplayProps{
		Mode:   progress.ModeError,
		Prefix: "Prefix",
		Detail: errors.New("expected failure").Error(),
	})

	b.StopRenderLoop()
}

func TestExampleOutput(t *testing.T) {
	if !testing.Verbose() {
		return
	}
	ExampleSingleBar()
	ExampleSingleBar_err()
}
