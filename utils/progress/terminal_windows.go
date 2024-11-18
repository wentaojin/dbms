//go:build windows
// +build windows

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
package progress

import (
	"fmt"
	"io"
	"os"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/sys/windows"
)

var (
	termSizeWidth  = atomic.Int32{}
	termSizeHeight = atomic.Int32{}
)

func updateTerminalSize() error {
	handle := windows.Handle(os.Stdout.Fd())
	var info windows.ConsoleScreenBufferInfo
	err := windows.GetConsoleScreenBufferInfo(handle, &info)
	if err != nil {
		return err
	}
	termSizeWidth.Store(int32(info.Window.Right - info.Window.Left + 1))
	termSizeHeight.Store(int32(info.Window.Bottom - info.Window.Top + 1))
	return nil
}

func moveCursorUp(w io.Writer, n int) {
	_, _ = fmt.Fprintf(w, "\033[%dA", n)
}

func moveCursorDown(w io.Writer, n int) {
	_, _ = fmt.Fprintf(w, "\033[%dB", n)
}

func moveCursorToLineStart(w io.Writer) {
	_, _ = fmt.Fprintf(w, "\r")
}

func clearLine(w io.Writer) {
	_, _ = fmt.Fprintf(w, "\033[2K")
}

func init() {
	_ = updateTerminalSize()

	go func() {
		// interval check
		ticker := time.NewTicker(60 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			handle := windows.Handle(os.Stdout.Fd())
			var info windows.ConsoleScreenBufferInfo
			_ = windows.GetConsoleScreenBufferInfo(handle, &info)

			if (int32(info.Window.Right-info.Window.Left+1) != termSizeWidth.Load()) || (int32(info.Window.Bottom-info.Window.Top+1) != termSizeHeight.Load()) {
				_ = updateTerminalSize()
			}
		}
	}()
}
