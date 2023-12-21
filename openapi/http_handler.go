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
package openapi

import (
	"bytes"
	"net/http"
	"net/http/pprof"

	"github.com/gin-gonic/gin"
)

type responseWriter struct {
	gin.ResponseWriter
	b *bytes.Buffer
}

// Write rewrite Write([]byte) (int, error) method
func (w responseWriter) Write(b []byte) (int, error) {
	w.b.Write(b)
	return w.ResponseWriter.Write(b)
}

// GetGinHTTPResponse gets gin http response
func GetGinHTTPResponse(c *gin.Context) string {
	respWrite := &responseWriter{
		c.Writer,
		bytes.NewBuffer([]byte{}),
	}
	c.Writer = respWrite

	c.Next()

	return respWrite.b.String()
}

// GetHTTPDebugHandler returns a HTTP handler to handle debug information.
func GetHTTPDebugHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
