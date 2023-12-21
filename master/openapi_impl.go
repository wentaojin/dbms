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
	"fmt"
	"net/http"
	"net/http/httputil"
	"strings"

	"github.com/wentaojin/dbms/utils/stringutil"

	middleware "github.com/deepmap/oapi-codegen/pkg/gin-middleware"
	"github.com/getkin/kin-openapi/openapi3"

	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/openapi"
	"go.uber.org/zap"
)

// InitOpenAPIHandler returns a HTTP handler to handle dbms-master apis
func (s *Server) InitOpenAPIHandler() (*gin.Engine, error) {
	swagger, err := openapi.GetSwagger()
	if err != nil {
		return nil, fmt.Errorf("openapi get swagger failed: [%v]", err)
	}
	// servers configure sever api base path, avoid gin-middleware request valid failed, report error {"error":"no matching operation was found"}
	swagger.Servers = openapi3.Servers{&openapi3.Server{URL: openapi.DBMSAPIBasePath}}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	// middlewares
	// add a ginzap middleware, which:
	//   - log requests, like a combined access and error log.
	r.Use(ginzap.GinzapWithConfig(logger.GetRootLogger().With(zap.String("component", "gin")), &ginzap.Config{
		TimeFormat: logger.LogTimeFmt,
		UTC:        false}))

	// logs all panic to error log
	//   - stack means whether output the stack info.
	r.Use(ginzap.RecoveryWithZap(logger.GetRootLogger().With(zap.String("component", "gin")), true))

	// reverse proxy
	r.Use(s.reverseRequestToLeader())

	// db ping
	r.Use(s.pingDBConnReady())

	// use validation middleware to check all requests against the OpenAPI schema.
	r.Use(middleware.OapiRequestValidatorWithOptions(swagger, &middleware.Options{
		SilenceServersWarning: true, // forbid servers parameter check warn
	}))

	// register handlers
	openapi.RegisterHandlersWithOptions(r, s, openapi.GinServerOptions{BaseURL: openapi.DBMSAPIBasePath})

	return r, nil
}

// reverseRequestToLeader used for reverses request to leader
func (s *Server) reverseRequestToLeader() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		isLeader, err := s.election.IsLeader(ctx)
		if err != nil {
			c.IndentedJSON(http.StatusOK, openapi.Response{
				Code:  http.StatusBadRequest,
				Error: err.Error(),
			})
			return
		}
		if isLeader {
			c.Next()
		} else {
			leaderAddr, err := s.election.Leader(ctx)
			if err != nil {
				c.IndentedJSON(http.StatusOK, openapi.Response{
					Code:  http.StatusBadRequest,
					Error: err.Error(),
				})
				return
			}

			// simpleProxy just reverse to leader host
			simpleProxy := httputil.ReverseProxy{
				Director: func(req *http.Request) {
					req.URL.Scheme = "http"
					req.URL.Host = leaderAddr
					req.Host = leaderAddr
				},
			}

			logger.Info("reverse request to leader", zap.String("Request URL", c.Request.URL.String()), zap.String("leader", leaderAddr))

			simpleProxy.ServeHTTP(c.Writer, c.Request)
			c.Abort()
		}
	}
}

// pingDBConnReady used for ping db connection is whether active
func (s *Server) pingDBConnReady() gin.HandlerFunc {
	return func(c *gin.Context) {
		// exclude /api/v1/database interface request
		if strings.EqualFold(c.Request.URL.Path, stringutil.StringBuilder(openapi.DBMSAPIBasePath, openapi.APIDatabasePath)) {
			c.Next()
		} else {
			if !s.dbConnReady.Load() {
				c.IndentedJSON(http.StatusOK, openapi.Response{
					Code:  http.StatusBadRequest,
					Error: fmt.Sprintf("database connection is not ready, disable service, please create database and wait retrying"),
				})
				c.Abort()
				return
			}
			c.Next()
		}
	}
}

func (s *Server) APIListDatabase(c *gin.Context) {
	database, err := s.listDatabase(c.Request.Context())
	if err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: database,
	})
}

func (s *Server) APIPutDatabase(c *gin.Context) {
	var req openapi.APIPutDatabaseJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}

	database, err := s.upsertDatabase(c.Request.Context(), req)
	if err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: database})
}

func (s *Server) APIDeleteDatabase(c *gin.Context) {
	database, err := s.deleteDatabase(c.Request.Context())
	if err != nil {
		c.IndentedJSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusNoContent, openapi.Response{
		Code: http.StatusNoContent,
		Data: database,
	})
}

func (s *Server) APIListDatasource(c *gin.Context) {
	var req openapi.APIListDatasourceJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.IndentedJSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	datasource, err := s.listDatasource(c.Request.Context(), req)
	if err != nil {
		c.IndentedJSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusCreated, openapi.Response{
		Code: http.StatusCreated,
		Data: datasource,
	})
}

func (s *Server) APIPutDatasource(c *gin.Context) {
	var req openapi.APIPutDatasourceJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	datasource, err := s.upsertDatasource(c.Request.Context(), req)
	if err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: datasource,
	})
}

func (s *Server) APIDeleteDatasource(c *gin.Context) {
	var req openapi.APIDeleteDatasourceJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.IndentedJSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	delMsg, err := s.deleteDatasource(c.Request.Context(), req)
	if err != nil {
		c.IndentedJSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusNoContent, openapi.Response{
		Code: http.StatusNoContent,
		Data: delMsg,
	})
}

func (s *Server) APIDeleteTaskMigrateRule(c *gin.Context) {
	var req openapi.APIDeleteTaskMigrateRuleJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.IndentedJSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	delMsg, err := s.deleteTaskMigrateRule(c.Request.Context(), req)
	if err != nil {
		c.IndentedJSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusNoContent, openapi.Response{
		Code: http.StatusNoContent,
		Data: delMsg,
	})
}

func (s *Server) APIListTaskMigrateRule(c *gin.Context) {
	var req openapi.APIListTaskMigrateRuleJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.IndentedJSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	listMsg, err := s.listTaskMigrateRule(c.Request.Context(), req)
	if err != nil {
		c.IndentedJSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusCreated, openapi.Response{
		Code: http.StatusCreated,
		Data: listMsg,
	})
}

func (s *Server) APIPutTaskMigrateRule(c *gin.Context) {
	var req openapi.APIPutTaskMigrateRuleJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	upsertMsg, err := s.upsertTaskMigrateRule(c.Request.Context(), req)
	if err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: upsertMsg,
	})
}

func (s *Server) APIDeleteStructMigrateTask(c *gin.Context) {
	var req openapi.APIDeleteStructMigrateTaskJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.IndentedJSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	delMsg, err := s.deleteStructMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.IndentedJSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusNoContent, openapi.Response{
		Code: http.StatusNoContent,
		Data: delMsg,
	})
}

func (s *Server) APIListStructMigrateTask(c *gin.Context) {
	var req openapi.APIListStructMigrateTaskJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.IndentedJSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	listMsg, err := s.listStructMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.IndentedJSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusCreated, openapi.Response{
		Code: http.StatusCreated,
		Data: listMsg,
	})
}

func (s *Server) APIPutStructMigrateTask(c *gin.Context) {
	var req openapi.APIPutStructMigrateTaskJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	upsertMsg, err := s.upsertStructMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: upsertMsg,
	})
}

func (s *Server) APIPutTask(c *gin.Context) {
	var req openapi.APIPutTaskJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	task, err := s.upsertTask(c.Request.Context(), req)
	if err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: task,
	})
}

func (s *Server) APIDeleteTask(c *gin.Context) {
	var req openapi.APIDeleteTaskJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	task, err := s.deleteTask(c.Request.Context(), req)
	if err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: task,
	})
}

func (s *Server) APIKillTask(c *gin.Context) {
	var req openapi.APIKillTaskJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	task, err := s.killTask(c.Request.Context(), req)
	if err != nil {
		c.IndentedJSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.IndentedJSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: task,
	})
}
