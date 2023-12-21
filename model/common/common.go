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
package common

import (
	"context"
	"time"

	"gorm.io/gorm"
)

type Entity struct {
	Comment   string    `gorm:"type:varchar(1000);comment:comment content" json:"comment"`
	CreatedAt time.Time `gorm:"<-:create" json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type GormDB struct {
	MetaDB *gorm.DB
}

func WarpDB(db *gorm.DB) GormDB {
	return GormDB{MetaDB: db}
}

type ctxTxnKeyStruct struct{}

var ctxTxnKey = ctxTxnKeyStruct{}

func CtxWithTransaction(ctx context.Context, db *gorm.DB) context.Context {
	return context.WithValue(ctx, ctxTxnKey, db)
}

func (m *GormDB) DB(ctx context.Context) *gorm.DB {
	iface := ctx.Value(ctxTxnKey)

	if iface != nil {
		tx, ok := iface.(*gorm.DB)
		if !ok {
			return nil
		}

		return tx
	}

	return m.MetaDB.WithContext(ctx)
}
