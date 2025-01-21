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
package tidb

import (
	"encoding/binary"
	"fmt"

	"github.com/wentaojin/dbms/message"
)

// RowEventDecoder is an abstraction for events decoder
// this interface is only for testing now
type RowEventDecoder interface {
	// AddKeyValue add the received key and values to the decoder,
	// should be called before `HasNext`
	// decoder decode the key and value into the event format.
	AddKeyValue(key, value []byte) error

	// HasNext returns
	//     1. the type of the next event
	//     2. a bool if the next event is exist
	//     3. error
	HasNext() (MsgEventType, bool, error)
	// NextResolvedEvent returns the next resolved event if exists
	NextResolvedEvent() (uint64, error)
	// NextRowChangedEvent returns the next row changed event if exists
	NextRowChangedEvent() (*RowChangedEvent, error)
	// NextDDLEvent returns the next DDL event if exists
	NextDDLEvent() (*DDLChangedEvent, error)
}

/*
 Solved Case:
 	key [A{"ts":454376015366979596,"scm":"marvin","tbl":"t1","rid":1,"t":1}A{"ts":454376015366979596,"scm":"marvin","tbl":"t1","rid":3,"t":1}]
 	values [I{"u":{"id":{"t":3,"h":true,"f":11,"v":1},"val":{"t":15,"f":64,"v":"aa"}}}I{"u":{"id":{"t":3,"h":true,"f":11,"v":3},"val":{"t":15,"f":64,"v":"cc"}}}]
*/
// BatchDecoder decodes the byte of a batch into the original messages.
type BatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte

	nextKey        *MessageEventKey
	nextEvent      *RowChangedEvent
	msgCompresion  string
	caseFieldRuleS string
}

// NewBatchDecoder creates a new BatchDecoder.
func NewBatchDecoder(caseFieldRuleS, msgCompresion string) RowEventDecoder {
	return &BatchDecoder{
		caseFieldRuleS: caseFieldRuleS,
		msgCompresion:  msgCompresion,
	}
}

// AddKeyValue implements the RowEventDecoder interface
func (b *BatchDecoder) AddKeyValue(key, value []byte) error {
	if len(b.keyBytes) != 0 || len(b.valueBytes) != 0 {
		return fmt.Errorf("open-protocol codec invalid data, decoder key and value not nil")
	}
	version := binary.BigEndian.Uint64(key[:8])
	key = key[8:]
	if version != BatchVersion {
		return fmt.Errorf("open-protocol codec invalid data, unexpected key format version [%d], should be [%d]", version, BatchVersion)
	}

	b.keyBytes = key
	b.valueBytes = value

	return nil
}

// HasNext implements the RowEventDecoder interface
func (b *BatchDecoder) HasNext() (MsgEventType, bool, error) {
	isNext, err := b.hashNext()
	if err != nil || !isNext {
		return 0, isNext, err
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}

	// Batch key-value
	if b.nextKey.MsgEventType == MsgEventTypeRow {
		valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
		value := b.valueBytes[8 : valueLen+8]
		b.valueBytes = b.valueBytes[valueLen+8:]

		rowMsg := new(MessageRowEventValue)

		value, err := message.Decompress(b.msgCompresion, value)
		if err != nil {
			return MsgEventTypeUnknown, false, fmt.Errorf("open-protocol codec invalid data, decompress data failed: %v", err)
		}

		if err := rowMsg.Decode(b.caseFieldRuleS, value); err != nil {
			return b.nextKey.MsgEventType, false, err
		}
		event, err := MsgConvRowChangedEvent(b.nextKey, rowMsg)
		if err != nil {
			return MsgEventTypeUnknown, false, fmt.Errorf("open-protocol codec invalid data, message conv row changed failed: %v", err)
		}
		b.nextEvent = event
	}

	return b.nextKey.MsgEventType, true, nil
}

func (b *BatchDecoder) hashNext() (bool, error) {
	keyLen := len(b.keyBytes)
	valueLen := len(b.valueBytes)
	if keyLen > 0 && valueLen > 0 {
		return true, nil
	}
	if keyLen == 0 && valueLen != 0 || keyLen != 0 && valueLen == 0 {
		return false, fmt.Errorf("open-protocol meet invalid data, key Length [%d], value length [%d]", keyLen, valueLen)
	}
	return false, nil
}

func (b *BatchDecoder) decodeNextKey() error {
	keyLen := binary.BigEndian.Uint64(b.keyBytes[:8])
	key := b.keyBytes[8 : keyLen+8]
	msgKey := new(MessageEventKey)
	err := msgKey.Decode(key)
	if err != nil {
		return err
	}
	b.nextKey = msgKey
	b.keyBytes = b.keyBytes[keyLen+8:]
	return nil
}

// NextResolvedEvent implements the RowEventDecoder interface
func (b *BatchDecoder) NextResolvedEvent() (uint64, error) {
	if b.nextKey.MsgEventType != MsgEventTypeResolved {
		return 0, fmt.Errorf("open-protocol codec invalid data, not found resolved event message")
	}

	resolvedTs := b.nextKey.CommitTs
	b.nextKey = nil
	// resolved ts event's value part is empty, can be ignored.
	b.valueBytes = nil
	return resolvedTs, nil
}

// NextDDLEvent implements the RowEventDecoder interface
func (b *BatchDecoder) NextDDLEvent() (*DDLChangedEvent, error) {
	if b.nextKey.MsgEventType != MsgEventTypeDDL {
		return nil, fmt.Errorf("open-protocol codec invalid data, not found ddl event message")
	}

	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]

	value, err := message.Decompress(b.msgCompresion, value)
	if err != nil {
		return nil, fmt.Errorf("open-protocol codec invalid data, decompress DDL event failed: %v", err)
	}

	ddlMsg := new(MessageDDLEventValue)
	if err := ddlMsg.Decode(value); err != nil {
		return nil, err
	}
	ddlEvent := MsgConvDDLEvent(b.caseFieldRuleS, b.nextKey, ddlMsg)

	b.nextKey = nil
	b.valueBytes = nil
	return ddlEvent, nil
}

// NextRowChangedEvent implements the RowEventDecoder interface
func (b *BatchDecoder) NextRowChangedEvent() (*RowChangedEvent, error) {
	if b.nextKey.MsgEventType != MsgEventTypeRow {
		return nil, fmt.Errorf("open-protocol codec invalid data, not found row event message")
	}
	b.nextKey = nil
	return b.nextEvent, nil
}
