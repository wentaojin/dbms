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
package oceanbase

import (
	"encoding/json"
	"fmt"
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
	HasNext() (string, bool, error)
	// NextRowChangedEvent returns the next row changed event if exists
	NextRowChangedEvent() (*RowChangedEvent, error)
	// NextDDLEvent returns the next DDL event if exists
	NextDDLEvent() (*DDLChangedEvent, error)
}

// Decoder decodes the byte of a batch into the original messages.
type Decoder struct {
	keyBytes   []byte
	valueBytes []byte

	message *Message
}

// NewDecoder creates a new Decoder.
func NewDecoder() RowEventDecoder {
	return &Decoder{}
}

// AddKeyValue implements the RowEventDecoder interface
func (b *Decoder) AddKeyValue(key, value []byte) error {
	if len(b.keyBytes) != 0 || len(b.valueBytes) != 0 {
		return fmt.Errorf("codec invalid data, decoder key and value not nil")
	}

	b.keyBytes = key
	b.valueBytes = value
	return nil
}

// HasNext implements the RowEventDecoder interface
func (b *Decoder) HasNext() (string, bool, error) {
	isNext, err := b.hashNext()
	if err != nil || !isNext {
		return "", isNext, err
	}

	if err := json.Unmarshal(b.valueBytes, b.message); err != nil {
		return "", isNext, err
	}
	return b.message.RecordType, true, nil
}

func (b *Decoder) hashNext() (bool, error) {
	keyLen := len(b.keyBytes)
	valueLen := len(b.valueBytes)
	if keyLen > 0 && valueLen > 0 {
		return true, nil
	}
	if keyLen == 0 && valueLen != 0 || keyLen != 0 && valueLen == 0 {
		return false, fmt.Errorf("meet invalid data, key Length [%d], value length [%d]", keyLen, valueLen)
	}
	return false, nil
}

// NextDDLEvent implements the RowEventDecoder interface
func (b *Decoder) NextDDLEvent() (*DDLChangedEvent, error) {
	if b.message.RecordType != MsgRecordTypeDDL {
		return nil, fmt.Errorf("codec invalid data, not found ddl event message")
	}

	b.valueBytes = nil
	return b.message.DecodeDDLChangedEvent(), nil
}

// NextRowChangedEvent implements the RowEventDecoder interface
func (b *Decoder) NextRowChangedEvent() (*RowChangedEvent, error) {
	switch b.message.RecordType {
	case MsgRecordTypeDelete, MsgRecordTypeInsert, MsgRecordTypeUpdate, MsgRecordTypeROW:
		return b.message.DecodeRowChangedEvent(), nil
	case MsgRecordTypeHeartbeat:
		return nil, fmt.Errorf("codec invalid data, should not be recevie heartbeat message")
	default:
		return nil, fmt.Errorf("codec invalid data, not found row event message")
	}
}
