/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package schemaregistry

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/linkedin/goavro"
	"github.com/pickme-go/errors"
)

// Encoder holds the reference to Registry and Subject which can be used to encode and decode messages
type Encoder struct {
	subject  *Subject
	registry *Registry
	codec    *goavro.Codec
}

// NewEncoder return the pointer to a Encoder for given Subject from the Registry
func NewEncoder(reg *Registry, subject *Subject) (*Encoder, error) {
	codec, err := goavro.NewCodec(subject.Schema)
	if err != nil {
		reg.logger.Error(fmt.Sprintf(`cannot init encoder due to codec failed due to %+v`, err))
		return nil, errors.WithPrevious(err, fmt.Sprintf(`cannot init encoder due to codec failed due to %+v`, err))
	}

	return &Encoder{
		subject:  subject,
		registry: reg,
		codec:    codec,
	}, nil
}

// Encode return a byte slice with a avro encoded message. magic byte and schema id will be appended to its beginning
//	╔════════════════════╤════════════════════╤══════════════════════╗
//	║ magic byte(1 byte) │ schema id(4 bytes) │ AVRO encoded message ║
//	╚════════════════════╧════════════════════╧══════════════════════╝
//
func (s *Encoder) Encode(data interface{}) ([]byte, error) {
	return encode(s.subject.Id, s.codec, data)
}

// Decode returns the decoded go interface of avro encoded message and error if its unable to decode
func (s *Encoder) Decode(data []byte) (interface{}, error) {
	return decode(s.registry.idMap, data)
}

func encodePrefix(id int) []byte {
	byt := make([]byte, 5)
	binary.BigEndian.PutUint32(byt[1:], uint32(id))
	return byt
}

func decodePrefix(byt []byte) int {
	return int(binary.BigEndian.Uint32(byt[1:5]))
}

//Schema return the subject asociated with the Encoder
func (s *Encoder) Schema() string {
	return s.subject.Schema
}

func encode(subjectId int, codec *goavro.Codec, data interface{}) ([]byte, error) {
	byt, err := json.Marshal(data)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`json marshal failed due to %+v`, err))
	}

	native, _, err := codec.NativeFromTextual(byt)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`native from textual failed due to %+v`, err))
	}

	magic := encodePrefix(subjectId)

	return codec.BinaryFromNative(magic, native)
}

// decode returns the decoded go interface of avro encoded message and error if its unable to decode
func decode(encoders map[int]*Encoder, data []byte) (interface{}, error) {
	if len(data) < 5 {
		return nil, errors.New(`message length is zero`)
	}

	schemaID := decodePrefix(data)

	encoder, ok := encoders[schemaID]
	if !ok {
		return nil, errors.New(fmt.Sprintf(`schema id [%d] dose not registred`, schemaID))
	}

	native, _, err := encoder.codec.NativeFromBinary(data[5:])
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`schema id [%d] dose not registred`, schemaID))
	}

	byt, err := encoder.codec.TextualFromNative(nil, native)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`textual from native failed due to %+v`, err))
	}

	if encoder.subject.JsonDecoder == nil {
		return nil, errors.New(fmt.Sprintf(`json decoder does not exist for schema %d`, schemaID))
	}

	return encoder.subject.JsonDecoder(byt)
}
