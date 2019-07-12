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
		reg.logger.Error(`schema-registry.encoder`, fmt.Sprintf(`cannot init encoder due to codec failed due to %+v`, err))
		return nil, errors.WithPrevious(err, `schema-registry.encoder`, fmt.Sprintf(`cannot init encoder due to codec failed due to %+v`, err))
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
	byt, err := json.Marshal(data)
	if err != nil {
		s.registry.logger.Error(`schema-registry.encoder`, fmt.Sprintf(`json marshal failed due to %+v`, err))
		return nil, errors.WithPrevious(err, `schema-registry.encoder`, fmt.Sprintf(`json marshal failed due to %+v`, err))
	}

	native, _, err := s.codec.NativeFromTextual(byt)
	if err != nil {
		s.registry.logger.Error(`schema-registry.encoder`, fmt.Sprintf(`native from textual failed due to %+v`, err))
		return nil, errors.WithPrevious(err, `schema-registry.encoder`, fmt.Sprintf(`native from textual failed due to %+v`, err))
	}

	magic := s.encodePrefix(s.subject.Id)

	return s.codec.BinaryFromNative(magic, native)
}

// Decode returns the decoded go interface of avro encoded message and error if its unable to decode
func (s *Encoder) Decode(data []byte) (interface{}, error) {
	if len(data) < 5 {
		s.registry.logger.Error(`schema-registry.encoder`, `message length is zero`)
		return nil, errors.New(`schema-registry.encoder`, `message length is zero`)
	}

	schemaID := s.decodePrefix(data)

	encoder, ok := s.registry.idMap[schemaID]
	if !ok {
		return nil, errors.New(`schema-registry.encoder`, fmt.Sprintf(`schema id [%d] dose not registred`, schemaID))
	}

	native, _, err := encoder.codec.NativeFromBinary(data[5:])
	if err != nil {
		s.registry.logger.Error(`schema-registry.encoder`, fmt.Sprintf(`native from binary failed due to %+v`, err))
		return nil, errors.WithPrevious(err, `schema-registry.encoder`, fmt.Sprintf(`schema id [%d] dose not registred`, schemaID))
	}

	byt, err := encoder.codec.TextualFromNative(nil, native)
	if err != nil {
		s.registry.logger.Error(`schema-registry.encoder`, fmt.Sprintf(`textual from native failed due to %+v`, err))
		return nil, errors.WithPrevious(err, `schema-registry.encoder`, fmt.Sprintf(`textual from native failed due to %+v`, err))
	}

	if encoder.subject.JsonDecoder == nil {
		return nil, errors.New(`schema-registry.encoder`, fmt.Sprintf(`json decoder does not exist for schema %d`, schemaID))
	}

	return encoder.subject.JsonDecoder(byt)
}

func (s *Encoder) encodePrefix(id int) []byte {
	byt := make([]byte, 5)
	binary.BigEndian.PutUint32(byt[1:], uint32(id))
	return byt
}

func (s *Encoder) decodePrefix(byt []byte) int {
	return int(binary.BigEndian.Uint32(byt[1:5]))
}

//Schema return the subject asociated with the Encoder
func (s *Encoder) Schema() string {
	return s.subject.Schema
}
