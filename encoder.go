/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package schema_registry

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/linkedin/goavro"
)

type Encoder struct {
	subject  *Subject
	registry *Registry
	codec    *goavro.Codec
}

func NewEncoder(reg *Registry, subject *Subject) (*Encoder, error) {
	codec, err := goavro.NewCodec(subject.Schema)
	if err != nil {
		reg.logger.Error(`schema-registry.encoder`,
			fmt.Sprintf(`cannot init encoder due to codec failed due to %+v`, err))
		return nil, err
	}

	return &Encoder{
		subject:  subject,
		registry: reg,
		codec:    codec,
	}, nil
}

func (s *Encoder) Encode(data interface{}) ([]byte, error) {
	byt, err := json.Marshal(data)
	if err != nil {
		s.registry.logger.Error(`schema-registry.encoder`,
			fmt.Sprintf(`json marshal failed due to %+v`, err))
		return nil, err
	}

	native, _, err := s.codec.NativeFromTextual(byt)
	if err != nil {
		s.registry.logger.Error(`schema-registry.encoder`,
			fmt.Sprintf(`native from textual failed due to %+v`, err))
		return nil, err
	}

	magic := s.encodePrefix(s.subject.Id)

	return s.codec.BinaryFromNative(magic, native)
}

func (s *Encoder) Decode(data []byte) (interface{}, error) {
	if len(data) < 5 {
		s.registry.logger.Error(`schema-registry.encoder`, `message length is zero`)
		return nil, errors.New(`schema-registry.encoder: message length is zero`)
	}

	schemaId := s.decodePrefix(data)

	encoder, ok := s.registry.idMap[schemaId]
	if !ok {
		return nil, errors.New(fmt.Sprintf(`schema-registry.encoder: schema id [%d] dose not registred`, schemaId))
	}

	native, _, err := encoder.codec.NativeFromBinary(data[5:])
	if err != nil {
		s.registry.logger.Error(`schema-registry.encoder`,
			fmt.Sprintf(`native from binary failed due to %+v`, err))
		return nil, err
	}

	byt, err := encoder.codec.TextualFromNative(nil, native)
	if err != nil {
		s.registry.logger.Error(`schema-registry.encoder`,
			fmt.Sprintf(`textual from native failed due to %+v`, err))
		return nil, err
	}

	if encoder.subject.JsonDecoder == nil {
		return nil, errors.New(fmt.Sprintf(
			`schema-registry.encoder : json decoder does not exist for schema %d`, schemaId))
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

func (s *Encoder) Schema() string {
	return s.subject.Schema
}
