/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package schemaregistry

// Encoder holds the reference to Registry and Subject which can be used to encode and decode messages
type GenericEncoder struct {
	registry *Registry
}

// NewEncoder return the pointer to a Encoder for given Subject from the Registry
func NewGenericEncoder(reg *Registry) (*Encoder, error) {
	return &Encoder{
		registry: reg,
	}, nil
}

func (s *GenericEncoder) Encode(data interface{}) ([]byte, error) {
	panic(`generic encoder does not support encoding of messages`)
}

// Decode returns the decoded go interface of avro encoded message and error if its unable to decode
func (s *GenericEncoder) Decode(data []byte) (interface{}, error) {
	return decode(s.registry.idMap, data)
}

//Schema return the subject asociated with the Encoder
func (s *GenericEncoder) Schema() string {
	return `generic`
}
