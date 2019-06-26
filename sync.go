package schema_registry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/olekukonko/tablewriter"
	"github.com/pickme-go/k-stream/consumer"
	"sort"
)

type backgroundSync struct {
	bootstrapServers []string
	storageTopic     string
	registry         *Registry
	consumer         consumer.PartitionConsumer
	synced           bool
}

type key struct {
	Subject string `json:"subject"`
	Keytype string `json:"keytype"`
	Version int    `json:"version"`
}

type value struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Id      int    `json:"id"`
	Schema  string `json:"schema"`
	Deleted bool   `json:"deleted"`
}

func newSync(bootstrapServers []string, storageTopic string, registry *Registry) (*backgroundSync, error) {
	config := consumer.NewPartitionConsumerConfig()
	config.BootstrapServers = bootstrapServers
	config.Logger = registry.logger

	c, err := consumer.NewPartitionConsumer(config)
	if err != nil {
		return nil, err
	}

	return &backgroundSync{
		bootstrapServers: bootstrapServers,
		consumer:         c,
		registry:         registry,
		storageTopic:     storageTopic,
	}, nil

}

func (s *backgroundSync) start() error {

	s.registry.logger.Info(`schema-registry.sync`, `background sync started...`)
	pConsumer, err := s.consumer.Consume(s.storageTopic, 0, sarama.OffsetOldest)
	if err != nil {
		return err
	}

	synced := make(chan bool, 1)
	go s.startConsumer(pConsumer, synced)
	<-synced

	s.print()
	s.registry.logger.Info(`schema-registry.sync`, `background sync done`)

	return nil
}

func (s *backgroundSync) startConsumer(events <-chan consumer.Event, syncDone chan bool) {
	for ev := range events {
		switch e := ev.(type) {
		case *consumer.Record:
			s.apply(e.Key, e.Value)
		case *consumer.PartitionEnd:
			s.synced = true
			syncDone <- true
		}
	}
}

func (s *backgroundSync) print() {
	b := new(bytes.Buffer)
	table := tablewriter.NewWriter(b)
	table.SetHeader([]string{`Schema Id`, `subject`, `version`, `json decoder`})

	s.registry.logger.Warn(``, s.registry.schemas)
	for _, subject := range s.registry.schemas {
		for _, version := range subject {
			table.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT})
			table.SetAutoFormatHeaders(true)
			table.Append([]string{
				fmt.Sprint(version.subject.Id),
				fmt.Sprint(version.subject.Subject),
				fmt.Sprint(Version(version.subject.Version)),
				fmt.Sprint(version.subject.JsonDecoder != nil),
				//fmt.Sprint(version.subject.Schema),
			})
		}

	}
	table.Render()
	s.registry.logger.Info(`schema-registry.sync`, fmt.Sprintf("schemas registered\n%s", b.String()))
}

func (s *backgroundSync) apply(keyByt []byte, valByt []byte) {
	key := key{}
	value := value{}

	// empty keys and values has to be ignored
	if len(keyByt) < 1 || len(valByt) < 1 {
		return
	}

	if err := json.Unmarshal(keyByt, &key); err != nil {
		s.registry.logger.Error(`schema-registry.sync`, fmt.Sprintf(`key unmarshal key due to %+v`, err))
		return
	}

	if err := json.Unmarshal(valByt, &value); err != nil {
		s.registry.logger.Error(`schema-registry.sync`, fmt.Sprintf(`key unmarshal value due to %+v`, err))
		return
	}

	// we only need schemas
	if key.Keytype != `SCHEMA` {
		return
	}

	if value.Subject == `` {
		return
	}

	// if subject is not registered ignore (we don't need the entire schema registry here)
	if encoder, ok := s.registry.schemas[value.Subject]; ok {

		if _, ok := encoder[value.Version]; !ok {

			// get previous version encoder
			var previous = func(v int, encoders map[int]*Encoder) *Encoder {
				var versions []int
				for _, encoder := range encoders {
					versions = append(versions, encoder.subject.Version)
				}

				sort.Ints(versions)

				for _, ver := range versions {
					if ver < v {
						return encoders[ver]
					}
				}

				return nil
			}

			// get previous versions decoder (assumption is new version is always compatible with the old version)
			// TODO add compatibility check with schema registry and throw error if fails
			if prv := previous(value.Version, encoder); prv != nil {

				// assume this is only an encoder
				if prv.subject.JsonDecoder == nil {
					return
				}

				e, err := NewEncoder(s.registry, &Subject{
					Subject:     value.Subject,
					Version:     value.Version,
					Schema:      value.Schema,
					Id:          value.Id,
					JsonDecoder: prv.subject.JsonDecoder,
				})

				if err != nil {
				}

				encoder[value.Version] = e

				s.registry.schemas[value.Subject] = encoder
				s.registry.idMap[value.Id] = e

				if s.synced {
					s.print()
					s.registry.logger.Info(`schema-registry.sync`, `registry updated`)
				}
				return
			}
		}
	}

}
