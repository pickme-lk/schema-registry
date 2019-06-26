package schema_registry

import (
	"fmt"
	"github.com/datamountaineer/schema-registry"
	"github.com/pickme-go/log"
	"sync"
)

type Version int

const VersionLatest Version = -1
const VersionAll Version = -2

func (v Version) String() string {

	if v == VersionLatest {
		return `Latest`
	}

	if v == VersionAll {
		return `All`
	}

	return fmt.Sprint(int(v))
}

type jsonDecoder func(data []byte) (v interface{}, err error)

type Subject struct {
	Schema      string      `json:"subject"` // The actual AVRO subject
	Subject     string      `json:"subject"` // Subject where the subject is registered for
	Version     int         `json:"version"` // Version within this subject
	Id          int         `json:"id"`      // Registry's unique id
	JsonDecoder jsonDecoder `json:"json_decoder"`
}

type options struct {
	backGroundSync   bool
	bootstrapServers []string
	storageTopic     string
	logger           log.PrefixedLogger
}

type Registry struct {
	schemas map[string]map[int]*Encoder // subject/version/encoder
	idMap   map[int]*Encoder
	client  *schemaregistry.Client
	mu      *sync.RWMutex
	options *options
	logger  log.PrefixedLogger
}

type Option func(*options)

func WithBackgroundSync(bootstrapServers []string, storageTopic string) Option {
	return func(options *options) {
		options.bootstrapServers = bootstrapServers
		options.storageTopic = storageTopic
		options.backGroundSync = true
	}
}

func WithLogger(logger log.PrefixedLogger) Option {
	return func(options *options) {
		options.logger = logger
	}
}

func NewRegistry(url string, opts ...Option) (*Registry, error) {

	options := new(options)
	for _, opt := range opts {
		opt(options)
	}

	if options.logger == nil {
		options.logger = noopLogger{}
	}

	c, err := schemaregistry.NewClient(url)
	if err != nil {
		return nil, err
	}

	r := &Registry{
		schemas: make(map[string]map[int]*Encoder),
		idMap:   make(map[int]*Encoder),
		client:  c,
		mu:      new(sync.RWMutex),
		options: options,
		logger:  options.logger,
	}

	return r, nil
}

func (r *Registry) Register(subject string, version int, decoder jsonDecoder) error {
	if _, ok := r.schemas[subject]; ok {
		if _, ok := r.schemas[subject][version]; ok {
			r.logger.Warn(`schema-registry.registry`, fmt.Sprintf(`subject [%s][%s] already registred`, subject, Version(version)))
		}
	}

	if version == int(VersionAll) {
		versions, err := r.client.Versions(subject)
		if err != nil {
			return err
		}
		for _, v := range versions {
			if err := r.Register(subject, v, decoder); err != nil {
				return err
			}
		}
		return nil
	}

	var clientSub schemaregistry.Schema
	if version == int(VersionLatest) {
		sub, err := r.client.GetLatestSchema(subject)
		if err != nil {
			return err
		}

		clientSub = sub
	} else {
		sub, err := r.client.GetSchemaBySubject(subject, version)
		if err != nil {
			return err
		}

		clientSub = sub
	}

	s := &Subject{
		Schema:      clientSub.Schema,
		Id:          clientSub.ID,
		Version:     clientSub.Version,
		Subject:     clientSub.Subject,
		JsonDecoder: decoder,
	}

	if r.schemas[subject] == nil {
		r.schemas[subject] = make(map[int]*Encoder)
	}

	e, err := NewEncoder(r, s)
	if err != nil {
		return err
	}

	r.schemas[subject][version] = e
	r.idMap[clientSub.ID] = e

	r.logger.Info(`schema-registry.registry`, fmt.Sprintf(`subject [%s][%s] registred`, subject, Version(version)))

	return nil
}

func (r *Registry) Sync() error {
	if r.options.backGroundSync {
		bgSync, err := newSync(r.options.bootstrapServers, r.options.storageTopic, r)
		if err != nil {
			return err
		}

		if err := bgSync.start(); err != nil {
			return err
		}
	}

	return nil
}

func (r *Registry) WithSchema(subject string, version int) *Encoder {
	r.mu.Lock()
	defer r.mu.Unlock()

	e, ok := r.schemas[subject][version]
	if !ok {
		panic(fmt.Sprintf(`schema-registry.registry: unregistred subject [%s][%d]`, subject, version))
	}

	return e
}

func (r *Registry) WithLatestSchema(subject string) *Encoder {
	r.mu.Lock()
	defer r.mu.Unlock()

	versions, ok := r.schemas[subject]
	if !ok {
		panic(fmt.Sprintf(`schema-registry.registry: unregistred subject [%s]`, subject))
	}
	var v int
	for _, version := range versions {
		if version.subject.Version > v {
			v = version.subject.Version
		}
	}

	return versions[v]
}

/*func (r *Registry) GenericEncoder(subject string, version int) *GenericEncoder {
	s, ok := r.schemas[subject][version]
	if !ok {
		log.Fatal(log.WithPrefix(`avro.registry`, fmt.Sprintf(`unregistred subject [%s]`, subject)))
	}

	return &GenericEncoder{
		subject: s,
	}
}*/
