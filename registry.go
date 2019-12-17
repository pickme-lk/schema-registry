package schemaregistry

import (
	"fmt"
	"sync"

	registry "github.com/landoop/schema-registry"
	"github.com/pickme-go/log"
)

// Version is the type to hold default register vrsion options
type Version int

const (
	//VersionLatest constant hold the flag to register the latest version of the subject
	VersionLatest Version = -1
	//VersionAll constant hold the flag to register all the versions of the subject
	VersionAll Version = -2
)

// String returns the registed version type
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

//Subject holds the Schema information of the registered subject
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
	logger           log.Logger
}

// Registry type holds schema registry details
type Registry struct {
	schemas map[string]map[int]*Encoder // subject/version/encoder
	idMap   map[int]*Encoder
	client  *registry.Client
	mu      *sync.RWMutex
	options *options
	logger  log.Logger
}

//Option is a type to host NewRegistry configurations
type Option func(*options)

// WithBackgroundSync returns a Configurations to create a NewRegistry with kafka dynamic schema sync.
// function required slice of kafka bootstrapServers and schema storageTopic as inputs
func WithBackgroundSync(bootstrapServers []string, storageTopic string) Option {
	return func(options *options) {
		options.bootstrapServers = bootstrapServers
		options.storageTopic = storageTopic
		options.backGroundSync = true
	}
}

//WithLogger returns a Configurations to create a NewRegistry with given PrefixedLogger
func WithLogger(logger log.Logger) Option {
	return func(options *options) {
		options.logger = logger
	}
}

// NewRegistry returns pointer to connected registry with given options or error if it's unable to connect
func NewRegistry(url string, opts ...Option) (*Registry, error) {

	options := new(options)
	for _, opt := range opts {
		opt(options)
	}

	if options.logger == nil {
		options.logger = log.NewNoopLogger()
	}

	c, err := registry.NewClient(url)
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

// Register registers the given subject, version and JSON value decoder to the Registry
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

	var clientSub registry.Schema
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

// Sync function start the background schema sync from kafka topic
//
// Newly Created Schemas will register in background and application does not require any restart
func (r *Registry) Sync() error {
	//if r.options.backGroundSync {
	//	bgSync, err := newSync(r.options.bootstrapServers, r.options.storageTopic, r)
	//	if err != nil {
	//		return err
	//	}
	//
	//	if err := bgSync.start(); err != nil {
	//		return err
	//	}
	//}
	//
	return nil
}

// WithSchema return the specific encoder which registered at the initialization under the subject and version
func (r *Registry) WithSchema(subject string, version int) *Encoder {
	r.mu.Lock()
	defer r.mu.Unlock()

	e, ok := r.schemas[subject][version]
	if !ok {
		panic(fmt.Sprintf(`schema-registry.registry: unregistred subject [%s][%d]`, subject, version))
	}

	return e
}

// WithLatestSchema returns the latest event version encoder registered under given subject
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
