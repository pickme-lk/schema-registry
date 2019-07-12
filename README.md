# Schema Registry Client

[![GoDoc](https://godoc.org/github.com/pickme-go/schema-registry?status.svg)](https://godoc.org/github.com/pickme-go/schema-registry)

This repository contains wrapper function to communicate 
Confluent's Kafka schema registry via REST API and 
schema dynamic sync directly from kafka topic.

Client
------
Download library using `go get -u github.com/pickme-go/schema-registry`

Following code slice create a schema registry client 
```go
import schemaregistry "github.com/pickme-go/schema-registry"

registry, _ := schemaregistry.NewRegistry(`localhost:8089/`)
```

Following code line register an event `com.pickme.events.test` with version `1`
```go
import schemaregistry "github.com/pickme-go/schema-registry"

registry.Register(`com.pickme.events.test`, 1, func(data []byte) (v interface{}, err error)
```

Dynamically sync schema's from kafka schema data topic
```go
import schemaregistry "github.com/pickme-go/schema-registry"

registry, _ := schemaregistry.NewRegistry(`localhost:8089/`,schemaregistry.WithBackgroundSync([]string{`localhost:9092`}, `__schemas`))
registry.Sync()
```

Message Structure
-----------------
Avro encoded events are published with magic byte and schema Id added to it. 
Following structure shows the message format used in the library to encode the message. 

    +====================+====================+======================+
    | Magic byte(1 byte) | Schema ID(4 bytes) | AVRO encoded message |
    +====================+====================+======================+

ToDo
----
 - Write unit test
 - Write mock functions
 - write benchmarks
 - setup travis for automated testing