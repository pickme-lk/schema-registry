/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package main

import (
	"time"

	"github.com/pickme-go/log"
	schemaregistry "github.com/pickme-go/schema-registry"
)

func main() {

	// init a new schema registry instance and connect
	registry, err := schemaregistry.NewRegistry(`localhost:8089/`,
		schemaregistry.WithBackgroundSync([]string{`localhost:9092`}, `__schemas`))
	if err != nil {
		log.Fatal(err)
	}

	if err := registry.Register(`com.org.events.test.TestTwo`, 1, func(data []byte) (v interface{}, err error) {
		return nil, nil
	}); err != nil {
		log.Fatal(err)
	}

	if err = registry.Sync(); err != nil {
		log.Fatal(err)
	}

	log.Info(`your event is successfully registered`)

	time.Sleep(10 * time.Minute)
}
