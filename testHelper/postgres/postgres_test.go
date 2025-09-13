package postgres_test

import (
	"testing"

	"github.com/ory/dockertest"
	"github.com/stretchr/testify/assert"

	"github.com/TimKotowski/pg-kafka-outbox/testHelper/postgres"
)

func TestPostgresContainerSetup(t *testing.T) {
	pool, err := dockertest.NewPool("")
	assert.NoError(t, err)

	testContainers := 2
	var resources []postgres.Resource
	for i := 0; i < testContainers; i++ {
		resource := postgres.SetUp(pool, t)
		resources = append(resources, resource)
	}
	assert.Len(t, resources, testContainers)

	for _, resource := range resources {
		err = resource.DB.Ping()
		assert.NoError(t, err)
	}
}
