package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	outbox "github.com/TimKotowski/pg-kafka-outbox"
	"github.com/TimKotowski/pg-kafka-outbox/migrations"
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
	"github.com/stretchr/testify/assert"
	"github.com/uptrace/bun"
)

const (
	postgresDefaultPassword = "password"
	postgresDefaultUser     = "outbox"
	postgresDefaultDB       = "outbox_kafka"

	tag = "17"
)

type Resource struct {
	Dsn string

	DB *bun.DB

	ContainerName string

	ConstainerID string
}

func SetUp(pool *dockertest.Pool, t *testing.T) Resource {
	ctx := context.Background()
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        tag,
		Env: []string{
			"POSTGRES_PASSWORD=" + postgresDefaultPassword,
			"POSTGRES_USER=" + postgresDefaultUser,
			"POSTGRES_DB=" + postgresDefaultDB,
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	assert.NoError(t, err)

	t.Cleanup(func() {
		if err := pool.Purge(resource); err != nil {
			assert.NoError(t, err)
		}
	})

	databaseURL := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		postgresDefaultUser,
		postgresDefaultPassword,
		resource.GetBoundIP("5432/tcp"),
		resource.GetPort("5432/tcp"),
		postgresDefaultDB,
	)

	pool.MaxWait = 20 * time.Second
	db, err := pgIsReady(pool, databaseURL)
	assert.NoError(t, err)

	if db == nil {
		assert.NoError(t, errors.New("something went horribly wrong, db connection unsuccessful"))
	}

	err = migrations.Migrate(ctx, db)
	assert.NoError(t, err)

	return Resource{
		Dsn:           databaseURL,
		DB:            db,
		ContainerName: resource.Container.Name,
		ConstainerID:  resource.Container.ID,
	}
}

func pgIsReady(pool *dockertest.Pool, dsn string) (*bun.DB, error) {
	var err error
	var db *bun.DB

	if err := pool.Retry(func() error {
		db, err = outbox.GetDBConnection(outbox.NewConfig(outbox.WithDSN(dsn)))
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		return nil, err
	}

	return db, nil
}
