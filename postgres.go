package outbox

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

func initializeDB(config *Config) (*bun.DB, error) {
	db, err := GetDBConnection(config)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func GetDBConnection(config *Config) (*bun.DB, error) {
	if config.DSN == "" {
		return nil, errors.New("connection string is empty, unable to establish connection")
	}

	pgxCfg, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection due to %w", err)
	}
	if config.TLSConfig != nil {
		pgxCfg.ConnConfig.TLSConfig = config.TLSConfig
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), pgxCfg)
	if err != nil {
		return nil, err
	}

	db := bun.NewDB(stdlib.OpenDBFromPool(pool), pgdialect.New())
	if err = db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
