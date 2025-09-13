package outboxdb

import (
	"context"
	"log"

	"github.com/uptrace/bun"
)

func RunInTx(ctx context.Context, db *bun.DB, fn func(tx bun.Tx) error) error {
	err := db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		log.Println("database transaction rolled back due to", err.Error())
	}

	return err
}

func RunInTxWithReturnType[T any](ctx context.Context, db *bun.DB, fn func(tx bun.Tx) (T, error)) (T, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return *new(T), err
	}

	var committed bool
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	result, err := fn(tx)
	if err != nil {
		return *new(T), err
	}

	if err := tx.Commit(); err != nil {
		return *new(T), err
	}

	committed = true

	return result, nil
}
