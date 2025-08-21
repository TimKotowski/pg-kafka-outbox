package repository

import (
	"context"
	"log"

	"github.com/uptrace/bun"
)

type Repository interface {
	GetScheduledJobs() ([]Job, error)

	DeleteJobs(jobIds []string) (int, error)

	RequeueOrphanedJobs() (int, error)
}

type repository struct {
	db *bun.DB
}

func NewRepository(db *bun.DB) Repository {
	return &repository{
		db: db,
	}
}

func (r *repository) GetScheduledJobs() ([]Job, error) {
	return nil, nil
}

func (r *repository) DeleteJobs(jobIds []string) (int, error) {
	return 0, nil
}

func (r *repository) RequeueOrphanedJobs() (int, error) {
	return 0, nil
}

func RunInTx(ctx context.Context, db *bun.DB, fn func(tx bun.Tx) error) error {
	err := db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		log.Println("database transcation rolled back due to", err.Error())
	}

	return err
}
