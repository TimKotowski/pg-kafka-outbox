package repository

import (
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
