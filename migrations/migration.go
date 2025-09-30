package migrations

import (
	"context"
	"embed"
	"log"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

// Migrations TODO: Work on revamping migration to remove global variable.
var Migrations = migrate.NewMigrations()

//go:embed schema/*.sql
var migrationFS embed.FS

func init() {
	if err := Migrations.Discover(migrationFS); err != nil {
		panic(err)
	}
}

func Migrate(ctx context.Context, db *bun.DB) error {
	m := migrate.NewMigrator(
		db,
		Migrations,
		migrate.WithTableName("outbox_migrations"),
		migrate.WithLocksTableName("outbox_migration_locks"),
	)
	if err := m.Init(ctx); err != nil {
		return err
	}

	group, err := m.Migrate(ctx)
	if err != nil {
		return err
	}

	if group.IsZero() {
		log.Println("no new migrations were applied")
	} else {
		log.Printf("applied migration group: %s (migrations: %v)", group, group.Migrations)
	}

	migrations, err := m.AppliedMigrations(ctx)
	if err != nil {
		return err
	}

	log.Printf("total applied migrations %d", len(migrations))

	return nil
}
