package migrations

import (
	"context"
	"embed"
	"log"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

var Migrations = migrate.NewMigrations()

//go:embed schema/*.sql
var sqlMigrations embed.FS

func init() {
	if err := Migrations.Discover(sqlMigrations); err != nil {
		panic(err)
	}
}

func Migrate(ctx context.Context, db *bun.DB) {
	m := migrate.NewMigrator(db, Migrations)
	if err := m.Init(ctx); err != nil {
		log.Fatal(err)
	}

	group, err := m.Migrate(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Check if any migrations were applied
	if group.IsZero() {
		log.Println("no new migrations were applied")
	} else {
		log.Printf("applied migration group: %s (migrations: %v)", group, group.Migrations)
	}

	migrations, err := m.AppliedMigrations(ctx)
	if err != nil {
		log.Fatal(err)
	}

	log.Fatalf("total applied migrations %d", len(migrations))
}
