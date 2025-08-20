package migrations

import (
	"context"
	"embed"
	"fmt"

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

func Migrate(ctx context.Context, db *bun.DB) error {
	m := migrate.NewMigrator(db, Migrations)
	if err := m.Init(ctx); err != nil {
		return err
	}

	group, err := m.Migrate(ctx)
	if err != nil {
		return err
	}

	if group.IsZero() {
		fmt.Println("no new migrations were applied")
	} else {
		fmt.Printf("applied migration group: %s (migrations: %v)", group, group.Migrations)
	}

	migrations, err := m.AppliedMigrations(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("total applied migrations %d", len(migrations))

	return nil
}
