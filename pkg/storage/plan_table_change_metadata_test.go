package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanTableChangeMetadata(t *testing.T) {
	const (
		dropNickname = `ALTER TABLE "public"."users" DROP COLUMN "nickname"`
		addEmail     = `ALTER TABLE "public"."users" ADD COLUMN "email" text`
	)
	recorded := map[string]string{"postgres_reviewed_drop": `{"kind":"column","name":"nickname"}`}
	plan := func(tables ...TableChange) *Plan {
		return &Plan{PlanIdentifier: "plan-1", Namespaces: map[string]*NamespacePlanData{"public": {Tables: tables}}}
	}

	t.Run("returns the metadata of the change the task runs", func(t *testing.T) {
		p := plan(
			TableChange{Table: "users", DDL: addEmail},
			TableChange{Table: "users", DDL: dropNickname, Metadata: recorded},
		)
		got, err := p.TableChangeMetadata("public", "users", dropNickname)
		require.NoError(t, err)
		assert.Equal(t, recorded, got)

		got["postgres_reviewed_drop"] = "changed"
		again, err := p.TableChangeMetadata("public", "users", dropNickname)
		require.NoError(t, err)
		assert.Equal(t, recorded, again, "the caller gets a copy, never the stored map")
	})

	t.Run("returns nil when no change matches or the match carries none", func(t *testing.T) {
		p := plan(TableChange{Table: "users", DDL: addEmail}, TableChange{Table: "users", DDL: dropNickname, Metadata: recorded})
		for name, args := range map[string][3]string{
			"other namespace": {"app", "users", dropNickname},
			"other table":     {"public", "orders", dropNickname},
			"other DDL":       {"public", "users", `ALTER TABLE "public"."users" DROP COLUMN "legacy"`},
			"no metadata":     {"public", "users", addEmail},
		} {
			got, err := p.TableChangeMetadata(args[0], args[1], args[2])
			require.NoError(t, err, name)
			assert.Nil(t, got, name)
		}
	})

	t.Run("identical duplicates resolve to their shared metadata", func(t *testing.T) {
		p := plan(TableChange{Table: "users", DDL: dropNickname, Metadata: recorded}, TableChange{Table: "users", DDL: dropNickname, Metadata: recorded})
		got, err := p.TableChangeMetadata("public", "users", dropNickname)
		require.NoError(t, err)
		assert.Equal(t, recorded, got)
	})

	t.Run("duplicates that disagree are an error", func(t *testing.T) {
		p := plan(
			TableChange{Table: "users", DDL: dropNickname, Metadata: recorded},
			TableChange{Table: "users", DDL: dropNickname, Metadata: map[string]string{"postgres_reviewed_drop": `{"kind":"column","name":"other"}`}},
		)
		_, err := p.TableChangeMetadata("public", "users", dropNickname)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `plan plan-1 holds more than one change to table "users" in namespace "public"`)
	})
}
