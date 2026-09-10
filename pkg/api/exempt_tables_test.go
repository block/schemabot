package api

import (
	"testing"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanResponseFromProtoCarriesExemptTables(t *testing.T) {
	response := planResponseFromProto(&ternv1.PlanResponse{ExemptTables: []*ternv1.ExemptTables{{
		Namespace: "app", Tables: []string{"orders_archive_2024"}, Reason: "archive naming",
	}}})
	require.Len(t, response.ExemptTables, 1)
	assert.Equal(t, "app", response.ExemptTables[0].Namespace)
	assert.Equal(t, []string{"orders_archive_2024"}, response.ExemptTables[0].Tables)
	assert.Equal(t, "archive naming", response.ExemptTables[0].Reason)
}
