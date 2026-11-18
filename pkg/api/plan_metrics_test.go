package api

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// The API owns the plan counter: every plan attempt is counted exactly once,
// under the deployment it resolved to, and only a plan whose response was
// stored counts as a success. A plan refused before the data plane is asked,
// refused after it answered, or lost to storage counts once as an error, so a
// caller that also counted would double every plan.
func TestExecutePlan_CountsEachPlanOnce(t *testing.T) {
	tests := []struct {
		name           string
		request        func() PlanRequest
		planResp       *ternv1.PlanResponse
		storeErr       error
		wantErr        bool
		wantStatus     string
		wantDeployment string
	}{
		{
			name:           "stored plan",
			request:        placedNamespacesRequest,
			planResp:       &ternv1.PlanResponse{PlanId: "plan-primary"},
			wantStatus:     "success",
			wantDeployment: "eu",
		},
		{
			name: "invalid schema files",
			request: func() PlanRequest {
				req := placedNamespacesRequest()
				req.SchemaFiles["ns_1"] = nil
				return req
			},
			planResp:       &ternv1.PlanResponse{PlanId: "plan-primary"},
			wantErr:        true,
			wantStatus:     "error",
			wantDeployment: "unknown",
		},
		{
			name:           "refused drop",
			request:        placedNamespacesRequest,
			planResp:       &ternv1.PlanResponse{PlanId: "plan-primary", Changes: dropsPlan("ns_1", "legacy")},
			wantErr:        true,
			wantStatus:     "error",
			wantDeployment: "eu",
		},
		{
			name:           "plan not stored",
			request:        placedNamespacesRequest,
			planResp:       &ternv1.PlanResponse{PlanId: "plan-primary"},
			storeErr:       errors.New("storage unavailable"),
			wantErr:        true,
			wantStatus:     "error",
			wantDeployment: "eu",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := installManualMetricReader(t)
			client := &mockTernClient{isRemote: true, planResp: tt.planResp}
			plans := &capturingPlanStore{createErr: tt.storeErr}
			_, err := namespaceSelectionService(t, client, plans).ExecutePlan(t.Context(), tt.request())
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			var counted []string
			for _, dp := range collectCounterPoints(t, reader, "schemabot.plans.total") {
				for range dp.Value {
					counted = append(counted, attributeValue(t, dp, "status")+" "+attributeValue(t, dp, "deployment"))
				}
			}
			assert.Equal(t, []string{tt.wantStatus + " " + tt.wantDeployment}, counted)
		})
	}
}
